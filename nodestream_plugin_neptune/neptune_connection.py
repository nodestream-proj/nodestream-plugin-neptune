import asyncio
import json
import random
from abc import ABC, abstractmethod
from logging import getLogger

import botocore
from aiobotocore.session import get_session

# The copy pipeline fans writes out across `--flush-concurrency` lanes (commonly
# 16-64), and all lanes plus the reader share a single boto client. botocore's
# default connection pool is only `max_pool_connections=10`, so beyond ~10 lanes
# the extra ones block waiting for a free connection — the pipeline goes idle
# waiting on the pool rather than on Neptune, and throughput stops scaling with
# flush concurrency. Default to a pool large enough to cover typical fan-out.
DEFAULT_MAX_POOL_CONNECTIONS = 64


def _with_pool_config(client_kwargs: dict) -> dict:
    """Ensure the boto client has an adequately sized connection pool.

    Returns a copy of ``client_kwargs`` whose ``config`` carries a
    ``max_pool_connections`` of at least ``DEFAULT_MAX_POOL_CONNECTIONS``. A
    caller-supplied ``config`` is respected: only its pool size is raised (never
    lowered), and its other settings are preserved.
    """
    kwargs = dict(client_kwargs)
    existing = kwargs.get("config")
    if existing is not None:
        current = getattr(existing, "max_pool_connections", None) or 0
        if current >= DEFAULT_MAX_POOL_CONNECTIONS:
            return kwargs
        kwargs["config"] = existing.merge(
            botocore.config.Config(max_pool_connections=DEFAULT_MAX_POOL_CONNECTIONS)
        )
    else:
        kwargs["config"] = botocore.config.Config(
            max_pool_connections=DEFAULT_MAX_POOL_CONNECTIONS
        )
    return kwargs


class NeptuneConnection(ABC):
    @property
    def logger(self):
        return getLogger(self.__class__.__name__)

    async def execute(self, query_stmt: str, parameters) -> dict | None:
        response: dict | None = None
        max_retries = 3
        retry_delay = 1

        await self._ensure_client()

        try:
            if self.client is not None:
                try:
                    response = await self.__retry(
                        func=lambda: self.__attempt_query(
                            self.client, query_stmt, parameters
                        ),
                        max_retries=max_retries,
                        delay=retry_delay,
                        exceptions=self._get_retryable_exceptions(self.client),
                    )

                except botocore.exceptions.EndpointConnectionError as e:
                    self.logger.error(f"\nFailed to connect to database: {e}")
                    try:
                        child_error = e.kwargs["error"]
                        self.logger.error(child_error)
                    except Exception:
                        pass
                except (
                    botocore.exceptions.NoCredentialsError,
                    self.client.exceptions.AccessDeniedException,
                ) as e:
                    self.logger.error(f"\nUnexpected error: {e}.")
                except Exception as e:
                    self.logger.error(
                        f"\nUnexpected error: {e} for query: {query_stmt}."
                    )
            if response is not None and response.get("payload"):
                response["payload"].close()
            return response
        except botocore.exceptions.NoRegionError as e:
            self.logger.error(f"\nUnexpected error: {e}.")

    def _get_client_lock(self) -> "asyncio.Lock":
        """Return the client-init lock, creating it lazily.

        The lock is created on first use rather than in ``__init__`` so it binds
        to the event loop that actually runs the queries. Constructing an
        asyncio.Lock before the loop is running can attach it to the wrong loop
        in some Python versions.
        """
        lock = getattr(self, "_client_lock", None)
        if lock is None:
            lock = asyncio.Lock()
            self._client_lock = lock
        return lock

    async def _ensure_client(self):
        """Create the boto client exactly once, even under concurrency.

        Many query coroutines run concurrently (the executor fans out partitions
        with asyncio.gather, across many flush lanes). Without mutual exclusion
        they would all observe ``self.client is None`` on the first burst and
        each create their own client, resolving credentials dozens of times in
        parallel and orphaning all but one client (leaking sessions/fds and
        hammering the credential provider). The lock ensures a single client is
        created and then reused by everyone else.
        """
        if self.client is not None:
            return
        async with self._get_client_lock():
            # Re-check inside the lock: another coroutine may have created it
            # while we were waiting.
            if self.client is None:
                self.boto_context_manager = self._create_boto_client()
                self.client = await self.boto_context_manager.__aenter__()

    @abstractmethod
    def _create_boto_client(self):
        pass

    @abstractmethod
    async def _execute_query(
        self, client, query_stmt: str, parameters: str
    ) -> dict | None:
        pass

    async def __attempt_query(
        self, client, query_stmt: str, parameters: str
    ) -> dict | None:
        """
        Attempts to execute OC query `query_stmt` with `parameters` via `client`

        Args:
            client:
                Client to run query. Client is of whichever type is produced by `self._create_boto_client()`
            query_stmt: str
                OpenCypher query statement string to execute.
            parameters: str
                Query parameters encoded as a JSON string.

        Returns: dict | None
            The response from the query.

        Raises:
            Exception: Any exception thrown from the client when attempting queries.
        """
        response = await self._execute_query(
            client,
            query_stmt=query_stmt,
            parameters=parameters,
        )

        code = response["ResponseMetadata"]["HTTPStatusCode"]
        if code != 200:
            self.logger.error(f"Query `{query_stmt}` failed with response:\n{response}")

        return response

    async def __retry(self, func, max_retries: int, exceptions, delay=0):
        """
        Retries a function `func` up to `attempts` times on encountering exceptions in `exceptions`.
        Waits for `delay` seconds between retries.

        Args:
            max_retries: int
                Number of retry attempts.
            exceptions: (Exception...)
                Exceptions to catch and retry on (tuple of exception types).
            delay: int
                Delay in seconds between retries.
            func: function
                Function to be retried.

        Returns: dict | None
            The return value of the successful function call.

        Raises:
            Exception: The last encountered exception if all retries fail.
        """

        for attempt in range(1, max_retries + 1):
            try:
                return await func()
            except exceptions as e:
                if attempt < max_retries:
                    self.logger.warning(
                        f"Query failed on attempt {attempt}/{max_retries} with error: {e} "
                        f"Retrying in {delay}s."
                    )
                    await asyncio.sleep(delay + random.uniform(0, 0.5))
                    delay *= 2
                    continue  # retry on conflict exception
                else:
                    self.logger.exception(
                        f"Query failed on attempt {attempt}/{max_retries} with error: {e} "
                        f"Max retries reached."
                    )
                    raise e

    @abstractmethod
    def _get_retryable_exceptions(self):
        pass

    async def close(self):
        if self.boto_context_manager is not None:
            await self.boto_context_manager.__aexit__(None, None, None)


class NeptuneDBConnection(NeptuneConnection):
    @classmethod
    def from_configuration(
        cls, host: str, graph_id: str = None, region: str = None, **client_kwargs
    ):
        if host is None:
            raise ValueError("A `host` must be specified when `mode` is 'database'.")
        if graph_id is not None:
            raise ValueError(
                "A `graph_id` should not be used with Neptune Database, `host=<Neptune Endpoint>` should be used "
                "instead. If using Neptune Analytics, set `mode='analytics'."
            )
        return cls(host=host, region=region, **client_kwargs)

    def __init__(self, host: str, region: str = None, **client_kwargs) -> None:
        self.host = host
        self.boto_session = get_session()
        self.region = region
        self.client_kwargs = client_kwargs
        self.client = None
        self.boto_context_manager = None
        self._client_lock = None

    def _create_boto_client(self):
        return self.boto_session.create_client(
            "neptunedata",
            endpoint_url=self.host,
            region_name=self.region,
            **_with_pool_config(self.client_kwargs),
        )

    async def _execute_query(self, client, query_stmt: str, parameters):
        self.logger.debug(
            "Executing Cypher Query to Neptune Database",
            extra={
                "query": query_stmt,
                "host": self.host,
            },
        )

        return await client.execute_open_cypher_query(
            openCypherQuery=query_stmt,
            # Use json.dumps() to warp dict's key/values in double quotes.
            parameters=json.dumps(parameters),
        )

    def _get_retryable_exceptions(self, client):
        return (client.exceptions.ConcurrentModificationException,)


class NeptuneAnalyticsConnection(NeptuneConnection):
    @classmethod
    def from_configuration(
        cls, graph_id: str, host: str = None, region: str = None, **client_kwargs
    ):
        if graph_id is None:
            raise ValueError(
                "A `graph_id` must be specified when `mode` is 'analytics'."
            )
        if host is not None:
            raise ValueError(
                "A `host` should not be used with Neptune Analytics, `graph_id=<Graph Identifier>` should be used instead. If using Neptune Database, set `mode='database'."
            )
        return cls(graph_id=graph_id, region=region, **client_kwargs)

    def __init__(self, graph_id: str, region: str = None, **client_kwargs) -> None:
        self.graph_id = graph_id
        self.boto_session = get_session()
        self.region = region
        self.client_kwargs = client_kwargs
        self.client = None
        self.boto_context_manager = None
        self._client_lock = None

    def _create_boto_client(self):
        return self.boto_session.create_client(
            "neptune-graph",
            region_name=self.region,
            **_with_pool_config(self.client_kwargs),
        )

    async def _execute_query(self, client, query_stmt: str, parameters):
        self.logger.debug(
            "Executing Cypher Query to Neptune Analytics",
            extra={
                "query": query_stmt,
                "graph_id": self.graph_id,
            },
        )

        response = await client.execute_query(
            graphIdentifier=self.graph_id,
            queryString=query_stmt,
            language="OPEN_CYPHER",
            parameters=parameters,
        )
        return await self._decode_payload(response)

    @staticmethod
    async def _decode_payload(response):
        """Expose the query results of a Neptune Analytics response as `results`.

        Unlike Neptune Database, Neptune Analytics returns the result set only
        inside a streaming `payload` (a JSON document of the form
        `{"results": [...]}`). Readers such as the type retriever and the
        extractors expect `response["results"]`, so the stream is read and
        decoded here, before `NeptuneConnection.execute` closes it.
        """
        if not isinstance(response, dict) or "results" in response:
            return response
        payload = response.get("payload")
        if payload is None:
            return response
        body = await payload.read()
        response["results"] = json.loads(body).get("results", []) if body else []
        return response

    def _get_retryable_exceptions(self, client):
        return (client.exceptions.ConflictException,)
