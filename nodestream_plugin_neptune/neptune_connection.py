import asyncio
import json
import random
from abc import ABC, abstractmethod
from logging import getLogger

from aiobotocore.session import get_session


class NeptuneConnection(ABC):
    @property
    def logger(self):
        return getLogger(self.__class__.__name__)

    async def execute(self, query_stmt: str, parameters) -> dict | None:
        response: dict | None = None
        max_retries = 3
        retry_delay = 1

        async with self._create_boto_client() as client:
            try:
                response = await self.__retry(
                    func=lambda: self.__attempt_query(client, query_stmt, parameters),
                    max_retries=max_retries,
                    delay=retry_delay,
                    exceptions=self._get_retryable_exceptions(client),
                )

            except Exception as e:
                self.logger.exception(f"Unexpected error: {e} for query: {query_stmt}.")

        return response

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


class NeptuneDBConnection(NeptuneConnection):
    @classmethod
    def from_configuration(cls, host: str, graph_id: str = None, **client_kwargs):
        if host is None:
            raise ValueError("A `host` must be specified when `mode` is 'database'.")
        if graph_id is not None:
            raise ValueError(
                "A `graph_id` should not be used with Neptune Database, `host=<Neptune Endpoint>` should be used "
                "instead. If using Neptune Analytics, set `mode='analytics'."
            )
        return cls(host=host, **client_kwargs)

    def __init__(self, host: str, **client_kwargs) -> None:
        self.host = host
        self.boto_session = get_session()
        self.client_kwargs = client_kwargs

    def _create_boto_client(self):
        return self.boto_session.create_client(
            "neptunedata", endpoint_url=self.host, **self.client_kwargs
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
    def from_configuration(cls, graph_id: str, host: str = None, **client_kwargs):
        if graph_id is None:
            raise ValueError(
                "A `graph_id` must be specified when `mode` is 'analytics'."
            )
        if host is not None:
            raise ValueError(
                "A `host` should not be used with Neptune Analytics, `graph_id=<Graph Identifier>` should be used instead. If using Neptune Database, set `mode='database'."
            )
        return cls(graph_id=graph_id, **client_kwargs)

    def __init__(self, graph_id: str, **client_kwargs) -> None:
        self.graph_id = graph_id
        self.boto_session = get_session()
        self.client_kwargs = client_kwargs

    def _create_boto_client(self):
        return self.boto_session.create_client("neptune-graph", **self.client_kwargs)

    async def _execute_query(self, client, query_stmt: str, parameters):
        self.logger.debug(
            "Executing Cypher Query to Neptune Analytics",
            extra={
                "query": query_stmt,
                "graph_id": self.graph_id,
            },
        )

        return await client.execute_query(
            graphIdentifier=self.graph_id,
            queryString=query_stmt,
            language="OPEN_CYPHER",
            parameters=parameters,
        )

    def _get_retryable_exceptions(self, client):
        return (client.exceptions.ConflictException,)
