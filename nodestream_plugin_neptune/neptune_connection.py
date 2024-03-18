import json
from abc import ABC, abstractmethod
from logging import getLogger

from aiobotocore.session import get_session


class NeptuneConnection(ABC):
    @property
    def logger(self):
        return getLogger(self.__class__.__name__)

    async def execute(self, query_stmt: str, parameters):
        response = None
        async with self._create_boto_client() as client:
            try:
                response = await self._execute_query(
                    client,
                    query_stmt=query_stmt,
                    parameters=parameters,
                )

                code = response["ResponseMetadata"]["HTTPStatusCode"]
                if code != 200:
                    self.logger.error(
                        f"Query `{query_stmt}` failed with response:\n{response}"
                    )

            except Exception as e:
                self.logger.exception(
                    f"Unexpected error for query: {query_stmt}.", e, stack_info=True
                )

        return response

    @abstractmethod
    def _create_boto_client(self):
        pass

    @abstractmethod
    async def _execute_query(self, client, query_stmt: str, parameters: str):
        pass


class NeptuneDBConnection(NeptuneConnection):
    @classmethod
    def from_configuration(cls, host: str, graph_id: str = None, **client_kwargs):
        if host is None:
            raise ValueError(
                "A `host` must be specified when `mode` is 'database'."
            )
        if graph_id is not None:
            raise ValueError(
                "A `graph_id` should not be used with Neptune Database, `host=<Neptune Endpoint>` should be used "
                "instead. If using Neptune Analytics, set `mode='analytics'."
            )
        return cls(host=host, **client_kwargs)

    def __init__(self, host: str,  **client_kwargs) -> None:
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
