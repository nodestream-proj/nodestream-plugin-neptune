import json
from abc import ABC, abstractmethod
from logging import getLogger

from aiobotocore.session import get_session


class NeptuneConnection(ABC):
    @abstractmethod
    async def execute(self, query_stmt: str, parameters: list):
        pass


class NeptuneDBConnection(NeptuneConnection):
    @classmethod
    def from_configuration(cls, host: str, region: str):
        return cls(host=host, region=region)

    def __init__(self, host: str, region: str) -> None:
        self.host = host
        self.region = region
        self.session = get_session()
        self.logger = getLogger(self.__class__.__name__)

    async def execute(self, query_stmt: str, parameters: list):
        response = None
        async with self.session.create_client(
            "neptunedata", region_name=self.region, endpoint_url=self.host
        ) as client:
            try:
                self.logger.debug(
                    "Executing Cypher Query to Neptune Database",
                    extra={
                        "query": query_stmt,
                        "host": self.host,
                    },
                )

                response = await client.execute_open_cypher_query(
                    openCypherQuery=query_stmt,
                    # Use json.dumps() to warp dict's key/values in double quotes.
                    parameters=json.dumps(parameters),
                )

                code = response["ResponseMetadata"]["HTTPStatusCode"]
                if code != 200:
                    self.logger.error(
                        f"Query `{query_stmt}` failed with response:\n{response}"
                    )

            except Exception:
                self.logger.exception(
                    f"Unexpected error for query: {query_stmt}.", stack_info=True
                )

        return response


class NeptuneAnalyticsConnection(NeptuneConnection):
    @classmethod
    def from_configuration(cls, graph_id: str, region: str):
        return cls(graph_id=graph_id, region=region)

    def __init__(self, graph_id: str, region: str) -> None:
        self.graph_id = graph_id
        self.region = region
        self.session = get_session()
        self.logger = getLogger(self.__class__.__name__)

    async def execute(self, query_stmt: str, parameters: list):
        response = None
        async with self.session.create_client("neptune-graph") as client:
            try:
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
