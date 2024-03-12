from nodestream.databases.copy import TypeRetriever
from nodestream.databases.database_connector import (DatabaseConnector,
                                                     QueryExecutor)

from .ingest_query_builder import NeptuneIngestQueryBuilder
from .neptune_connection import NeptuneAnalyticsConnection, NeptuneDBConnection
from .neptune_query_executor import NeptuneQueryExecutor


class NeptuneConnector(DatabaseConnector, alias="neptune"):
    @classmethod
    def from_file_data(
        cls, mode: str, region: str = None, host: str = None, graph_id: str = None, **kwargs
    ):
        return cls(
            mode=mode,
            host=host,
            graph_id=graph_id,
            region=region,
            async_partitions=kwargs.get("async_partitions"),
            ingest_query_builder=NeptuneIngestQueryBuilder(),
        )

    def __init__(
        self,
        mode: str,
        async_partitions: int,
        ingest_query_builder: NeptuneIngestQueryBuilder,
        host: str = None,
        graph_id: str = None,
        region: str = None,
    ) -> None:
        if mode == "database":
            self.connection = NeptuneDBConnection.from_configuration(host=host, graph_id=graph_id, region=region)
        elif mode == "analytics":
            self.connection = NeptuneAnalyticsConnection.from_configuration(graph_id=graph_id, host=host)
        else:
            raise ValueError("`mode` must be either 'database' or 'analytics'")

        self.mode = mode
        self.host = host
        self.graph_id = graph_id
        self.region = region
        self.ingest_query_builder = ingest_query_builder
        self.async_partitions = async_partitions

    def make_query_executor(self) -> QueryExecutor:
        return NeptuneQueryExecutor(
            connection=self.connection,
            ingest_query_builder=self.ingest_query_builder,
            async_partitions=self.async_partitions,
        )

    def make_type_retriever(self) -> TypeRetriever:
        from .type_retriever import NeptuneDBTypeRetriever

        return NeptuneDBTypeRetriever(self)

    def make_migrator(self) -> TypeRetriever:
        raise NotImplementedError
