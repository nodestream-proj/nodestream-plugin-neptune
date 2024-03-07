from nodestream.databases.copy import TypeRetriever
from nodestream.databases.database_connector import (DatabaseConnector,
                                                     QueryExecutor)

from .ingest_query_builder import NeptuneIngestQueryBuilder
from .neptune_connection import NeptuneDBConnection, NeptuneAnalyticsConnection
from .neptune_query_executor import NeptuneQueryExecutor


class NeptuneConnector(DatabaseConnector, alias="neptune"):
    @classmethod
    def from_file_data(cls, mode: str, region: str, host: str = None, graph_id: str = None, **kwargs):
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
        region: str,
        async_partitions: int,
        ingest_query_builder: NeptuneIngestQueryBuilder,
        host: str = None,
        graph_id: str = None
    ) -> None:
        if mode != "database" and mode != "analytics":
            raise ValueError("`mode` must be either 'database' or 'analytics'")
        if mode == "database":
            if host is None:
                raise ValueError("A `host` must be specified when `mode` is 'database'.")
            if graph_id is not None:
                raise ValueError("A `graph_id` should not be used with Neptune Database, `host=<Neptune Endpoint>` should be used instead. If using Neptune Analytics, set `mode='analytics'.")
            self.connection = NeptuneDBConnection(host=host, region=region)
        elif mode == "analytics":
            if graph_id is None:
                raise ValueError("A `graph_id` must be specified when `mode` is 'analytics'.")
            if host is not None:
                raise ValueError("A `host` should not be used with Neptune Analytics, `graph_id=<Graph Identifier>` should be used instead. If using Neptune Database, set `mode='database'.")
            self.connection = NeptuneAnalyticsConnection(graph_id=graph_id, region=region)

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
