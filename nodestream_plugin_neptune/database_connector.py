from nodestream.databases.copy import TypeRetriever
from nodestream.databases.database_connector import QueryExecutor, DatabaseConnector
from .ingest_query_builder import NeptuneDBIngestQueryBuilder


class NeptuneDatabaseConnector(DatabaseConnector, alias="neptune_db"):
    @classmethod
    def from_file_data(
        cls,
        host: str,
        region: str,
        **kwargs
    ):
        # Make this use boto3
        return cls(
            host=host,
            region=region,
            async_partitions=kwargs.get("async_partitions"),
            ingest_query_builder=NeptuneDBIngestQueryBuilder()
        )

    def __init__(
        self,
        region,
        host,
        async_partitions,
        ingest_query_builder: NeptuneDBIngestQueryBuilder
    ) -> None:
        self.host = host
        self.region = region
        self.ingest_query_builder = ingest_query_builder
        self.async_partitions = async_partitions

    def make_query_executor(self) -> QueryExecutor:
        from .query_executor import NeptuneDBQueryExecutor

        return NeptuneDBQueryExecutor(
            host=self.host,
            region=self.region,
            ingest_query_builder=self.ingest_query_builder,
            async_partitions=self.async_partitions
        )

    def make_type_retriever(self) -> TypeRetriever:
        from .type_retriever import NeptuneDBTypeRetriever

        return NeptuneDBTypeRetriever(self)

    def make_migrator(self) -> TypeRetriever:
        raise NotImplementedError


class NeptuneAnalyticsDatabaseConnector(NeptuneDatabaseConnector, alias="neptune_analytics"):
    @classmethod
    def from_file_data(
            cls,
            region: str,
            graph_id: str,
            **kwargs
    ):
        return cls(
            region=region,
            graph_id=graph_id,
            async_partitions=kwargs.get("async_partitions"),
            ingest_query_builder=NeptuneDBIngestQueryBuilder()
        )

    def __init__(
            self,
            region,
            graph_id,
            async_partitions,
            ingest_query_builder: NeptuneDBIngestQueryBuilder
    ) -> None:
        self.region = region
        self.graph_id = graph_id
        self.ingest_query_builder = ingest_query_builder
        self.async_partitions = async_partitions

    def make_query_executor(self) -> QueryExecutor:
        from .neptune_analytics_query_executor import NeptuneAnalyticsQueryExecutor

        return NeptuneAnalyticsQueryExecutor(
            region=self.region,
            graph_id=self.graph_id,
            ingest_query_builder=self.ingest_query_builder,
            async_partitions=self.async_partitions
        )
