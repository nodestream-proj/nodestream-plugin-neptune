from nodestream.databases.copy import TypeRetriever
from nodestream.databases.database_connector import QueryExecutor, DatabaseConnector
from .ingest_query_builder import NeptuneDBIngestQueryBuilder
from typing import Any, Dict, List, Optional, Tuple, Union

class NeptuneDatabaseConnector(DatabaseConnector, alias="neptune"):
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
        from .query_executor import NeptuneQueryExecutor

        return NeptuneQueryExecutor(
            host=self.host,
            region=self.region,
            ingest_query_builder=self.ingest_query_builder,
            async_partitions=self.async_partitions
        )

    def make_type_retriever(self) -> TypeRetriever:
        from .type_retriever import NeptuneDBTypeRetriever

        return NeptuneDBTypeRetriever(self)
