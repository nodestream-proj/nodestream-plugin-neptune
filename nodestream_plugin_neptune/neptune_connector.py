from nodestream.databases.copy import TypeRetriever
from nodestream.databases.database_connector import (DatabaseConnector,
                                                     QueryExecutor)

from .ingest_query_builder import NeptuneIngestQueryBuilder
from .neptune_connection import NeptuneAnalyticsConnection, NeptuneDBConnection
from .neptune_query_executor import NeptuneQueryExecutor


class NeptuneConnector(DatabaseConnector, alias="neptune"):
    """A Connector for AWS Neptune Database and AWS Neptune Analytics.

    This class is responsible for creating the various components needed for
    nodestream to interact with a Neptune graph. It is also responsible
    for providing the configuration options for the Neptune database.
    """

    @classmethod
    def from_file_data(
        cls,
        mode: str,
        host: str = None,
        graph_id: str = None,
        **client_kwargs
    ):
        """
        Parameters
        ----------
        mode : str
            Either "database" or "analytics". Selects target type of Neptune graph
        host : str, optional
            Used with mode="database", specify the endpoint of the target Neptune database cluster
        graph_id : str, optional
            Used with mode="analytics", specify the graph identifier of the target Neptune Analytics graph
        client_kwargs : optional
            Additional keyword arguments to be passed to the boto3 client constructor
        """

        return cls(
            mode=mode,
            host=host,
            graph_id=graph_id,
            ingest_query_builder=NeptuneIngestQueryBuilder(),
            **client_kwargs
        )

    def __init__(
        self,
        mode: str,
        ingest_query_builder: NeptuneIngestQueryBuilder,
        host: str = None,
        graph_id: str = None,
        **client_kwargs
    ) -> None:
        if mode == "database":
            self.connection = NeptuneDBConnection.from_configuration(
                host=host, graph_id=graph_id, **client_kwargs
            )
        elif mode == "analytics":
            self.connection = NeptuneAnalyticsConnection.from_configuration(
                graph_id=graph_id, host=host, **client_kwargs
            )
        else:
            raise ValueError("`mode` must be either 'database' or 'analytics'")

        self.mode = mode
        self.host = host
        self.graph_id = graph_id
        self.ingest_query_builder = ingest_query_builder

    def make_query_executor(self) -> QueryExecutor:
        return NeptuneQueryExecutor(
            connection=self.connection,
            ingest_query_builder=self.ingest_query_builder,
        )

    def make_type_retriever(self) -> TypeRetriever:
        from .type_retriever import NeptuneDBTypeRetriever

        return NeptuneDBTypeRetriever(self)

    def make_migrator(self) -> TypeRetriever:
        raise NotImplementedError
