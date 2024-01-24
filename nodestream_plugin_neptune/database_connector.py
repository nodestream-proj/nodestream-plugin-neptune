from nodestream.databases import TypeRetriever, DatabaseConnector
from nodestream.databases.query_executor import QueryExecutor

from .query_executor import NeptuneQueryExecutor
from .type_retriever import NeptuneTypeRetriever


class NeptuneDatabaseConnector(DatabaseConnector, alias="neptune"):
    """A Connector for a Neptune Graph Databases.

    This class is responsible for creating the various components needed for
    nodestream to interact with a Neptune database. It is also responsible
    for providing the configuration options for the Neo4j database.
    """

    # TODO: Implement this.
    pass
