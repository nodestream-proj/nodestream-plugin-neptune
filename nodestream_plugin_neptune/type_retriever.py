import json
from typing import AsyncGenerator

from nodestream.databases.copy import TypeRetriever
from nodestream.model import PropertySet, RelationshipWithNodes
from nodestream.model.graph_objects import Node, Relationship

from .extractor import NeptuneDBExtractor
from .neptune_connector import NeptuneConnector

FETCH_ALL_NODES_BY_TYPE_QUERY_FORMAT = """
MATCH (n:{type})
RETURN n SKIP $offset LIMIT $limit
"""

FETCH_ALL_RELATIONSHIPS_BY_TYPE_QUERY_FORMAT = """
MATCH (a:{from_type})-[r:{rel_type}]->(b:{to_type})
RETURN a, r, b SKIP $offset LIMIT $limit
"""

COUNT_NODES_BY_TYPE_QUERY_FORMAT = """
MATCH (n:{type})
RETURN count(n) AS count
"""

COUNT_RELATIONSHIPS_BY_TYPE_QUERY_FORMAT = """
MATCH ()-[r:{type}]->()
RETURN count(r) AS count
"""


class NeptuneDBTypeRetriever(TypeRetriever):
    def __init__(self, connector: NeptuneConnector, limit: int = 100) -> None:
        self.connector = connector
        self.limit = limit

    def map_neptune_node_to_nodestream_node(self, node, type: str = None) -> Node:
        labels = node.get("~labels", [])
        # WORKAROUND: Convert list-valued properties to tuples so they are
        # hashable. The nodestream core debouncer uses property values as
        # dict keys and crashes on lists. Remove this once nodestream core
        # handles unhashable property types.
        properties = {
            k: tuple(v) if isinstance(v, list) else v
            for k, v in node.get("~properties", {}).items()
        }
        type = type or labels[0]
        return Node(
            type=type,
            properties=PropertySet(properties),
            additional_types=tuple(l for l in labels if l != type),
        )

    def map_neptune_relationship_to_nodestream_relationship(
        self, relationship
    ) -> Relationship:
        return Relationship(
            type=relationship.get("~type", relationship.get("type", "")),
            properties=PropertySet(relationship.get("~properties", {})),
        )

    def get_node_type_extractor(self, type: str) -> NeptuneDBExtractor:
        return NeptuneDBExtractor(
            FETCH_ALL_NODES_BY_TYPE_QUERY_FORMAT.format(type=type),
            self.connector,
            limit=self.limit,
        )

    def get_relationship_type_extractor(
        self, from_node_type: str, to_node_type: str, relationship_type: str
    ) -> NeptuneDBExtractor:
        return NeptuneDBExtractor(
            FETCH_ALL_RELATIONSHIPS_BY_TYPE_QUERY_FORMAT.format(
                from_type=from_node_type,
                rel_type=relationship_type,
                to_type=to_node_type,
            ),
            self.connector,
            limit=self.limit,
        )

    async def _execute_count_query(self, query: str) -> int:
        from .neptune_query_executor import NeptuneQueryExecutor

        executor: NeptuneQueryExecutor = self.connector.make_query_executor()
        response = await executor.query(query, {})
        if response and response.get("results"):
            return response["results"][0]["count"]
        return 0

    async def preview_node_count(self, node_type: str) -> int:
        query = COUNT_NODES_BY_TYPE_QUERY_FORMAT.format(type=node_type)
        return await self._execute_count_query(query)

    async def preview_relationship_count(self, relationship_type: str) -> int:
        query = COUNT_RELATIONSHIPS_BY_TYPE_QUERY_FORMAT.format(type=relationship_type)
        return await self._execute_count_query(query)

    async def get_nodes_of_type(self, node_type: str) -> AsyncGenerator[Node, None]:
        extractor = self.get_node_type_extractor(node_type)
        async for row in extractor.extract_records():
            yield self.map_neptune_node_to_nodestream_node(row["n"], type=node_type)

    async def get_relationships_of_type_between(
        self, from_node_type: str, to_node_type: str, relationship_type: str
    ) -> AsyncGenerator[RelationshipWithNodes, None]:
        extractor = self.get_relationship_type_extractor(
            from_node_type, to_node_type, relationship_type
        )
        async for row in extractor.extract_records():
            yield RelationshipWithNodes(
                from_node=self.map_neptune_node_to_nodestream_node(row["a"]),
                to_node=self.map_neptune_node_to_nodestream_node(row["b"]),
                relationship=self.map_neptune_relationship_to_nodestream_relationship(
                    row["r"]
                ),
            )
