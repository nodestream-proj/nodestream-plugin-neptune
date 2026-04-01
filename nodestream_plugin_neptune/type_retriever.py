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
    def __init__(self, connector: NeptuneConnector) -> None:
        self.connector = connector

    def map_neptune_node_to_nodestream_node(self, node: Node, type: str = None) -> Node:
        # NOTE: I don't think this will work in all cases.
        # But I think this will require shaking out in the future.
        type = type or next(iter(node.labels))
        return Node(
            type=type,
            properties=PropertySet(node),
            additional_types=tuple(label for label in node.labels if label != type),
        )

    def map_neptune_relationship_to_nodestream_relationship(
        self, relationship: Relationship
    ) -> Relationship:
        return Relationship(
            type=relationship.type,
            properties=PropertySet(relationship),
        )

    def get_node_type_extractor(self, type: str) -> NeptuneDBExtractor:
        return NeptuneDBExtractor(
            FETCH_ALL_NODES_BY_TYPE_QUERY_FORMAT.format(type=type), self.connector
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
        )

    async def _execute_count_query(self, query: str) -> int:
        from .neptune_query_executor import NeptuneQueryExecutor

        executor: NeptuneQueryExecutor = self.connector.make_query_executor()
        response = await executor.query(query, json.dumps({}))
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
