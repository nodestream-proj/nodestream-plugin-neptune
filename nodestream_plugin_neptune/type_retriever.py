import asyncio
from logging import getLogger
from typing import Any, AsyncGenerator, Optional

from nodestream.databases.copy import TypeHistogram, TypeRetriever
from nodestream.model import PropertySet, RelationshipWithNodes
from nodestream.model.creation_rules import NodeCreationRule, RelationshipCreationRule
from nodestream.model.graph_objects import Node, Relationship
from nodestream.pipeline import Extractor
from nodestream.schema import Schema

from .neptune_connector import NeptuneConnector
from .neptune_query_executor import NeptuneQueryExecutor

logger = getLogger(__name__)

# Reserved Neptune field names. `~`-prefixed names are reserved by Neptune and
# cannot collide with a real user property. These are optional on write: Neptune
# uses a UUID when `~id` is absent and defaults the label/type to `vertex`/`edge`.
RESERVED_ID = "~id"
RESERVED_LABEL = "~label"
RESERVED_TYPE = "~type"

# Property name that carries a source relationship's native `~id` on copy.
# Neptune Analytics does not support custom edge ids.  Ids are extracted and
# stored as a neptuneEdgeId property for provenance.
EDGE_ID_PROPERTY = "neptuneEdgeId"

# Bound on how many per-type count queries run concurrently while building the
# histogram. Keeps the preflight from opening a connection per type on a wide
# schema.
MAX_HISTOGRAM_CONCURRENCY = 20

# Page size for the SKIP/LIMIT scan used to walk each node/relationship type.
DEFAULT_PAGE_SIZE = 10_000

# Recommends larger page sizes if number of pages per type is high
PAGE_COUNT_WARN_THRESHOLD = 100
PAGE_COUNT_TARGET = 50


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
    # Retained as a class attribute for callers/tests that reference the default.
    DEFAULT_PAGE_SIZE = DEFAULT_PAGE_SIZE
    PAGE_COUNT_WARN_THRESHOLD = PAGE_COUNT_WARN_THRESHOLD
    PAGE_COUNT_TARGET = PAGE_COUNT_TARGET

    def __init__(
        self,
        connector: NeptuneConnector,
        schema: Optional[Schema] = None,
        *,
        limit: int = DEFAULT_PAGE_SIZE,
        preload_nodes: bool = False,
        shard_size: Optional[int] = None,
        **_ignored: Any,
    ) -> None:
        # schema is Optional only so unit tests can construct the retriever
        # without a full Schema; the copy path always passes one.
        super().__init__(schema=schema)
        self.connector = connector
        self.limit = limit
        self.preload_nodes = preload_nodes
        self.shard_size = shard_size
        self.logger = logger
        self.histogram: Optional[TypeHistogram] = None
        self._page_size_warned: set = set()

    # -- mapping helpers --------------------------------------------------------

    @staticmethod
    def _node_properties(node: dict) -> dict:
        """Build the nodestream property dict from a Neptune node record.

        - Multi-valued (list) properties are converted to tuples so they are
          hashable: nodestream core's debouncer uses property values as dict
          keys and crashes on lists. The write path converts them back to lists.
        - The native reserved id is exposed under the ``~id`` spelling (optional:
          Neptune mints a UUID on write when absent) so a copy schema can key on
          it. We do NOT expose ``~label``: the label is carried as the node
          ``type`` and a property literally named ``~label`` is reserved and
          unsafe to write.
        """
        properties = {
            k: tuple(v) if isinstance(v, list) else v
            for k, v in node.get("~properties", {}).items()
        }
        if "~id" in node:
            properties[RESERVED_ID] = node["~id"]
        return properties

    @staticmethod
    def map_neptune_node_to_nodestream_node(
        node: dict,
        type: Optional[str] = None,
        schema: Optional[Schema] = None,
    ) -> Node:
        """Map a Neptune node record to a nodestream ``Node``.

        When a ``schema`` is supplied, the node type's declared key properties
        are moved out of ``properties`` into ``key_values`` so the write path
        can synthesize a stable identity (nodestream 0.16 derives node identity
        from ``key_values``, not ``properties``). A schema that keys on ``~id``
        therefore makes the native Neptune id the node identity. When no schema
        is supplied (unit tests, ad-hoc mapping) all fields stay in
        ``properties`` and ``key_values`` is left empty.
        """
        labels = node.get("~labels", [])
        properties = NeptuneDBTypeRetriever._node_properties(node)
        type = type or (labels[0] if labels else None)

        key_values: dict[str, Any] = {}
        if schema is not None and type is not None:
            node_schema = schema.get_node_type_by_name(type)
            if node_schema is not None:
                for key_name in node_schema.keys:
                    if key_name in properties:
                        key_values[key_name] = properties.pop(key_name)

        return Node(
            type=type,
            key_values=PropertySet(key_values),
            properties=PropertySet(properties),
            additional_types=tuple(label for label in labels if label != type),
        )

    @staticmethod
    def map_neptune_relationship_to_nodestream_relationship(
        relationship: dict,
    ) -> Relationship:
        """Map a Neptune relationship record to a nodestream ``Relationship``.

        The native ``~id`` (when present) is preserved as the plain property
        ``neptuneEdgeId`` rather than ``~id``: Neptune Analytics does not support
        custom edge ids, and writing ``~id`` through a property map stores a
        literal property named ``~id`` that clashes with the reserved name.
        We do NOT expose ``~type`` as a property: it is carried as the
        relationship ``type`` and a property named ``~type`` is reserved and
        unsafe to write.
        """
        rel_type = relationship.get("~type", relationship.get("type", ""))
        properties = dict(relationship.get("~properties", {}))
        if "~id" in relationship:
            properties[EDGE_ID_PROPERTY] = relationship["~id"]
        return Relationship(
            type=rel_type,
            properties=PropertySet(properties),
        )

    # -- histogram / get counts --------------------------

    async def build_histogram(self) -> TypeHistogram:
        node_types = [node_type.name for node_type in self.schema.nodes]
        relationship_types = [
            relationship_type.name for relationship_type in self.schema.relationships
        ]
        node_counts = await self._gather_counts(
            node_types, COUNT_NODES_BY_TYPE_QUERY_FORMAT
        )
        relationship_counts = await self._gather_counts(
            relationship_types, COUNT_RELATIONSHIPS_BY_TYPE_QUERY_FORMAT
        )
        for type_name, count in {**node_counts, **relationship_counts}.items():
            self._warn_page_size_if_needed(type_name, count)
        self.histogram = TypeHistogram(
            node_counts=node_counts, relationship_counts=relationship_counts
        )
        return self.histogram

    async def _gather_counts(
        self, type_names: list[str], query_format: str
    ) -> dict[str, int]:
        semaphore = asyncio.Semaphore(MAX_HISTOGRAM_CONCURRENCY)

        async def bounded(type_name: str) -> int:
            async with semaphore:
                return await self._execute_count_query(
                    query_format.format(type=type_name)
                )

        counts = await asyncio.gather(*(bounded(t) for t in type_names))
        return dict(zip(type_names, counts))

    async def _execute_count_query(self, query: str) -> int:
        executor: NeptuneQueryExecutor = self.connector.make_query_executor()
        response = await executor.query(query, {})
        if response and response.get("results"):
            return response["results"][0]["count"]
        return 0

    def _warn_page_size_if_needed(self, type_name: str, count: int) -> None:
        """Emit a one-time, prescriptive warning when the configured page size
        is small relative to a type's size.

        Neptune DB only supports offset pagination for a full-type scan, whose
        per-page cost grows with depth. When a type needs many pages, the deep
        pages dominate and throughput decays. We know the exact count here, so
        we tell the user the specific ``limit`` to set.
        """
        if self.limit <= 0 or count <= self.PAGE_COUNT_WARN_THRESHOLD * self.limit:
            return
        if type_name in self._page_size_warned:
            return
        self._page_size_warned.add(type_name)
        recommended = -(-count // self.PAGE_COUNT_TARGET)  # ceil(count/target)
        magnitude = 10 ** (len(str(recommended)) - 1)
        recommended = -(-recommended // magnitude) * magnitude
        self.logger.warning(
            "Type %r has %d items but the read page size is %d (~%d pages). "
            "Neptune DB only offers offset pagination, whose per-page cost rises "
            "with depth, so many pages will slow the copy substantially. "
            "Re-run with `--retriever-option limit=%d` (or higher) for this copy.",
            type_name,
            count,
            self.limit,
            -(-count // self.limit),
            recommended,
        )

    # -- fetch_extractors  -------------

    def _verify_histogram_built(self) -> None:
        if self.histogram is None:
            raise RuntimeError(
                "build_histogram() must be called before fetch_extractors()"
            )

    async def fetch_extractors(self) -> AsyncGenerator[Extractor, None]:
        self._verify_histogram_built()
        if self.preload_nodes:
            for extractor in self._node_extractors():
                yield extractor
            for extractor in self._relationship_extractors():
                yield extractor
        else:
            for extractor in self._relationship_extractors():
                yield extractor
            for extractor in self._node_extractors():
                yield extractor

    def _node_extractors(self) -> list["NeptuneNodeExtractor"]:
        return [
            NeptuneNodeExtractor(self.connector, node_type, self.schema, self.limit)
            for node_type, count in self.histogram.node_counts.items()
            if count > 0
        ]

    def _relationship_extractors(self) -> list["NeptuneRelationshipExtractor"]:
        extractors: list[NeptuneRelationshipExtractor] = []
        for relationship_type, count in self.histogram.relationship_counts.items():
            if count <= 0:
                continue
            for adjacency in self.schema.get_adjacencies_by_relationship_type(
                relationship_type
            ):
                extractors.append(
                    NeptuneRelationshipExtractor(
                        self.connector,
                        adjacency.from_node_type,
                        adjacency.to_node_type,
                        relationship_type,
                        self.schema,
                        self.limit,
                    )
                )
        return extractors


class _NeptunePagingExtractor(Extractor):
    """Shared offset-pagination loop for a single node/relationship type.

    Pages a type with unordered ``SKIP $offset LIMIT $limit`` (see the module
    docstring for why Neptune uses a large unordered page rather than an ordered
    keyset/shard scan). Subclasses map each returned row and yield an ingestible.
    """

    def __init__(
        self,
        connector: NeptuneConnector,
        query: str,
        limit: int,
    ) -> None:
        self.connector = connector
        self.query = query
        self.limit = limit

    def map_row(self, row: dict):  # pragma: no cover - overridden
        raise NotImplementedError

    async def extract_records(self) -> AsyncGenerator[Any, None]:
        executor: NeptuneQueryExecutor = self.connector.make_query_executor()
        offset = 0
        while True:
            params = {"limit": self.limit, "offset": offset}
            response = await executor.query(self.query, params)
            rows = list(response["results"]) if response else []
            if not rows:
                break
            for row in rows:
                mapped = self.map_row(row)
                if mapped is not None:
                    yield mapped
            offset += self.limit


class NeptuneNodeExtractor(_NeptunePagingExtractor):
    """Pages one node type and yields mapped node ingestibles."""

    def __init__(
        self,
        connector: NeptuneConnector,
        node_type: str,
        schema: Schema,
        limit: int,
    ) -> None:
        super().__init__(
            connector,
            FETCH_ALL_NODES_BY_TYPE_QUERY_FORMAT.format(type=node_type),
            limit,
        )
        self.node_type = node_type
        self.schema = schema

    def map_row(self, row: dict):
        node = NeptuneDBTypeRetriever.map_neptune_node_to_nodestream_node(
            row["n"], type=self.node_type, schema=self.schema
        )
        return node.into_ingest()


class NeptuneRelationshipExtractor(_NeptunePagingExtractor):
    """Pages one relationship adjacency and yields mapped relationship ingestibles."""

    def __init__(
        self,
        connector: NeptuneConnector,
        from_node_type: str,
        to_node_type: str,
        relationship_type: str,
        schema: Schema,
        limit: int,
    ) -> None:
        super().__init__(
            connector,
            FETCH_ALL_RELATIONSHIPS_BY_TYPE_QUERY_FORMAT.format(
                from_type=from_node_type,
                rel_type=relationship_type,
                to_type=to_node_type,
            ),
            limit,
        )
        self.from_node_type = from_node_type
        self.to_node_type = to_node_type
        self.relationship_type = relationship_type
        self.schema = schema

    def map_row(self, row: dict):
        from_node = NeptuneDBTypeRetriever.map_neptune_node_to_nodestream_node(
            row["a"], type=self.from_node_type, schema=self.schema
        )
        to_node = NeptuneDBTypeRetriever.map_neptune_node_to_nodestream_node(
            row["b"], type=self.to_node_type, schema=self.schema
        )
        relationship = (
            NeptuneDBTypeRetriever.map_neptune_relationship_to_nodestream_relationship(
                row["r"]
            )
        )
        return RelationshipWithNodes(
            from_node=from_node,
            to_node=to_node,
            relationship=relationship,
            from_side_node_creation_rule=NodeCreationRule.EAGER,
            to_side_node_creation_rule=NodeCreationRule.EAGER,
            relationship_creation_rule=RelationshipCreationRule.EAGER,
        ).into_ingest()
