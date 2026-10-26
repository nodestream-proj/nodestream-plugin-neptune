import pytest
from hamcrest import assert_that, contains_inanyorder, equal_to, has_length
from nodestream.databases.query_executor import OperationOnNodeIdentity
from nodestream.model import (
    Node,
    NodeCreationRule,
    PropertySet,
    RelationshipCreationRule,
    RelationshipWithNodes,
)
from nodestream.schema import Schema
from nodestream.schema.state import Adjacency, AdjacencyCardinality, GraphObjectSchema

from nodestream_plugin_neptune.ingest_query_builder import NeptuneIngestQueryBuilder
from nodestream_plugin_neptune.neptune_connector import NeptuneConnector
from nodestream_plugin_neptune.neptune_query_executor import NeptuneQueryExecutor
from nodestream_plugin_neptune.type_retriever import (
    DEFAULT_PAGE_SIZE,
    EDGE_ID_PROPERTY,
    FETCH_ALL_NODES_BY_TYPE_QUERY_FORMAT,
    FETCH_ALL_RELATIONSHIPS_BY_TYPE_QUERY_FORMAT,
    RESERVED_ID,
    RESERVED_LABEL,
    RESERVED_TYPE,
    NeptuneDBTypeRetriever,
    NeptuneNodeExtractor,
    NeptuneRelationshipExtractor,
)

# The row-mapping helpers are static methods on NeptuneDBTypeRetriever (they
# carry no retriever state; the extractors reuse them). Alias them here so the
# mapping tests below read the same as when they were module-level functions.
map_neptune_node_to_nodestream_node = (
    NeptuneDBTypeRetriever.map_neptune_node_to_nodestream_node
)
map_neptune_relationship_to_nodestream_relationship = (
    NeptuneDBTypeRetriever.map_neptune_relationship_to_nodestream_relationship
)

NODE_TYPE_A = "Widget"
NODE_TYPE_B = "Gadget"
REL_TYPE = "LINKS_TO"


# ─── Fixtures ───────────────────────────────────────────────────────────


def build_schema() -> Schema:
    schema = Schema()
    widget = GraphObjectSchema(NODE_TYPE_A)
    widget.add_key(RESERVED_ID)
    schema.put_node_type(widget)
    gadget = GraphObjectSchema(NODE_TYPE_B)
    gadget.add_key(RESERVED_ID)
    schema.put_node_type(gadget)
    schema.put_relationship_type(GraphObjectSchema(REL_TYPE))
    schema.add_adjacency(
        Adjacency(NODE_TYPE_A, NODE_TYPE_B, REL_TYPE), AdjacencyCardinality()
    )
    return schema


@pytest.fixture
def connector(mocker):
    return mocker.Mock(NeptuneConnector)


@pytest.fixture
def subject(connector):
    return NeptuneDBTypeRetriever(connector, build_schema())


def executor_returning(mocker, connector, query_impl):
    query_executor = mocker.AsyncMock(NeptuneQueryExecutor)
    query_executor.query.side_effect = query_impl
    connector.make_query_executor.return_value = query_executor
    return query_executor


async def async_generator(*items):
    for item in items:
        yield item


# ─── Construction / defaults ──────────────────────────────────────────────


def test_default_page_size_is_large(connector):
    retriever = NeptuneDBTypeRetriever(connector)
    assert_that(retriever.limit, equal_to(10_000))
    assert_that(NeptuneDBTypeRetriever.DEFAULT_PAGE_SIZE, equal_to(10_000))
    assert_that(DEFAULT_PAGE_SIZE, equal_to(10_000))


def test_explicit_limit_overrides_default(connector):
    retriever = NeptuneDBTypeRetriever(connector, limit=100_000)
    assert_that(retriever.limit, equal_to(100_000))


def test_accepts_copy_command_kwargs(connector):
    retriever = NeptuneDBTypeRetriever(
        connector,
        build_schema(),
        limit=5000,
        preload_nodes=True,
        shard_size=1000,
        some_future_option="ignored",
    )
    assert_that(retriever.preload_nodes, equal_to(True))
    assert_that(retriever.shard_size, equal_to(1000))
    assert_that(retriever.limit, equal_to(5000))


# ─── build_histogram ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_build_histogram_counts_each_type(subject, connector, mocker):
    counts = {NODE_TYPE_A: 2, NODE_TYPE_B: 3, REL_TYPE: 5}

    def query_impl(query, params):
        for type_name, count in counts.items():
            if f":{type_name})" in query or f":{type_name}]" in query:
                return {"results": [{"count": count}]}
        return {"results": [{"count": 0}]}

    executor_returning(mocker, connector, query_impl)

    histogram = await subject.build_histogram()

    assert_that(histogram.node_counts, equal_to({NODE_TYPE_A: 2, NODE_TYPE_B: 3}))
    assert_that(histogram.relationship_counts, equal_to({REL_TYPE: 5}))
    assert_that(subject.histogram, equal_to(histogram))


@pytest.mark.asyncio
async def test_build_histogram_count_empty_is_zero(subject, connector, mocker):
    executor_returning(mocker, connector, lambda q, p: {"results": []})

    histogram = await subject.build_histogram()

    assert_that(histogram.node_counts, equal_to({NODE_TYPE_A: 0, NODE_TYPE_B: 0}))
    assert_that(histogram.relationship_counts, equal_to({REL_TYPE: 0}))


# ─── fetch_extractors ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_fetch_extractors_requires_histogram(subject):
    with pytest.raises(RuntimeError):
        [extractor async for extractor in subject.fetch_extractors()]


@pytest.mark.asyncio
async def test_fetch_extractors_yields_one_per_populated_type(
    subject, connector, mocker
):
    executor_returning(mocker, connector, lambda q, p: {"results": [{"count": 1}]})
    await subject.build_histogram()

    extractors = [extractor async for extractor in subject.fetch_extractors()]

    assert_that(extractors, has_length(3))  # 2 node types + 1 rel adjacency
    node_types = [
        e.node_type for e in extractors if isinstance(e, NeptuneNodeExtractor)
    ]
    rel_types = [
        e.relationship_type
        for e in extractors
        if isinstance(e, NeptuneRelationshipExtractor)
    ]
    assert_that(node_types, contains_inanyorder(NODE_TYPE_A, NODE_TYPE_B))
    assert_that(rel_types, equal_to([REL_TYPE]))


@pytest.mark.asyncio
async def test_fetch_extractors_skips_zero_count_types(subject, connector, mocker):
    def query_impl(query, params):
        if f":{NODE_TYPE_B})" in query:
            return {"results": [{"count": 4}]}
        return {"results": [{"count": 0}]}

    executor_returning(mocker, connector, query_impl)
    await subject.build_histogram()

    extractors = [extractor async for extractor in subject.fetch_extractors()]

    assert_that(extractors, has_length(1))
    assert_that(isinstance(extractors[0], NeptuneNodeExtractor), equal_to(True))
    assert_that(extractors[0].node_type, equal_to(NODE_TYPE_B))


@pytest.mark.asyncio
async def test_fetch_extractors_default_order_relationships_then_nodes(
    subject, connector, mocker
):
    executor_returning(mocker, connector, lambda q, p: {"results": [{"count": 1}]})
    await subject.build_histogram()

    kinds = [type(e).__name__ async for e in subject.fetch_extractors()]

    assert_that(kinds[0], equal_to("NeptuneRelationshipExtractor"))
    assert_that(kinds[-1], equal_to("NeptuneNodeExtractor"))


@pytest.mark.asyncio
async def test_fetch_extractors_preload_nodes_puts_nodes_first(connector, mocker):
    retriever = NeptuneDBTypeRetriever(connector, build_schema(), preload_nodes=True)
    executor_returning(mocker, connector, lambda q, p: {"results": [{"count": 1}]})
    await retriever.build_histogram()

    kinds = [type(e).__name__ async for e in retriever.fetch_extractors()]

    assert_that(kinds[0], equal_to("NeptuneNodeExtractor"))
    assert_that(kinds[-1], equal_to("NeptuneRelationshipExtractor"))


# ─── Typed extractor queries + paging + mapping ──────────────────────────────


def test_node_extractor_query_template(subject):
    extractor = NeptuneNodeExtractor(
        subject.connector, NODE_TYPE_A, subject.schema, subject.limit
    )
    expected = FETCH_ALL_NODES_BY_TYPE_QUERY_FORMAT.format(type=NODE_TYPE_A)
    assert_that(extractor.query, equal_to(expected))


def test_relationship_extractor_query_template(subject):
    extractor = NeptuneRelationshipExtractor(
        subject.connector,
        NODE_TYPE_A,
        NODE_TYPE_B,
        REL_TYPE,
        subject.schema,
        subject.limit,
    )
    expected = FETCH_ALL_RELATIONSHIPS_BY_TYPE_QUERY_FORMAT.format(
        from_type=NODE_TYPE_A, rel_type=REL_TYPE, to_type=NODE_TYPE_B
    )
    assert_that(extractor.query, equal_to(expected))


@pytest.mark.asyncio
async def test_node_extractor_pages_and_yields_ingestibles(subject, connector, mocker):
    pages = [
        {
            "results": [
                {
                    "n": {
                        "~id": "node-1",
                        "~labels": [NODE_TYPE_B],
                        "~properties": {"name": "alpha"},
                    }
                },
                {
                    "n": {
                        "~id": "node-2",
                        "~labels": [NODE_TYPE_B],
                        "~properties": {"name": "beta"},
                    }
                },
            ]
        },
        {"results": []},
    ]

    def query_impl(query, params):
        return (
            pages[params["offset"] // 2]
            if params["offset"] // 2 < len(pages)
            else {"results": []}
        )

    executor_returning(mocker, connector, query_impl)
    extractor = NeptuneNodeExtractor(connector, NODE_TYPE_B, build_schema(), limit=2)

    records = [record async for record in extractor.extract_records()]

    assert_that(records, has_length(2))

    source = records[0].source
    assert_that(source.type, equal_to(NODE_TYPE_B))
    assert_that(dict(source.key_values), equal_to({RESERVED_ID: "node-1"}))


@pytest.mark.asyncio
async def test_relationship_extractor_pages_and_yields_ingestibles(connector, mocker):
    pages = [
        {
            "results": [
                {
                    "a": {"~id": "a-1", "~labels": [NODE_TYPE_A], "~properties": {}},
                    "b": {"~id": "b-1", "~labels": [NODE_TYPE_B], "~properties": {}},
                    "r": {
                        "~id": "e-1",
                        "~type": REL_TYPE,
                        "~properties": {"score": 5},
                    },
                }
            ]
        },
        {"results": []},
    ]

    def query_impl(query, params):
        return (
            pages[params["offset"]]
            if params["offset"] < len(pages)
            else {"results": []}
        )

    executor_returning(mocker, connector, query_impl)
    extractor = NeptuneRelationshipExtractor(
        connector, NODE_TYPE_A, NODE_TYPE_B, REL_TYPE, build_schema(), limit=1
    )

    records = [record async for record in extractor.extract_records()]

    assert_that(records, has_length(1))


# ─────────────── Node mapping / native ~id exposure  ──────────────────────────
#
# The mapper offers the native Neptune ~id as selectable key material so a copy
# schema can key on ~id (unique) instead of a non-unique property.  ~id is optional
# in Neptune (a UUID is minted on write when absent), so the mapper must handle its
# absence. In nodestream 0.16 node identity comes from key_values, so when a schema
# is supplied the declared keys are split out of properties into key_values.
#
# ~id is key material ONLY. Whatever is not consumed as a key must not carry it
# along as a property: Neptune Database rejects ~id in a SET map (the whole batch
# is lost) and Neptune Analytics / Neo4j would store a literal "~id" property.


def test_node_never_carries_native_id_as_property_without_schema():
    node = {
        "~id": "node-42",
        "~labels": [NODE_TYPE_B],
        "~properties": {"name": "sample", "size": 2014},
    }
    result = map_neptune_node_to_nodestream_node(node, type=NODE_TYPE_B)

    assert_that(RESERVED_ID not in result.properties, equal_to(True))
    assert_that(result.properties["name"], equal_to("sample"))
    assert_that(result.properties["size"], equal_to(2014))
    assert_that(dict(result.key_values), equal_to({}))


def test_node_keyed_on_native_id_populates_key_values():
    schema = build_schema()
    node = {"~id": "node-1", "~labels": [NODE_TYPE_B], "~properties": {"name": "x"}}
    result = map_neptune_node_to_nodestream_node(node, type=NODE_TYPE_B, schema=schema)

    assert_that(dict(result.key_values), equal_to({RESERVED_ID: "node-1"}))
    assert_that(RESERVED_ID not in result.properties, equal_to(True))
    assert_that(result.has_valid_id, equal_to(True))


def test_node_keyed_on_user_property_drops_native_id():
    schema = Schema()
    widget = GraphObjectSchema(NODE_TYPE_A)
    widget.add_key("id")
    schema.put_node_type(widget)
    node = {
        "~id": "node-7",
        "~labels": [NODE_TYPE_A],
        "~properties": {"id": 12345, "label": "example"},
    }
    result = map_neptune_node_to_nodestream_node(node, type=NODE_TYPE_A, schema=schema)

    assert_that(dict(result.key_values), equal_to({"id": 12345}))
    # The native id is not the key here, so it is dropped, not left as a property.
    assert_that(RESERVED_ID not in result.properties, equal_to(True))
    assert_that(result.properties["label"], equal_to("example"))


def test_node_does_not_expose_label_as_property():
    node = {"~id": "node-1", "~labels": [NODE_TYPE_B], "~properties": {"name": "x"}}
    result = map_neptune_node_to_nodestream_node(node, type=NODE_TYPE_B)

    assert_that(RESERVED_LABEL not in result.properties, equal_to(True))
    assert_that(result.type, equal_to(NODE_TYPE_B))


def test_node_omits_native_id_when_absent():
    node = {"~labels": [NODE_TYPE_B], "~properties": {"name": "no id node"}}
    result = map_neptune_node_to_nodestream_node(node, type=NODE_TYPE_B)

    assert_that(RESERVED_ID not in result.properties, equal_to(True))


def test_node_multi_label_sets_additional_types():
    node = {"~id": "node-1", "~labels": [NODE_TYPE_B, "Extra"], "~properties": {}}
    result = map_neptune_node_to_nodestream_node(node, type=NODE_TYPE_B)

    assert_that(result.type, equal_to(NODE_TYPE_B))
    assert_that("Extra" in result.additional_types, equal_to(True))
    assert_that(RESERVED_LABEL not in result.properties, equal_to(True))


def test_node_list_properties_converted_to_tuples():
    node = {
        "~id": "node-1",
        "~labels": [NODE_TYPE_B],
        "~properties": {"tags": ["a", "b"]},
    }
    result = map_neptune_node_to_nodestream_node(node, type=NODE_TYPE_B)

    assert_that(result.properties["tags"], equal_to(("a", "b")))


def test_relationship_preserves_native_id_as_neptune_edge_id():
    rel = {"~id": "e-123", "~type": REL_TYPE, "~properties": {"score": 5}}
    result = map_neptune_relationship_to_nodestream_relationship(rel)

    assert_that(EDGE_ID_PROPERTY, equal_to("neptuneEdgeId"))
    assert_that(result.type, equal_to(REL_TYPE))
    assert_that(result.properties[EDGE_ID_PROPERTY], equal_to("e-123"))
    assert_that(result.properties["score"], equal_to(5))
    # Neptune Analytics rejects custom edge ids; never emit a `~id` property.
    assert_that(RESERVED_ID not in result.properties, equal_to(True))
    assert_that(RESERVED_TYPE not in result.properties, equal_to(True))


def test_relationship_omits_edge_id_when_absent():
    rel = {"~type": REL_TYPE, "~properties": {}}
    result = map_neptune_relationship_to_nodestream_relationship(rel)

    assert_that(EDGE_ID_PROPERTY not in result.properties, equal_to(True))
    assert_that(RESERVED_ID not in result.properties, equal_to(True))


# ──────── Page-size guidance warning, now driven by build_histogram ─────────


@pytest.mark.asyncio
async def test_build_histogram_warns_when_type_is_large(subject, connector, mocker):
    big = subject.PAGE_COUNT_WARN_THRESHOLD * subject.limit * 5

    def query_impl(query, params):
        if f":{NODE_TYPE_B})" in query:
            return {"results": [{"count": big}]}
        return {"results": [{"count": 1}]}

    executor_returning(mocker, connector, query_impl)
    warn = mocker.patch.object(subject.logger, "warning")

    await subject.build_histogram()

    warn.assert_called_once()
    recommended = warn.call_args.args[-1]
    assert_that(recommended > subject.limit, equal_to(True))


@pytest.mark.asyncio
async def test_build_histogram_does_not_warn_when_all_types_small(
    subject, connector, mocker
):
    executor_returning(
        mocker, connector, lambda q, p: {"results": [{"count": subject.limit}]}
    )
    warn = mocker.patch.object(subject.logger, "warning")

    await subject.build_histogram()

    warn.assert_not_called()


@pytest.mark.asyncio
async def test_warning_fires_once_per_type(subject, connector, mocker):
    big = subject.PAGE_COUNT_WARN_THRESHOLD * subject.limit * 5
    executor_returning(mocker, connector, lambda q, p: {"results": [{"count": big}]})
    warn = mocker.patch.object(subject.logger, "warning")

    await subject.build_histogram()
    await subject.build_histogram()

    assert_that(warn.call_count, equal_to(3))


# ──────── `~id` must never reach a Neptune SET clause (PR #41 review) ─────────
#
# Neptune Database rejects any write whose SET map carries `~id`
# (BadRequestException: "'~id' as a property in SET clause is not supported"),
# losing the whole UNWIND batch. Neptune Analytics accepts it but stores a
# literal property named `~id`. These tests drive the real mapper and the real
# query builder end to end and assert that what the COPY path produces never
# puts `~id` in the per-row write params (the map that becomes
# `SET node = ...` / `SET r = ...`). A backstop for non-copy sources lives in
# the query builder's own tests.

COPY_NODE_TYPE = "CopyTestPerson"
COPY_REL_TYPE = "CopyTestKNOWS"

NDB_NODE_RECORD = {
    "~id": "src-native-id-1",
    "~labels": [COPY_NODE_TYPE],
    "~properties": {"name": "Ada", "age": 36},
}


def _schema_keyed_on(key: str) -> Schema:
    schema = Schema()
    person = GraphObjectSchema(COPY_NODE_TYPE)
    person.add_key(key)
    schema.put_node_type(person)
    return schema


def _node_write_params(node) -> dict:
    operation = OperationOnNodeIdentity(node.identity_shape, NodeCreationRule.EAGER)
    batch = NeptuneIngestQueryBuilder().generate_batch_update_node_operation_batch(
        operation, [node]
    )
    (params,) = batch.parameters
    return params


def _relationship_write_params(relationship) -> dict:
    rel = RelationshipWithNodes(
        from_node=Node(COPY_NODE_TYPE, PropertySet({"name": "Ada"}), PropertySet({})),
        to_node=Node(COPY_NODE_TYPE, PropertySet({"name": "Bob"}), PropertySet({})),
        relationship=relationship,
        relationship_creation_rule=RelationshipCreationRule.EAGER,
    )
    return NeptuneIngestQueryBuilder().generate_update_rel_between_nodes_params(rel)


def test_copy_keyed_on_native_id_does_not_write_tilde_id():
    """Control: keying on `~id` moves it into key_values, out of the SET map."""
    node = map_neptune_node_to_nodestream_node(
        NDB_NODE_RECORD, type=COPY_NODE_TYPE, schema=_schema_keyed_on(RESERVED_ID)
    )
    params = _node_write_params(node)

    assert_that(RESERVED_ID not in params, equal_to(True))
    assert_that("src-native-id-1" in params["__node_id"], equal_to(True))
    assert_that(params["name"], equal_to("Ada"))


def test_copy_keyed_on_arbitrary_property_does_not_write_tilde_id():
    """Cole's case: key on `name`; the source `~id` must not reach the SET map."""
    node = map_neptune_node_to_nodestream_node(
        NDB_NODE_RECORD, type=COPY_NODE_TYPE, schema=_schema_keyed_on("name")
    )
    params = _node_write_params(node)

    assert_that(RESERVED_ID not in params, equal_to(True))
    assert_that(params["__node_id"], equal_to(f"{COPY_NODE_TYPE}_name:Ada"))
    assert_that(params["age"], equal_to(36))


def test_copy_without_schema_does_not_write_tilde_id():
    """No schema: nothing is moved into key_values, so `~id` stays in properties."""
    node = map_neptune_node_to_nodestream_node(NDB_NODE_RECORD, type=COPY_NODE_TYPE)
    params = _node_write_params(node)

    assert_that(RESERVED_ID not in params, equal_to(True))


def test_copied_relationship_does_not_write_tilde_id():
    """Regression guard for the neptuneEdgeId change."""
    relationship = map_neptune_relationship_to_nodestream_relationship(
        {"~id": "src-edge-1", "~type": COPY_REL_TYPE, "~properties": {"since": 2020}}
    )
    params = _relationship_write_params(relationship)

    assert_that(RESERVED_ID not in params, equal_to(True))
    assert_that(params[EDGE_ID_PROPERTY], equal_to("src-edge-1"))


# ──────── Declared keys that the source nodes do not store (PR #41 review) ────────
#
# Nodes written by this plugin keep their key only inside the composite ~id, not
# as properties. A copy schema keyed on a property the nodes do not store gets
# nodes with no identity, which nodestream core skips at DEBUG level: they vanish
# without a trace. The mapper therefore warns, once per type.


@pytest.fixture
def fresh_missing_key_warnings():
    import nodestream_plugin_neptune.type_retriever as tr

    tr._warn_missing_keys.cache_clear()
    yield tr
    tr._warn_missing_keys.cache_clear()


def test_missing_declared_key_warns_with_type_and_key(
    fresh_missing_key_warnings, mocker
):
    warn = mocker.patch.object(fresh_missing_key_warnings.logger, "warning")
    node = {"~id": "n-1", "~labels": [COPY_NODE_TYPE], "~properties": {"age": 36}}

    result = map_neptune_node_to_nodestream_node(
        node, type=COPY_NODE_TYPE, schema=_schema_keyed_on("name")
    )

    warn.assert_called_once()
    logged = warn.call_args.args[0] % warn.call_args.args[1:]
    assert_that(COPY_NODE_TYPE in logged, equal_to(True))
    assert_that("'name'" in logged, equal_to(True))
    assert_that("'~id'" in logged, equal_to(True))
    # The node is still produced (core decides what to do with it); it just has
    # no identity, and the native id still does not leak into its properties.
    assert_that(dict(result.key_values), equal_to({}))
    assert_that(RESERVED_ID not in result.properties, equal_to(True))


def test_missing_declared_key_warns_once_per_type_not_per_node(
    fresh_missing_key_warnings, mocker
):
    warn = mocker.patch.object(fresh_missing_key_warnings.logger, "warning")
    schema = _schema_keyed_on("name")
    for i in range(50):
        node = {"~id": f"n-{i}", "~labels": [COPY_NODE_TYPE], "~properties": {}}
        map_neptune_node_to_nodestream_node(node, type=COPY_NODE_TYPE, schema=schema)

    warn.assert_called_once()


def test_no_warning_when_declared_keys_are_present(fresh_missing_key_warnings, mocker):
    warn = mocker.patch.object(fresh_missing_key_warnings.logger, "warning")
    node = {"~id": "n-1", "~labels": [COPY_NODE_TYPE], "~properties": {"name": "Ada"}}

    result = map_neptune_node_to_nodestream_node(
        node, type=COPY_NODE_TYPE, schema=_schema_keyed_on("name")
    )
    map_neptune_node_to_nodestream_node(
        NDB_NODE_RECORD, type=COPY_NODE_TYPE, schema=_schema_keyed_on(RESERVED_ID)
    )

    warn.assert_not_called()
    assert_that(dict(result.key_values), equal_to({"name": "Ada"}))
