import math
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest
from hamcrest import assert_that, equal_to, equal_to_ignoring_whitespace
from nodestream.databases.query_executor import (
    OperationOnNodeIdentity,
    OperationOnRelationshipIdentity,
)
from nodestream.model import (
    Node,
    NodeCreationRule,
    PropertySet,
    Relationship,
    RelationshipCreationRule,
    RelationshipWithNodes,
    TimeToLiveConfiguration,
)
from nodestream.schema import GraphObjectType
from nodestream_plugin_neptune.ingest_query_builder import (
    NeptuneIngestQueryBuilder,
    _convert_unsupported_values,
    _drop_reserved_properties,
    _warn_reserved_property_dropped,
)
from nodestream_plugin_neptune.query import Query, QueryBatch
from pandas import Timestamp


@pytest.fixture
def query_builder():
    return NeptuneIngestQueryBuilder()


GREATEST_DAY = Timestamp(
    1998, 3, 25, 2, 0, 1
).timestamp()  # The Neptune plugin loads stores datetime's as epoch time.

BASIC_NODE_TTL = TimeToLiveConfiguration(
    graph_object_type=GraphObjectType.NODE,
    object_type="TestNodeType",
    expiry_in_hours=10,
)
BASIC_NODE_TTL_EXPECTED_QUERY = Query(
    "MATCH (x: TestNodeType) WHERE x.last_ingested_at <= $earliest_allowed_time DETACH DELETE x",
    {"earliest_allowed_time": GREATEST_DAY},
)

NODE_TTL_WITH_CUSTOM_QUERY = TimeToLiveConfiguration(
    graph_object_type=GraphObjectType.NODE,
    object_type="TestNodeType",
    custom_query="MATCH (n:TestNodeType) RETURN n",
    expiry_in_hours=10,
)

BASIC_REL_TTL = TimeToLiveConfiguration(
    graph_object_type=GraphObjectType.RELATIONSHIP,
    object_type="IS_RELATED_TO",
    expiry_in_hours=10,
)
BASIC_REL_TTL_EXPECTED_QUERY = Query(
    "MATCH ()-[x: IS_RELATED_TO]->() WHERE x.last_ingested_at <= $earliest_allowed_time DELETE x",
    {"earliest_allowed_time": GREATEST_DAY},
)


@patch("pandas.Timestamp.utcnow")
@pytest.mark.parametrize(
    "ttl,expected_query",
    [
        (BASIC_NODE_TTL, BASIC_NODE_TTL_EXPECTED_QUERY),
        (BASIC_REL_TTL, BASIC_REL_TTL_EXPECTED_QUERY),
    ],
)
def test_generates_expected_queries(mocked_utcnow, query_builder, ttl, expected_query):
    mocked_utcnow.return_value = Timestamp(1998, 3, 25, 12, 0, 1)
    resultant_query = query_builder.generate_ttl_query_from_configuration(ttl)
    assert_that(resultant_query, equal_to(expected_query))


def convert_timestamps(properties: PropertySet):
    for k, v in properties.items():
        if isinstance(v, Timestamp):
            properties[k] = v.timestamp()
    return properties


# In a simple node case, we should MERGE the node on the basis of its identity shape
# and then set all of the properties on the node.
SIMPLE_NODE = Node("TestType", {"id": "foo"})
SIMPLE_NODE_EXPECTED_QUERY = QueryBatch(
    'MERGE (node: TestType {`~id` : param.__node_id}) ON CREATE SET node = removeKeyFromMap(param, "__node_id") ON MATCH SET node += removeKeyFromMap(param, "__node_id")',
    [
        {
            "__node_id": "TestType_id:foo",
            "id": "foo",
            **convert_timestamps(SIMPLE_NODE.properties),
        }
    ],
)

SIMPLE_NODE_EXPECTED_QUERY_ON_MATCH = QueryBatch(
    'MATCH (node: TestType {`~id` : param.__node_id}) SET node += removeKeyFromMap(param, "__node_id")',
    [
        {
            "__node_id": "TestType_id:foo",
            "id": "foo",
            **convert_timestamps(SIMPLE_NODE.properties),
        }
    ],
)

# In a more complex node case, we should still MERGE the node on the basis of its identity shape
# but, in addition, we should add any additional labels that the node has.
COMPLEX_NODE = Node(
    "ComplexType",
    {"id": "foo"},
    additional_types=("ExtraTypeOne", "ExtraTypeTwo"),
)
COMPLEX_NODE_EXPECTED_QUERY = QueryBatch(
    'MERGE (node: ComplexType {`~id` : param.__node_id}) ON CREATE SET node = removeKeyFromMap(param, "__node_id") ON MATCH SET node += removeKeyFromMap(param, "__node_id") SET node:ExtraTypeOne:ExtraTypeTwo',
    [
        {
            "__node_id": "ComplexType_id:foo",
            "id": "foo",
            **convert_timestamps(COMPLEX_NODE.properties),
        }
    ],
)

COMPLEX_NODE_TWO = Node(
    "ComplexType",
    {"id_part1": "foo", "id_part2": "bar"},
    additional_types=("ExtraTypeOne", "ExtraTypeTwo"),
)

COMPLEX_NODE_TWO_EXPECTED_QUERY = QueryBatch(
    'MERGE (node: ComplexType {`~id` : param.__node_id}) ON CREATE SET node = removeKeyFromMap(param, "__node_id") ON MATCH SET node += removeKeyFromMap(param, "__node_id") SET node:ExtraTypeOne:ExtraTypeTwo',
    [
        {
            "__node_id": "ComplexType_id_part1:foo_id_part2:bar",
            "id_part1": "foo",
            "id_part2": "bar",
            **convert_timestamps(COMPLEX_NODE_TWO.properties),
        }
    ],
)


@pytest.mark.parametrize(
    "node,expected_query,node_creation_rule",
    [
        [SIMPLE_NODE, SIMPLE_NODE_EXPECTED_QUERY, NodeCreationRule.EAGER],
        [COMPLEX_NODE, COMPLEX_NODE_EXPECTED_QUERY, NodeCreationRule.EAGER],
        [COMPLEX_NODE_TWO, COMPLEX_NODE_TWO_EXPECTED_QUERY, NodeCreationRule.EAGER],
        [SIMPLE_NODE, SIMPLE_NODE_EXPECTED_QUERY_ON_MATCH, NodeCreationRule.MATCH_ONLY],
    ],
)
def test_node_update_generates_expected_queries(
    query_builder, node, expected_query, node_creation_rule
):
    operation = OperationOnNodeIdentity(node.identity_shape, node_creation_rule)
    query = query_builder.generate_batch_update_node_operation_batch(operation, [node])
    assert_that(query, equal_to(expected_query))


RELATIONSHIP_BETWEEN_TWO_NODES = RelationshipWithNodes(
    from_node=SIMPLE_NODE,
    to_node=COMPLEX_NODE,
    relationship=Relationship("RELATED_TO"),
)

RELATIONSHIP_BETWEEN_TWO_NODES_EXPECTED_QUERY = QueryBatch(
    "MATCH (from_node: TestType {`~id` : param.__from_node_id}) , (to_node: ComplexType {`~id` : param.__to_node_id}) "
    "MERGE (from_node)-[rel: RELATED_TO]->(to_node) "
    'ON CREATE SET rel = removeKeyFromMap(removeKeyFromMap(param, "__from_node_id"), "__to_node_id") '
    'ON MATCH SET rel += removeKeyFromMap(removeKeyFromMap(param, "__from_node_id"), "__to_node_id")',
    [
        {
            "__from_node_id": "TestType_id:foo",
            "__to_node_id": "ComplexType_id:foo",
            **convert_timestamps(
                RELATIONSHIP_BETWEEN_TWO_NODES.relationship.properties
            ),
        }
    ],
)

RELATIONSHIP_BETWEEN_TWO_NODES_WITH_MULTI_KEY = RelationshipWithNodes(
    from_node=SIMPLE_NODE,
    to_node=COMPLEX_NODE_TWO,
    relationship=Relationship("RELATED_TO"),
)

RELATIONSHIP_BETWEEN_TWO_NODES_EXPECTED_QUERY_WITH_MULTI_KEY = QueryBatch(
    "MATCH (from_node: TestType {`~id` : param.__from_node_id}) , (to_node: ComplexType {`~id` : param.__to_node_id}) "
    "MERGE (from_node)-[rel: RELATED_TO]->(to_node) "
    'ON CREATE SET rel = removeKeyFromMap(removeKeyFromMap(param, "__from_node_id"), "__to_node_id") '
    'ON MATCH SET rel += removeKeyFromMap(removeKeyFromMap(param, "__from_node_id"), "__to_node_id")',
    [
        {
            "__from_node_id": "TestType_id:foo",
            "__to_node_id": "ComplexType_id_part1:foo_id_part2:bar",
            **convert_timestamps(
                RELATIONSHIP_BETWEEN_TWO_NODES_WITH_MULTI_KEY.relationship.properties
            ),
        }
    ],
)

RELATIONSHIP_BETWEEN_TWO_NODES_WITH_MULTI_KEY_AND_CREATE = RelationshipWithNodes(
    from_node=SIMPLE_NODE,
    to_node=COMPLEX_NODE_TWO,
    relationship=Relationship("RELATED_TO"),
    relationship_creation_rule=RelationshipCreationRule.CREATE,
)

RELATIONSHIP_BETWEEN_TWO_NODES_EXPECTED_QUERY_WITH_MULTI_KEY_AND_CREATE = QueryBatch(
    "MATCH (from_node: TestType {`~id` : param.__from_node_id}) , (to_node: ComplexType {`~id` : param.__to_node_id}) "
    "CREATE (from_node)-[rel: RELATED_TO]->(to_node) "
    'SET rel += removeKeyFromMap(removeKeyFromMap(param, "__from_node_id"), "__to_node_id")',
    [
        {
            "__from_node_id": "TestType_id:foo",
            "__to_node_id": "ComplexType_id_part1:foo_id_part2:bar",
            **convert_timestamps(
                RELATIONSHIP_BETWEEN_TWO_NODES_WITH_MULTI_KEY_AND_CREATE.relationship.properties
            ),
        }
    ],
)


@pytest.mark.parametrize(
    "rel,expected_query",
    [
        [RELATIONSHIP_BETWEEN_TWO_NODES, RELATIONSHIP_BETWEEN_TWO_NODES_EXPECTED_QUERY],
        [
            RELATIONSHIP_BETWEEN_TWO_NODES_WITH_MULTI_KEY,
            RELATIONSHIP_BETWEEN_TWO_NODES_EXPECTED_QUERY_WITH_MULTI_KEY,
        ],
        [
            RELATIONSHIP_BETWEEN_TWO_NODES_WITH_MULTI_KEY_AND_CREATE,
            RELATIONSHIP_BETWEEN_TWO_NODES_EXPECTED_QUERY_WITH_MULTI_KEY_AND_CREATE,
        ],
    ],
)
def test_relationship_update_generates_expected_queries(
    query_builder, rel, expected_query
):
    to_op = OperationOnNodeIdentity(rel.to_node.identity_shape, NodeCreationRule.EAGER)
    from_op = OperationOnNodeIdentity(
        rel.from_node.identity_shape, NodeCreationRule.MATCH_ONLY
    )
    operation = OperationOnRelationshipIdentity(
        from_op,
        to_op,
        rel.relationship.identity_shape,
        relationship_creation_rule=rel.relationship_creation_rule,
    )
    query = query_builder.generate_batch_update_relationship_query_batch(
        operation, [rel]
    )
    assert_that(
        query.query_statement,
        equal_to_ignoring_whitespace(expected_query.query_statement),
    )
    assert_that(query.parameters, equal_to(expected_query.parameters))


def test_node_update_with_label_excluded_from_id():
    query_builder: NeptuneIngestQueryBuilder = NeptuneIngestQueryBuilder(
        include_label_in_id=False
    )

    expected_query: QueryBatch = QueryBatch(
        COMPLEX_NODE_TWO_EXPECTED_QUERY.query_statement,
        [
            {
                "__node_id": "id_part1:foo_id_part2:bar",
                "id_part1": "foo",
                "id_part2": "bar",
                **convert_timestamps(COMPLEX_NODE_TWO.properties),
            }
        ],
    )

    test_node_update_generates_expected_queries(
        query_builder=query_builder,
        node=COMPLEX_NODE_TWO,
        expected_query=expected_query,
        node_creation_rule=NodeCreationRule.EAGER,
    )


def test_relationship_update_with_label_excluded_from_id():
    query_builder: NeptuneIngestQueryBuilder = NeptuneIngestQueryBuilder(
        include_label_in_id=False
    )

    expected_query: QueryBatch = QueryBatch(
        RELATIONSHIP_BETWEEN_TWO_NODES_EXPECTED_QUERY_WITH_MULTI_KEY_AND_CREATE.query_statement,
        [
            {
                "__from_node_id": "id:foo",
                "__to_node_id": "id_part1:foo_id_part2:bar",
                **convert_timestamps(
                    RELATIONSHIP_BETWEEN_TWO_NODES_WITH_MULTI_KEY_AND_CREATE.relationship.properties
                ),
            }
        ],
    )

    test_relationship_update_generates_expected_queries(
        query_builder=query_builder,
        rel=RELATIONSHIP_BETWEEN_TWO_NODES_WITH_MULTI_KEY_AND_CREATE,
        expected_query=expected_query,
    )


def test_generate_params_in_order():
    node = Node("ComplexType", {"id_part1": "foo", "id_part2": "bar"})

    query_builder: NeptuneIngestQueryBuilder = NeptuneIngestQueryBuilder()
    key_params = query_builder.generate_node_key_params(node)
    expected = {"__node_id": "ComplexType_id_part1:foo_id_part2:bar"}
    assert_that(
        key_params,
        equal_to(expected),
    )


def test_generate_params_reverse_order():
    node = Node("ComplexType", {"id_part2": "bar", "id_part1": "foo"})

    query_builder: NeptuneIngestQueryBuilder = NeptuneIngestQueryBuilder()
    key_params = query_builder.generate_node_key_params(node)
    expected = {"__node_id": "ComplexType_id_part1:foo_id_part2:bar"}
    assert_that(
        key_params,
        equal_to(expected),
    )


def test_generate_params_null_value():
    node = Node("ComplexType", {"id_part1": None})

    query_builder: NeptuneIngestQueryBuilder = NeptuneIngestQueryBuilder()
    key_params = query_builder.generate_node_key_params(node)
    expected = {"__node_id": "ComplexType_id_part1:"}
    assert_that(
        key_params,
        equal_to(expected),
    )


def test_convert_unsupported_values_multivalued_collapses_lossy():
    # Multi-valued properties arrive as tuples (the type retriever converts
    # source lists to tuples for debouncer hashability). Neptune openCypher
    # CANNOT write an array value on either backend (NA: "Unsupported property
    # value provided"; NDB: "Property value must be a simple literal"), and NA
    # has no Gremlin endpoint, so there is no faithful write path. We collapse
    # to the first value (lossy) and keep scalars untouched.
    props = {"name": ("primary", "secondary"), "size": 2002}
    converted = _convert_unsupported_values(props)
    assert_that(converted["name"], equal_to("primary"))
    assert_that(isinstance(converted["name"], str), equal_to(True))
    assert_that(converted["size"], equal_to(2002))


def test_convert_unsupported_values_list_collapses_lossy():
    # Defensive: a raw list (not tuple) is collapsed the same way.
    props = {"tags": ["first", "second", "third"]}
    converted = _convert_unsupported_values(props)
    assert_that(converted["tags"], equal_to("first"))


def test_convert_unsupported_values_single_element_collapses_to_scalar():
    props = {"name": ("only",)}
    converted = _convert_unsupported_values(props)
    assert_that(converted["name"], equal_to("only"))


def test_convert_unsupported_values_empty_multivalued_dropped():
    # An empty multi-valued property has no value to keep, so it is dropped
    # (None removes the property on write).
    props = {"tags": ()}
    converted = _convert_unsupported_values(props)
    assert_that(converted["tags"], equal_to(None))


def test_convert_unsupported_values_logs_dropped_values(mocker):
    # The lossy collapse must be visible: every multi-valued property that loses
    # data logs a warning naming the property and the dropped values.
    import nodestream_plugin_neptune.ingest_query_builder as iqb

    warn = mocker.patch.object(iqb.logger, "warning")
    _convert_unsupported_values(
        {"name": ("keep", "drop1", "drop2")}, context="node of type 'Widget'"
    )
    warn.assert_called_once()
    # The dropped values and context appear in the log args.
    args = warn.call_args.args
    assert_that("Widget" in str(args), equal_to(True))


# ──────── Values the plugin cannot write must fail loudly, not vanish ─────────
#
# Neptune parameters travel as JSON (Neptune Database) or a botocore document
# (Neptune Analytics). A value outside str/bool/int/float/None (and datetime,
# which is converted) cannot be serialized. That used to be discovered inside
# the connection's retry/swallow path, which logged "Unexpected error", dropped
# the whole batch, and let the run exit 0. These tests pin the early, loud
# failure that replaces it.

# Stand-in for a source-driver type the plugin must not know about (the real
# case is neo4j.time.DateTime leaking through a Neo4j -> Neptune copy).
DriverDateTime = type("DateTime", (), {"__module__": "neo4j.time"})


@pytest.mark.parametrize(
    "value, type_name",
    [
        (Decimal("1.5"), "decimal.Decimal"),
        (b"raw-bytes", "bytes"),
        (date(2024, 1, 1), "datetime.date"),
        (timedelta(days=1), "datetime.timedelta"),
        (DriverDateTime(), "neo4j.time.DateTime"),
        ({"nested": 1}, "dict"),
        ({1, 2}, "set"),
    ],
)
def test_convert_unsupported_values_rejects_unwritable_types(value, type_name):
    with pytest.raises(TypeError) as excinfo:
        _convert_unsupported_values({"when": value}, context="node of type 'Widget'")
    message = str(excinfo.value)
    assert_that("'when'" in message, equal_to(True))
    assert_that("node of type 'Widget'" in message, equal_to(True))
    assert_that(type_name in message, equal_to(True))
    # The message tells the user what IS supported.
    assert_that("str, bool, int, float" in message, equal_to(True))


def test_convert_unsupported_values_accepts_writable_types_unchanged():
    props = {"s": "text", "i": 7, "f": 1.5, "b": True, "n": None, "big": 2**40}
    assert_that(_convert_unsupported_values(dict(props)), equal_to(props))


def test_convert_unsupported_values_converts_datetimes_to_posix_seconds():
    moment = datetime(2024, 4, 1, 23, 59, 59, tzinfo=timezone.utc)
    converted = _convert_unsupported_values({"dt": moment, "ts": Timestamp(moment)})
    assert_that(converted["dt"], equal_to(moment.timestamp()))
    assert_that(converted["ts"], equal_to(moment.timestamp()))


def test_convert_unsupported_values_validates_the_value_kept_by_collapse():
    # The collapse keeps the FIRST element, so that element is what gets written
    # and is what must be validated.
    with pytest.raises(TypeError) as excinfo:
        _convert_unsupported_values(
            {"tags": (Decimal("1"), "second")}, context="node of type 'Widget'"
        )
    assert_that("decimal.Decimal" in str(excinfo.value), equal_to(True))

    # A nested list kept by the collapse is still a list: not writable.
    with pytest.raises(TypeError):
        _convert_unsupported_values({"matrix": ((1, 2), (3, 4))})


def test_convert_unsupported_values_converts_datetime_kept_by_collapse():
    # Regression: a multi-valued datetime property used to be collapsed to a raw
    # datetime that was never converted, and then failed in json.dumps.
    moment = datetime(2024, 4, 1, tzinfo=timezone.utc)
    converted = _convert_unsupported_values({"seen": (moment, moment)})
    assert_that(converted["seen"], equal_to(moment.timestamp()))


def test_convert_unsupported_values_still_rejects_non_finite_numbers():
    with pytest.raises(ValueError):
        _convert_unsupported_values({"x": math.nan})
    with pytest.raises(ValueError):
        _convert_unsupported_values({"x": (math.inf, 1.0)})


def test_node_params_reject_unwritable_value_with_context():
    node = Node(
        "Widget",
        PropertySet({"sku": "a-1"}),
        PropertySet({"last_ingested_at": DriverDateTime()}),
    )
    with pytest.raises(TypeError) as excinfo:
        NeptuneIngestQueryBuilder().generate_update_node_operation_params(node)
    message = str(excinfo.value)
    assert_that("'last_ingested_at'" in message, equal_to(True))
    assert_that("node of type 'Widget'" in message, equal_to(True))
    assert_that("neo4j.time.DateTime" in message, equal_to(True))


def test_relationship_params_reject_unwritable_value_with_context():
    rel = Relationship("KNOWS", properties=PropertySet({"since": Decimal("2020")}))
    with pytest.raises(TypeError) as excinfo:
        NeptuneIngestQueryBuilder().generate_update_rel_params(rel)
    message = str(excinfo.value)
    assert_that("'since'" in message, equal_to(True))
    assert_that("relationship of type 'KNOWS'" in message, equal_to(True))


# ──────── `~id` must never be sent as a property (PR #41 review) ─────────
#
# Neptune Database rejects `~id` in a SET map ("'~id' as a property in SET clause
# is not supported") and the whole UNWIND batch is lost; Neptune Analytics accepts
# it and stores a literal property named "~id". Copying FROM Neptune used to leak
# the native id into node properties whenever the schema keyed on something else;
# the mapper no longer does, and this is the write-side backstop for every other
# source (and for user pipelines). Verified on both engines: `~id` is the only
# reserved name that misbehaves. `~label`, `~type`, `~start`, `~end` and arbitrary
# `~x` names are accepted as ordinary properties, so only `~id` is stripped.


@pytest.fixture
def fresh_reserved_warnings():
    _warn_reserved_property_dropped.cache_clear()
    yield
    _warn_reserved_property_dropped.cache_clear()


def test_drop_reserved_properties_removes_only_the_native_id():
    props = {"~id": "src-1", "name": "Ada", "~label": "kept", "~custom": 1}

    cleaned = _drop_reserved_properties(props, context="node of type 'Widget'")

    assert_that(cleaned, equal_to({"name": "Ada", "~label": "kept", "~custom": 1}))
    # The caller's mapping is not mutated.
    assert_that("~id" in props, equal_to(True))


def test_drop_reserved_properties_is_a_noop_without_reserved_names():
    props = {"name": "Ada", "age": 36}
    assert_that(_drop_reserved_properties(props), equal_to(props))


def test_drop_reserved_properties_warns_once_per_name_and_context(
    fresh_reserved_warnings, mocker
):
    import nodestream_plugin_neptune.ingest_query_builder as iqb

    warn = mocker.patch.object(iqb.logger, "warning")
    for _ in range(25):
        _drop_reserved_properties({"~id": "x"}, context="node of type 'Widget'")
    warn.assert_called_once()
    logged = warn.call_args.args[0] % warn.call_args.args[1:]
    assert_that("'~id'" in logged, equal_to(True))
    assert_that("node of type 'Widget'" in logged, equal_to(True))

    # A different type is a different situation and is reported again.
    _drop_reserved_properties({"~id": "x"}, context="node of type 'Gadget'")
    assert_that(warn.call_count, equal_to(2))


def test_node_params_never_carry_the_native_id_as_a_property():
    node = Node(
        "Widget",
        PropertySet({"sku": "a-1"}),
        PropertySet({"~id": "src-native-id", "age": 36}),
    )

    params = NeptuneIngestQueryBuilder().generate_update_node_operation_params(node)

    assert_that("~id" in params, equal_to(False))
    # Identity comes from the key, untouched by the strip.
    assert_that(params["__node_id"], equal_to("Widget_sku:a-1"))
    assert_that(params["age"], equal_to(36))


def test_relationship_params_never_carry_the_native_id_as_a_property():
    rel = Relationship(
        "KNOWS", properties=PropertySet({"~id": "src-edge-1", "since": 2020})
    )

    params = NeptuneIngestQueryBuilder().generate_update_rel_params(rel)

    assert_that("~id" in params, equal_to(False))
    assert_that(params["since"], equal_to(2020))


def test_relationship_key_values_are_never_stripped():
    # Only PROPERTIES are sanitized. Key values are identity: removing one would
    # silently change what the MERGE matches on.
    rel = Relationship(
        "KNOWS",
        key_values=PropertySet({"~id": "edge-key"}),
        properties=PropertySet({"~id": "ignored", "since": 2020}),
    )

    params = NeptuneIngestQueryBuilder().generate_update_rel_params(rel)

    assert_that(params["~id"], equal_to("edge-key"))
    assert_that(params["since"], equal_to(2020))


def test_other_reserved_looking_names_still_flow_through_to_the_write():
    node = Node(
        "Widget",
        PropertySet({"sku": "a-1"}),
        PropertySet({"~label": "x", "~type": "y"}),
    )
    params = NeptuneIngestQueryBuilder().generate_update_node_operation_params(node)
    assert_that(params["~label"], equal_to("x"))
    assert_that(params["~type"], equal_to("y"))


# Node key values are also written as properties. Identity stays in `~id`; the
# key properties keep key-only values queryable and readable as a copy source.


def test_key_only_node_writes_its_key_as_a_property():
    node = Node("Number", PropertySet({"number": 42}))

    params = NeptuneIngestQueryBuilder().generate_update_node_operation_params(node)

    assert_that(params["__node_id"], equal_to("Number_number:42"))
    assert_that(params["number"], equal_to(42))


def test_every_part_of_a_composite_key_is_written_as_a_property():
    node = Node(
        "Person",
        PropertySet({"first_name": "zach", "last_name": "probst"}),
        PropertySet({"age": 30}),
    )

    params = NeptuneIngestQueryBuilder().generate_update_node_operation_params(node)

    assert_that(
        params["__node_id"], equal_to("Person_first_name:zach_last_name:probst")
    )
    assert_that(params["first_name"], equal_to("zach"))
    assert_that(params["last_name"], equal_to("probst"))
    assert_that(params["age"], equal_to(30))


def test_declared_property_wins_over_a_normalized_key_of_the_same_name():
    # Keys are lowercased by default; the declared property keeps the original
    # value. Identity still uses the normalized key.
    node = Node(
        "Person",
        PropertySet({"first_name": "zach"}),
        PropertySet({"first_name": "Zach"}),
    )

    params = NeptuneIngestQueryBuilder().generate_update_node_operation_params(node)

    assert_that(params["__node_id"], equal_to("Person_first_name:zach"))
    assert_that(params["first_name"], equal_to("Zach"))


def test_native_id_key_is_not_written_as_a_property():
    # A copy schema can key on `~id`. It is already the node's identity and
    # Neptune Database rejects `~id` as a property, so it is left out.
    node = Node(
        "Widget",
        PropertySet({"~id": "Widget_sku:a-1"}),
        PropertySet({"name": "w"}),
    )

    params = NeptuneIngestQueryBuilder().generate_update_node_operation_params(node)

    assert_that("~id" in params, equal_to(False))
    assert_that(params["__node_id"], equal_to("Widget_~id:Widget_sku:a-1"))
    assert_that(params["name"], equal_to("w"))


def test_key_values_go_through_value_conversion():
    moment = datetime(2024, 3, 15, 12, 30, 45, tzinfo=timezone.utc)
    node = Node("Event", PropertySet({"at": moment}))

    params = NeptuneIngestQueryBuilder().generate_update_node_operation_params(node)

    assert_that(params["at"], equal_to(moment.timestamp()))


def test_writing_key_properties_does_not_mutate_the_node():
    node = Node("Number", PropertySet({"number": 1}), PropertySet({"label": "one"}))

    NeptuneIngestQueryBuilder().generate_update_node_operation_params(node)

    assert_that(dict(node.properties), equal_to({"label": "one"}))
    assert_that(dict(node.key_values), equal_to({"number": 1}))


def test_relationship_endpoint_params_are_identity_only():
    # Relationship queries only MATCH their endpoints, so endpoint key values are
    # not sent there; the endpoint nodes get their key properties from their own
    # node writes.
    rel = RelationshipWithNodes(
        from_node=Node("Person", PropertySet({"first_name": "a"})),
        to_node=Node("Person", PropertySet({"first_name": "b"})),
        relationship=Relationship("KNOWS"),
    )

    params = NeptuneIngestQueryBuilder().generate_update_rel_between_nodes_params(rel)

    assert_that("first_name" in params, equal_to(False))
    assert_that(params["__from_node_id"], equal_to("Person_first_name:a"))
    assert_that(params["__to_node_id"], equal_to("Person_first_name:b"))
