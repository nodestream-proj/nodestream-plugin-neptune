import json
from unittest.mock import patch

import pytest
from hamcrest import assert_that, equal_to, equal_to_ignoring_whitespace
from nodestream.databases.query_executor import (
    OperationOnNodeIdentity, OperationOnRelationshipIdentity)
from nodestream.model import (Node, NodeCreationRule, PropertySet,
                              Relationship, RelationshipCreationRule,
                              RelationshipWithNodes, TimeToLiveConfiguration)
from nodestream.schema import GraphObjectType
from pandas import Timestamp

from nodestream_plugin_neptune.ingest_query_builder import \
    NeptuneIngestQueryBuilder
from nodestream_plugin_neptune.query import Query, QueryBatch


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
            **convert_timestamps(SIMPLE_NODE.properties),
        }
    ],
)

SIMPLE_NODE_EXPECTED_QUERY_ON_MATCH = QueryBatch(
    'MATCH (node: TestType {`~id` : param.__node_id}) SET node += removeKeyFromMap(param, "__node_id")',
    [
        {
            "__node_id": "TestType_id:foo",
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
    query_builder: NeptuneIngestQueryBuilder = NeptuneIngestQueryBuilder(include_label_in_id=False)

    expected_query: QueryBatch = QueryBatch(
        COMPLEX_NODE_TWO_EXPECTED_QUERY.query_statement,
        [
            {
                "__node_id": "id_part1:foo_id_part2:bar",
                **convert_timestamps(COMPLEX_NODE_TWO.properties),
            }
        ],
    )

    test_node_update_generates_expected_queries(
        query_builder=query_builder,
        node=COMPLEX_NODE_TWO,
        expected_query=expected_query,
        node_creation_rule=NodeCreationRule.EAGER
    )


def test_relationship_update_with_label_excluded_from_id():
    query_builder: NeptuneIngestQueryBuilder = NeptuneIngestQueryBuilder(include_label_in_id=False)

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
    node = Node(
        "ComplexType",
        {"id_part1": "foo", "id_part2": "bar"}
    )

    query_builder: NeptuneIngestQueryBuilder = NeptuneIngestQueryBuilder()
    key_params = query_builder.generate_node_key_params(node)
    expected = {'__node_id': 'ComplexType_id_part1:foo_id_part2:bar'}
    assert_that(
        key_params,
        equal_to(expected),
    )


def test_generate_params_reverse_order():
    node = Node(
        "ComplexType",
        {"id_part2": "bar", "id_part1": "foo"}
    )

    query_builder: NeptuneIngestQueryBuilder = NeptuneIngestQueryBuilder()
    key_params = query_builder.generate_node_key_params(node)
    expected = {'__node_id': 'ComplexType_id_part1:foo_id_part2:bar'}
    assert_that(
        key_params,
        equal_to(expected),
    )


def test_generate_params_null_value():
    node = Node(
        "ComplexType",
        {'id_part1': None}
    )

    query_builder: NeptuneIngestQueryBuilder = NeptuneIngestQueryBuilder()
    key_params = query_builder.generate_node_key_params(node)
    expected = {'__node_id': 'ComplexType_id_part1:'}
    assert_that(
        key_params,
        equal_to(expected),
    )
