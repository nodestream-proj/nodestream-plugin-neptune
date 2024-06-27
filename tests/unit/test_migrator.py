import pytest
from hamcrest import assert_that

from nodestream.schema.migrations.operations import (
    AddAdditionalNodePropertyIndex,
    AddAdditionalRelationshipPropertyIndex,
    AddNodeProperty,
    AddRelationshipProperty,
    CreateNodeType,
    DropAdditionalNodePropertyIndex,
    DropAdditionalRelationshipPropertyIndex,
    DropNodeProperty,
    DropNodeType,
    DropRelationshipProperty,
    DropRelationshipType,
    NodeKeyExtended,
    NodeKeyPartRenamed,
    RelationshipKeyExtended,
    RelationshipKeyPartRenamed,
    RenameNodeProperty,
    RenameNodeType,
    RenameRelationshipProperty,
    RenameRelationshipType,
)

from nodestream_plugin_neptune.neptune_migrator import (NeptuneMigrator)
from nodestream_plugin_neptune.neptune_connection import (NeptuneDBConnection)

from .matchers import ran_query


@pytest.fixture
def database_connection(mocker):
    return mocker.Mock(NeptuneDBConnection)


@pytest.fixture
def migrator(database_connection):
    return NeptuneMigrator(database_connection)


@pytest.mark.asyncio
async def test_execute_relationship_key_part_renamed(migrator):
    operation = RelationshipKeyPartRenamed(
        old_key_part_name="old_key",
        new_key_part_name="new_key",
        relationship_type="RELATIONSHIP_TYPE",
    )
    await migrator.execute_operation(operation)
    expected_query = "MATCH ()-[r:`RELATIONSHIP_TYPE`]->() SET r.`new_key` = r.`old_key` REMOVE r.`old_key`"
    migrator.database_connection.execute.assert_called_with(expected_query, {})


@pytest.mark.asyncio
async def test_execute_relationship_property_renamed(migrator):
    operation = RenameRelationshipProperty(
        old_property_name="old_prop",
        new_property_name="new_prop",
        relationship_type="RELATIONSHIP_TYPE",
    )
    await migrator.execute_operation(operation)
    expected_query = "MATCH ()-[r:`RELATIONSHIP_TYPE`]->() SET r.`new_prop` = r.`old_prop` REMOVE r.`old_prop`"
    migrator.database_connection.execute.assert_called_with(expected_query, {})


@pytest.mark.asyncio
async def test_execute_relationship_key_extended(migrator):
    operation = RelationshipKeyExtended(
        added_key_property="key", relationship_type="RELATIONSHIP_TYPE", default="foo"
    )
    await migrator.execute_operation(operation)
    expected_query = "MATCH ()-[r:`RELATIONSHIP_TYPE`]->() SET r.`key` = coalesce(r.`key`, $value)"
    migrator.database_connection.execute.assert_called_with(
        expected_query, {"value": "foo"}
    )


@pytest.mark.asyncio
async def test_execute_relationship_property_added(migrator):
    operation = AddRelationshipProperty(
        property_name="prop", relationship_type="RELATIONSHIP_TYPE", default="foo"
    )
    await migrator.execute_operation(operation)
    expected_query = (
        "MATCH ()-[r:`RELATIONSHIP_TYPE`]->() SET r.`prop` = coalesce(r.`prop`, $value)"
    )
    migrator.database_connection.execute.assert_called_with(
        expected_query, {"value": "foo"}
    )


@pytest.mark.asyncio
async def test_execute_relationship_property_dropped(migrator):
    operation = DropRelationshipProperty(
        property_name="prop", relationship_type="RELATIONSHIP_TYPE"
    )
    await migrator.execute_operation(operation)
    expected_query = "MATCH ()-[r:`RELATIONSHIP_TYPE`]->() REMOVE r.`prop`"
    migrator.database_connection.execute.assert_called_with(expected_query, {})


@pytest.mark.asyncio
async def test_execute_relationship_type_renamed(migrator):
    operation = RenameRelationshipType(old_type="OLD_TYPE", new_type="NEW_TYPE")
    await migrator.execute_operation(operation)
    expected_query = "MATCH (n)-[r:`OLD_TYPE`]->(m) CREATE (n)-[r2:`NEW_TYPE`]->(m) SET r2 += r WITH r DELETE r"
    migrator.database_connection.execute.assert_called_with(expected_query, {})


@pytest.mark.asyncio
async def test_execute_relationship_type_created(migrator):
    # Neo4j Does not need us to do anything here.
    pass


@pytest.mark.asyncio
async def test_execute_relationship_type_dropped(migrator):
    operation = DropRelationshipType(name="RELATIONSHIP_TYPE")
    await migrator.execute_operation(operation)
    expected_query = "MATCH ()-[r:`RELATIONSHIP_TYPE`]->() DELETE r"
    migrator.database_connection.execute.assert_called_with(expected_query, {})


@pytest.mark.asyncio
async def test_execute_node_type_dropped(migrator):
    operation = DropNodeType(name="NodeType")
    await migrator.execute_operation(operation)
    expected_query = "MATCH (n:`NodeType`) DETACH DELETE n"
    migrator.database_connection.execute.assert_called_with(expected_query, {})


@pytest.mark.asyncio
async def test_add_additional_node_property_index(migrator):
    pass


@pytest.mark.asyncio
async def test_drop_additional_node_property_index(migrator):
    pass


@pytest.mark.asyncio
async def test_add_additional_relationship_property_index(migrator):
    pass


@pytest.mark.asyncio
async def test_drop_additional_relationship_property_index(migrator):
    pass


@pytest.mark.asyncio
async def test_rename_node_property(migrator):
    operation = RenameNodeProperty(
        old_property_name="old_prop", new_property_name="new_prop", node_type="NodeType"
    )
    await migrator.execute_operation(operation)
    expected_query = (
        "MATCH (n:`NodeType`) SET n.`new_prop` = n.`old_prop` REMOVE n.`old_prop`"
    )
    migrator.database_connection.execute.assert_called_with(expected_query, {})


@pytest.mark.asyncio
async def test_rename_node_type(migrator):
    operation = RenameNodeType(old_type="OLD_TYPE", new_type="NEW_TYPE")
    await migrator.execute_operation(operation)
    expected_query = "MATCH (n:`OLD_TYPE`) SET n:`NEW_TYPE` REMOVE n:`OLD_TYPE`"
    migrator.database_connection.execute.assert_called_with(expected_query, {})


@pytest.mark.asyncio
async def test_create_node_type_enterprise(migrator):
    pass


@pytest.mark.asyncio
async def test_add_node_property(migrator):
    operation = AddNodeProperty(
        property_name="prop", node_type="NodeType", default="foo"
    )
    await migrator.execute_operation(operation)
    expected_query = "MATCH (n:`NodeType`) SET n.`prop` = coalesce(n.`prop`, $value)"
    migrator.database_connection.execute.assert_called_with(
        expected_query, {"value": "foo"}
    )


@pytest.mark.asyncio
async def test_drop_node_property(migrator):
    operation = DropNodeProperty(property_name="prop", node_type="NodeType")
    await migrator.execute_operation(operation)
    expected_query = "MATCH (n:`NodeType`) REMOVE n.`prop`"
    migrator.database_connection.execute.assert_called_with(expected_query, {})


@pytest.mark.asyncio
async def test_node_key_extended_with_default(migrator):
    operation = NodeKeyExtended(
        added_key_property="key", node_type="NodeType", default="foo"
    )
    await migrator.execute_operation(operation)
    expected_query = "MATCH (n:`NodeType`) SET n.`key` = coalesce(n.`key`, $value)"
    migrator.database_connection.execute.assert_called_with(
        expected_query, {"value": "foo"}
    )


@pytest.mark.asyncio
async def test_node_key_renamed(migrator, mocker):
    migrator.get_properties_by_constraint_name = mocker.AsyncMock(return_value={"foo"})
    operation = NodeKeyPartRenamed(
        new_key_part_name="key", node_type="NodeType", old_key_part_name="foo"
    )
    await migrator.execute_operation(operation)
    migrator.database_connection.execute.assert_called_with(
        "MATCH (n:`NodeType`) SET n.`key` = n.`foo` REMOVE n.`foo`", {}
    )
