from typing import List

from nodestream.schema.migrations import (
    Migration,
    MigrationGraph,
    Migrator,
    OperationTypeRoutingMixin,
)
from nodestream.schema.migrations.operations import (
    AddAdditionalNodePropertyIndex,
    AddAdditionalRelationshipPropertyIndex,
    AddNodeProperty,
    AddRelationshipProperty,
    CreateNodeType,
    CreateRelationshipType,
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

from .neptune_connection import NeptuneConnection

LIST_MIGRATIONS_QUERY = "MATCH (m:__NodestreamMigration__) RETURN m.name as name"
MARK_MIGRATION_AS_EXECUTED_QUERY = "MERGE (:__NodestreamMigration__ {name: $name})"

DROP_ALL_NODES_OF_TYPE_FORMAT = "MATCH (n:`{type}`) DETACH DELETE n"
DROP_ALL_RELATIONSHIPS_OF_TYPE_FORMAT = "MATCH ()-[r:`{type}`]->() DELETE r"

SET_NODE_PROPERTY_FORMAT = "MATCH (n:`{node_type}`) SET n.`{property_name}` = coalesce(n.`{property_name}`, $value)"
SET_RELATIONSHIP_PROPERTY_FORMAT = "MATCH ()-[r:`{relationship_type}`]->() SET r.`{property_name}` = coalesce(r.`{property_name}`, $value)"

RENAME_NODE_PROPERTY_FORMAT = "MATCH (n:`{node_type}`) SET n.`{new_property_name}` = n.`{old_property_name}` REMOVE n.`{old_property_name}`"
RENAME_RELATIONSHIP_PROPERTY_FORMAT = "MATCH ()-[r:`{relationship_type}`]->() SET r.`{new_property_name}` = r.`{old_property_name}` REMOVE r.`{old_property_name}`"

RENAME_NODE_TYPE = "MATCH (n:`{old_type}`) SET n:`{new_type}` REMOVE n:`{old_type}`"
RENAME_REL_TYPE = "MATCH (n)-[r:`{old_type}`]->(m) CREATE (n)-[r2:`{new_type}`]->(m) SET r2 += r WITH r DELETE r"

DROP_REL_PROPERTY_FORMAT = (
    "MATCH ()-[r:`{relationship_type}`]->() REMOVE r.`{property_name}`"
)
DROP_NODE_PROPERTY_FORMAT = "MATCH (n:`{node_type}`) REMOVE n.`{property_name}`"


class NeptuneMigrator(OperationTypeRoutingMixin, Migrator):
    def __init__(self, database_connection: NeptuneConnection) -> None:
        self.database_connection = database_connection

    async def mark_migration_as_executed(self, migration: Migration) -> None:
        await self.database_connection.execute(
            MARK_MIGRATION_AS_EXECUTED_QUERY, {"name": migration.name}
        )

    async def get_completed_migrations(self, graph: MigrationGraph) -> List[Migration]:
        return [
            graph.get_migration(record["name"])
            for record in await self.database_connection.execute(LIST_MIGRATIONS_QUERY)
        ]

    async def execute_create_node_type(self, _: CreateNodeType) -> None:
        # Neptune does not need us to do anything here.
        pass

    async def execute_create_relationship_type(self, _: CreateRelationshipType) -> None:
        # Neptune does not need us to do anything here.
        pass

    async def execute_drop_node_type(self, operation: DropNodeType) -> None:
        query = DROP_ALL_NODES_OF_TYPE_FORMAT.format(type=operation.name)
        await self.database_connection.execute(query, {})

    async def execute_drop_relationship_type(
        self, operation: DropRelationshipType
    ) -> None:
        query = DROP_ALL_RELATIONSHIPS_OF_TYPE_FORMAT.format(type=operation.name)
        await self.database_connection.execute(query, {})

    async def execute_rename_node_property(self, operation: RenameNodeProperty) -> None:
        query = RENAME_NODE_PROPERTY_FORMAT.format(
            node_type=operation.node_type,
            old_property_name=operation.old_property_name,
            new_property_name=operation.new_property_name,
        )
        await self.database_connection.execute(query, {})

    async def execute_rename_relationship_property(
        self, operation: RenameRelationshipProperty
    ) -> None:
        query = RENAME_RELATIONSHIP_PROPERTY_FORMAT.format(
            relationship_type=operation.relationship_type,
            old_property_name=operation.old_property_name,
            new_property_name=operation.new_property_name,
        )
        await self.database_connection.execute(query, {})

    async def execute_rename_node_type(self, operation: RenameNodeType) -> None:
        query = RENAME_NODE_TYPE.format(
            old_type=operation.old_type, new_type=operation.new_type
        )
        await self.database_connection.execute(query, {})

    async def execute_rename_relationship_type(
        self, operation: RenameRelationshipType
    ) -> None:
        # Rename all rels of the old type to the new type.
        query = RENAME_REL_TYPE.format(
            old_type=operation.old_type, new_type=operation.new_type
        )
        await self.database_connection.execute(query, {})

    async def execute_add_additional_node_property_index(
        self, _: AddAdditionalNodePropertyIndex
    ) -> None:
        # Neptune does not need us to do anything here.
        pass

    async def execute_drop_additional_node_property_index(
        self, _: DropAdditionalNodePropertyIndex
    ) -> None:
        # Neptune does not need us to do anything here.
        pass

    async def execute_add_additional_relationship_property_index(
        self, _: AddAdditionalRelationshipPropertyIndex
    ) -> None:
        # Neptune does not need us to do anything here.
        pass

    async def execute_drop_additional_relationship_property_index(
        self, _: DropAdditionalRelationshipPropertyIndex
    ) -> None:
        # Neptune does not need us to do anything here.
        pass

    async def execute_add_node_property(self, operation: AddNodeProperty) -> None:
        query = SET_NODE_PROPERTY_FORMAT.format(
            node_type=operation.node_type, property_name=operation.property_name
        )
        await self.database_connection.execute(query, {"value": operation.default})

    async def execute_add_relationship_property(
        self, operation: AddRelationshipProperty
    ) -> None:
        query = SET_RELATIONSHIP_PROPERTY_FORMAT.format(
            relationship_type=operation.relationship_type,
            property_name=operation.property_name,
        )
        await self.database_connection.execute(query, {"value": operation.default})

    async def execute_drop_node_property(self, operation: DropNodeProperty) -> None:
        query = DROP_NODE_PROPERTY_FORMAT.format(
            node_type=operation.node_type, property_name=operation.property_name
        )
        await self.database_connection.execute(query, {})

    async def execute_drop_relationship_property(
        self, operation: DropRelationshipProperty
    ) -> None:
        query = DROP_REL_PROPERTY_FORMAT.format(
            relationship_type=operation.relationship_type,
            property_name=operation.property_name,
        )
        await self.database_connection.execute(query, {})

    async def execute_node_key_extended(self, operation: NodeKeyExtended) -> None:
        as_add_property = AddNodeProperty(
            operation.node_type, operation.added_key_property, operation.default
        )
        await self.execute_add_node_property(as_add_property)

    async def execute_relationship_key_extended(
        self, operation: RelationshipKeyExtended
    ) -> None:
        as_add_property = AddRelationshipProperty(
            operation.relationship_type,
            operation.added_key_property,
            operation.default,
        )
        await self.execute_add_relationship_property(as_add_property)

    async def execute_node_key_part_renamed(
        self, operation: NodeKeyPartRenamed
    ) -> None:
        as_property_renamed = RenameNodeProperty(
            operation.node_type,
            operation.old_key_part_name,
            operation.new_key_part_name,
        )
        await self.execute_rename_node_property(as_property_renamed)

    async def execute_relationship_key_part_renamed(
        self, operation: RelationshipKeyPartRenamed
    ) -> None:
        as_property_renamed = RenameRelationshipProperty(
            operation.relationship_type,
            operation.old_key_part_name,
            operation.new_key_part_name,
        )
        await self.execute_rename_relationship_property(as_property_renamed)
