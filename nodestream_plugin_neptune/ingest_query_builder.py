import re
import math
import numbers
from pandas import Timestamp
from datetime import datetime, timedelta
from functools import cache, wraps
from typing import Iterable

from cymple.builder import NodeAfterMergeAvailable, NodeAvailable, QueryBuilder

from nodestream.model import (
    Node,
    NodeCreationRule,
    Relationship,
    RelationshipCreationRule,
    RelationshipIdentityShape,
    RelationshipWithNodes,
    TimeToLiveConfiguration,
)
from nodestream.schema.schema import GraphObjectType
from nodestream.databases.query_executor import OperationOnNodeIdentity, OperationOnRelationshipIdentity
from .query import Query, QueryBatch

PROPERTIES_PARAM_NAME = "properties"
ADDITIONAL_LABELS_PARAM_NAME = "additional_labels"
GENERIC_NODE_REF_NAME = "node"
FROM_NODE_REF_NAME = "from_node"
FROM_NODE_PROPS_REF = {"from_node": "from_ref_props"}
FROM_NODE_PROPS_REF_STR = str(FROM_NODE_PROPS_REF)
TO_NODE_REF_NAME = "to_node"
TO_NODE_PROPS_REF = {"to_node": "to_ref_props"}
TO_NODE_PROPS_REF_STR = str(TO_NODE_PROPS_REF)
RELATIONSHIP_REF_NAME = "rel"
PARAMETER_CORRECTION_REGEX = re.compile(r"\"(params.__\w+)\"")
DELETE_NODE_QUERY = "MATCH (n) WHERE id(n) = id DETACH DELETE n"
DELETE_REL_QUERY = "MATCH ()-[r]->() WHERE id(r) = id DELETE r"


def correct_parameters(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        query = f(*args, **kwargs)
        return PARAMETER_CORRECTION_REGEX.sub(r"\1", query)

    return wrapper


def generate_prefixed_param_name(property_name: str, prefix: str) -> str:
    return f"__{prefix}_{property_name}"

def generate_id_param_name(node_ref_name: str) -> str:
    return generate_prefixed_param_name("id", node_ref_name)


def generate_properties_set_with_prefix(properties: Iterable[str], prefix: str):
    return {
        prop: f"param.{generate_prefixed_param_name(prop, prefix)}"
        for prop in properties
    }

@cache
def _match_node(
    node_operation: OperationOnNodeIdentity, name=GENERIC_NODE_REF_NAME
) -> NodeAvailable:
    identity = node_operation.node_identity
    node_id_param_name = generate_id_param_name(name)
    return (
        QueryBuilder()
        .match()
        .node(labels=identity.type, ref_name=name, properties={"`~id`": f"param.{node_id_param_name}"}, escape=False)
    )

@cache
def _generate_node_id_property(node_id_param_name: str) -> dict:
    return {"`~id`": f"param.{node_id_param_name}"}

@cache
def _merge_node(
    node_operation: OperationOnNodeIdentity, name=GENERIC_NODE_REF_NAME
) -> NodeAfterMergeAvailable:
    node_id_param_name = generate_id_param_name(GENERIC_NODE_REF_NAME)
    properties = generate_properties_set_with_prefix(
        node_operation.node_identity.keys, name
    )
    return (
        QueryBuilder()
        .merge()
        .node(
            labels=node_operation.node_identity.type,
            ref_name=name,
            properties=_generate_node_id_property(node_id_param_name),
            escape=False
        )
    )


@cache
def _make_relationship(
    rel_identity: RelationshipIdentityShape
):
    keys = generate_properties_set_with_prefix(rel_identity.keys, RELATIONSHIP_REF_NAME)
    match_rel_query = (
        QueryBuilder()
        .merge()
        .node(ref_name=FROM_NODE_REF_NAME)
        .related_to(
            ref_name=RELATIONSHIP_REF_NAME,
            properties=keys,
            label=rel_identity.type,
        )
        .node(ref_name=TO_NODE_REF_NAME)
    )

    return match_rel_query


def _to_string_values(props: dict):
    # Convert unsupported values to string
    for k, v in props.items():
        if isinstance(v, Timestamp):
            props[k] = str(v)
        elif not v:
            props[k] = 'None'
        elif isinstance(v, numbers.Number) and math.isnan(v):
            props[k] = "NaN"

    return props


class NeptuneDBIngestQueryBuilder:
    @cache
    @correct_parameters
    def generate_update_node_operation_query_statement(
        self,
        operation: OperationOnNodeIdentity,
        ref: str,
    ) -> str:
        """Generate a query to update a node in the database given a node type and a match strategy."""
        labels = [
            operation.node_identity.type,
            *operation.node_identity.additional_types,
        
        ]
        node_id_param_name = generate_id_param_name(GENERIC_NODE_REF_NAME)

        merge_node = (
            QueryBuilder()
            .merge()
            .node(
                labels=labels,
                ref_name=ref,
                properties={"`~id`": f"param.{node_id_param_name}"},
                escape=False
            )
        )
        on_create = f"ON CREATE SET {GENERIC_NODE_REF_NAME} = param"
        on_match = f"ON MATCH SET {GENERIC_NODE_REF_NAME} += param"
        query = f"{merge_node} {on_create} {on_match}"
        return query

    def generate_update_node_operation_params(self, node: Node) -> dict:
        """Generate the parameters for a query to update a node in the database."""

        node_props = {**self.generate_node_key_params(node), **node.properties}
        node_props = _to_string_values(node_props)

        return node_props

    def generate_node_key_params(self, node: Node, name=GENERIC_NODE_REF_NAME) -> dict:
        """Generate the parameters for a query to update a node in the database."""
        # Todo: What if no keys were given? Maybe let neptune decides.
        composite_key = "_".join([str(node.key_values[k]) for k in node.key_values])
        composite_key = f"{node.type}_{composite_key}"
        return {generate_prefixed_param_name("id", name): composite_key}

    @cache
    @correct_parameters
    def generate_update_relationship_operation_query_statement(
        self, operation: OperationOnRelationshipIdentity
    ) -> str:
        """Generate a query to update a relationship in the database given a relationship operation."""

        match_from_node_segment = _match_node(
            operation.from_node,
            FROM_NODE_REF_NAME
        )

        match_to_node_segment = _match_node(
            operation.to_node,
            TO_NODE_REF_NAME
        )
        match_to_node_segment = str(match_to_node_segment).replace("MATCH", ",")

        merge_rel_segment = _make_relationship(operation.relationship_identity)

        on_create = f"ON CREATE SET {RELATIONSHIP_REF_NAME} = param"

        on_match = f"ON MATCH SET {RELATIONSHIP_REF_NAME} += param"

        return f"{match_from_node_segment} {match_to_node_segment} {merge_rel_segment} {on_create} {on_match}"

    def generate_update_rel_params(self, rel: Relationship) -> dict:
        """Generate the parameters for a query to update a relationship in the database."""

        return _to_string_values({**rel.key_values, **rel.properties})

    def generate_update_rel_between_nodes_params(
        self, rel: RelationshipWithNodes
    ) -> dict:
        """Generate the parameters for a query to update a relationship in the database."""

        params = self.generate_update_rel_params(rel.relationship)
        params.update(self.generate_node_key_params(rel.from_node, FROM_NODE_REF_NAME))
        params.update(self.generate_node_key_params(rel.to_node, TO_NODE_REF_NAME))
        return params

    def generate_batch_update_node_operation_batch(
        self,
        operation: OperationOnNodeIdentity,
        nodes: Iterable[Node],
    ) -> QueryBatch:
        """Generate a batch of queries to update nodes in the database in the same way of the same type."""
        query = self.generate_update_node_operation_query_statement(
            operation=operation, ref=GENERIC_NODE_REF_NAME
        )

        params = [self.generate_update_node_operation_params(node) for node in nodes]
        return QueryBatch(query, params)

    def generate_batch_update_relationship_query_batch(
        self,
        operation: OperationOnRelationshipIdentity,
        relationships: Iterable[RelationshipWithNodes],
    ) -> QueryBatch:
        """Generate a batch of queries to update relationships in the database in the same way of the same type."""
        query_stmt = self.generate_update_relationship_operation_query_statement(operation)
        params = [
            self.generate_update_rel_between_nodes_params(rel) for rel in relationships
        ]

        return QueryBatch(
            query_stmt,
            params
        )

    def generate_ttl_match_query(self, config: TimeToLiveConfiguration) -> Query:
        raise NotImplementedError

    def generate_ttl_query_from_configuration(
        self, config: TimeToLiveConfiguration
    ) -> Query:
        raise NotImplementedError
