import math
import numbers
import re
from datetime import datetime
from functools import cache, wraps
from typing import Iterable

from cymple.builder import NodeAfterMergeAvailable, NodeAvailable, QueryBuilder
from nodestream.databases.query_executor import (
    OperationOnNodeIdentity, OperationOnRelationshipIdentity)
from nodestream.model import (Node, Relationship, RelationshipIdentityShape,
                              RelationshipWithNodes, TimeToLiveConfiguration)
from nodestream.schema.state import GraphObjectType
from pandas import Timedelta, Timestamp

from .query import Query, QueryBatch

GENERIC_NODE_REF_NAME = "node"
GENERIC_FROM_NODE_REF_NAME = "from_node"
GENERIC_TO_NODE_REF_NAME = "to_node"
RELATIONSHIP_REF_NAME = "rel"
PARAMETER_CORRECTION_REGEX = re.compile(r"\"(params.__\w+)\"")


def correct_parameters(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        query = f(*args, **kwargs)
        return PARAMETER_CORRECTION_REGEX.sub(r"\1", query)

    return wrapper


def generate_prefixed_param_name(property_name: str, prefix: str) -> str:
    return f"__{prefix}_{property_name}" if prefix else property_name


def generate_id_param_name(node_ref_name: str) -> str:
    return generate_prefixed_param_name("id", node_ref_name)


def generate_properties_set_with_prefix(properties: Iterable[str], prefix: str):
    return {
        prop: f"param.{generate_prefixed_param_name(prop, prefix)}"
        for prop in properties
    }


@cache
def _match_node(
    node_operation: OperationOnNodeIdentity,
    node_id_param_name: str,
    name=GENERIC_NODE_REF_NAME,
) -> NodeAvailable:
    identity = node_operation.node_identity
    return (
        QueryBuilder()
        .match()
        .node(
            labels=identity.type,
            ref_name=name,
            properties={"`~id`": f"param.{node_id_param_name}"},
            escape=False,
        )
    )


@cache
def _merge_node(
    labels: str, node_id_param_name: str, name=GENERIC_NODE_REF_NAME
) -> NodeAfterMergeAvailable:
    return (
        QueryBuilder()
        .merge()
        .node(
            labels=labels,
            ref_name=name,
            properties={"`~id`": f"param.{node_id_param_name}"},
            escape=False,
        )
    )


@cache
def _make_relationship(rel_identity: RelationshipIdentityShape):
    keys = generate_properties_set_with_prefix(rel_identity.keys, None)
    match_rel_query = (
        QueryBuilder()
        .merge()
        .node(ref_name=GENERIC_FROM_NODE_REF_NAME)
        .related_to(
            ref_name=RELATIONSHIP_REF_NAME,
            properties=keys,
            label=rel_identity.type,
            escape=False,
        )
        .node(ref_name=GENERIC_TO_NODE_REF_NAME)
    )

    return match_rel_query


def _convert_unsupported_values(props: dict):
    # Convert unsupported values.
    # Due to the structure of our queries, all data must be passed as valid JSON.
    for k, v in props.items():
        # Timestamp and datetime are not valid json. Convert to POSIX timestamp instead.
        if isinstance(v, (Timestamp, datetime)):
            props[k] = v.timestamp()
        elif isinstance(v, numbers.Number) and not math.isfinite(v):
            raise ValueError("NaN and Infinity float values are not supported")

    return props


class NeptuneIngestQueryBuilder:
    @cache
    @correct_parameters
    def generate_update_node_operation_query_statement(
        self,
        operation: OperationOnNodeIdentity,
        ref: str,
    ) -> str:
        """Generate a query to update a node in the database given a node type and a match strategy."""
        labels = ":".join(
            [
                operation.node_identity.type,
                *operation.node_identity.additional_types,
            ]
        )
        node_id_param_name = generate_id_param_name(GENERIC_NODE_REF_NAME)
        merge_node = _merge_node(labels, node_id_param_name, GENERIC_NODE_REF_NAME)

        """
        At this time, Neptune doesn't support nested maps very well.
        We get an error trying to access inner maps in the parameters
        of our openCypher query. 
        
        As such, node's properties and __node_id has to be kept at 
        the same level.

        As a work around, we use removeKeyFromMap() to remove __node_id before 
        setting node's properties. removeKeyFromMap() Neptune specific.
        """
        on_create = f"""ON CREATE SET {GENERIC_NODE_REF_NAME} = removeKeyFromMap(param, "{node_id_param_name}")"""
        on_match = f"""ON MATCH SET {GENERIC_NODE_REF_NAME} += removeKeyFromMap(param, "{node_id_param_name}")"""
        query = f"{merge_node} {on_create} {on_match}"
        return query

    def generate_update_node_operation_params(self, node: Node) -> dict:
        """Generate the parameters for a query to update a node in the database."""

        node_props = {**self.generate_node_key_params(node), **node.properties}
        node_props = _convert_unsupported_values(node_props)

        return node_props

    def generate_node_key_params(self, node: Node, name=GENERIC_NODE_REF_NAME) -> dict:
        """Generate the parameters for a query to update a node in the database."""

        # Todo: What if no keys were given? Maybe let neptune decides.
        # On uniqueness and keys in Neptune, see Schema Constraints in https://docs.aws.amazon.com/neptune/latest/userguide/migration-compatibility.html
        composite_key = "_".join([str(node.key_values[k]) for k in node.key_values])
        composite_key = f"{node.type}_{composite_key}"
        return {generate_prefixed_param_name("id", name): composite_key}

    @cache
    @correct_parameters
    def generate_update_relationship_operation_query_statement(
        self, operation: OperationOnRelationshipIdentity
    ) -> str:
        """Generate a query to update a relationship in the database given a relationship operation."""
        from_node_id_param_name = generate_id_param_name(GENERIC_FROM_NODE_REF_NAME)
        match_from_node_segment = _match_node(
            operation.from_node, from_node_id_param_name, GENERIC_FROM_NODE_REF_NAME
        )

        to_node_id_param_name = generate_id_param_name(GENERIC_TO_NODE_REF_NAME)
        match_to_node_segment = _match_node(
            operation.to_node, to_node_id_param_name, GENERIC_TO_NODE_REF_NAME
        )

        match_to_node_segment = str(match_to_node_segment).replace("MATCH", ",")

        merge_rel_segment = _make_relationship(operation.relationship_identity)

        # At this time, Neptune doesn't support nested maps very well.
        # See comments in generate_update_node_operation_query_statement()
        on_create = f"""ON CREATE SET {RELATIONSHIP_REF_NAME} = removeKeyFromMap(removeKeyFromMap(param, "{from_node_id_param_name}"), "{to_node_id_param_name}")"""

        on_match = f"""ON MATCH SET {RELATIONSHIP_REF_NAME} += removeKeyFromMap(removeKeyFromMap(param, "{from_node_id_param_name}"), "{to_node_id_param_name}")"""

        return f"{match_from_node_segment} {match_to_node_segment} {merge_rel_segment} {on_create} {on_match}"

    def generate_update_rel_params(self, rel: Relationship) -> dict:
        """Generate the parameters for a query to update a relationship in the database."""

        return _convert_unsupported_values({**rel.key_values, **rel.properties})

    def generate_update_rel_between_nodes_params(
        self, rel: RelationshipWithNodes
    ) -> dict:
        """Generate the parameters for a query to update a relationship in the database."""

        params = self.generate_update_rel_params(rel.relationship)
        params.update(
            self.generate_node_key_params(rel.from_node, GENERIC_FROM_NODE_REF_NAME)
        )
        params.update(
            self.generate_node_key_params(rel.to_node, GENERIC_TO_NODE_REF_NAME)
        )
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
        query_stmt = self.generate_update_relationship_operation_query_statement(
            operation
        )
        params = [
            self.generate_update_rel_between_nodes_params(rel) for rel in relationships
        ]

        return QueryBatch(query_stmt, params)

    def generate_ttl_query_from_configuration(
        self, config: TimeToLiveConfiguration
    ) -> Query:
        earliest_allowed_time = Timestamp.utcnow() - Timedelta(
            hours=config.expiry_in_hours
        )
        params = _convert_unsupported_values(
            {"earliest_allowed_time": earliest_allowed_time}
        )
        if config.custom_query is not None:
            return Query(config.custom_query, params)

        query_builder = QueryBuilder()
        ref_name = "x"

        if config.graph_object_type == GraphObjectType.NODE:
            query_builder = query_builder.match().node(
                labels=config.object_type, ref_name=ref_name
            )
        else:
            query_builder = (
                query_builder.match()
                .node()
                .related_to(label=config.object_type, ref_name=ref_name)
                .node()
            )

        query_builder = query_builder.where_literal(
            f"{ref_name}.last_ingested_at <= $earliest_allowed_time"
        )

        if config.graph_object_type == GraphObjectType.NODE:
            query_builder = query_builder.detach_delete(ref_name)
        else:
            query_builder = query_builder.delete(ref_name)

        return Query(str(query_builder), params)
