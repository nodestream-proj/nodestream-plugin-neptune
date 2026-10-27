import math
import numbers
import re
from datetime import datetime
from functools import cache, lru_cache, wraps
from logging import getLogger
from typing import Iterable

from cymple.builder import NodeAfterMergeAvailable, NodeAvailable, QueryBuilder
from nodestream.databases.query_executor import (
    OperationOnNodeIdentity,
    OperationOnRelationshipIdentity,
)
from nodestream.model import (
    Node,
    NodeCreationRule,
    Relationship,
    RelationshipCreationRule,
    RelationshipIdentityShape,
    RelationshipWithNodes,
    TimeToLiveConfiguration,
)
from nodestream.schema.state import GraphObjectType
from pandas import Timedelta, Timestamp

from .query import Query, QueryBatch

GENERIC_NODE_REF_NAME = "node"
GENERIC_FROM_NODE_REF_NAME = "from_node"
GENERIC_TO_NODE_REF_NAME = "to_node"
RELATIONSHIP_REF_NAME = "rel"
PARAMETER_CORRECTION_REGEX = re.compile(r"\"(params.__\w+)\"")

logger = getLogger(__name__)


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


# Property names that must never be written as properties. Neptune reserves
# ``~id`` for the element id: Neptune Database rejects it in a ``SET`` map
# ("'~id' as a property in SET clause is not supported") and the whole batch is
# lost, while Neptune Analytics accepts it and stores a literal property named
# ``~id``. Checked on both engines: every other ``~``-prefixed name (``~label``,
# ``~type``, ``~start``, ``~end``, arbitrary ``~x``) is accepted as an ordinary
# property, so only ``~id`` is stripped.
_RESERVED_PROPERTY_NAMES = ("~id",)


@lru_cache(maxsize=None)
def _warn_reserved_property_dropped(name: str, context: str = None) -> None:
    """Log, once per reserved name and context, that a property was dropped."""
    where = f" on {context}" if context else ""
    logger.warning(
        "Dropping property %r%s: Neptune reserves it for the element id and "
        "cannot write it as a property (Neptune Database rejects the write; "
        "Neptune Analytics would store a literal property with that name).",
        name,
        where,
    )


def _drop_reserved_properties(properties, context: str = None):
    """Return ``properties`` without names Neptune reserves (currently ``~id``).

    Only PROPERTIES are sanitized; callers must not pass key values, which are
    identity and are never altered. The input is not mutated. Copying from
    Neptune already keeps ``~id`` out of properties; this is the backstop for
    every other source and for user pipelines.
    """
    if not any(name in properties for name in _RESERVED_PROPERTY_NAMES):
        return properties

    for name in _RESERVED_PROPERTY_NAMES:
        if name in properties:
            _warn_reserved_property_dropped(name, context)
    return {k: v for k, v in properties.items() if k not in _RESERVED_PROPERTY_NAMES}


# Values the plugin can send as parameters unchanged (plus ``None``). Datetimes
# are converted to POSIX seconds; everything else is rejected, see below.
_WRITABLE_SCALARS = (str, bool, int, float)

_SUPPORTED_TYPES_HINT = (
    "Supported value types are str, bool, int, float, None, and datetime "
    "(written as POSIX seconds). Convert the value in the source or with a "
    "pipeline step before it reaches the writer."
)


def _type_name(value) -> str:
    """Fully qualified type name, e.g. ``neo4j.time.DateTime`` (bare for builtins)."""
    cls = type(value)
    if cls.__module__ == "builtins":
        return cls.__qualname__
    return f"{cls.__module__}.{cls.__qualname__}"


def _collapse_multivalued(key: str, values, where: str):
    """Collapse a multi-valued property to one value (lossy), logging the loss.

    Returns the single value to write, or ``None`` when there was nothing to
    keep (setting ``None`` removes the property on NA; NDB simply omits it).
    """
    values = list(values)
    if not values:
        logger.warning(
            "Neptune cannot store multi-valued property %r%s: it was "
            "empty, so the property is being dropped. Neptune openCypher "
            "cannot write array values (known limitation).",
            key,
            where,
        )
        return None

    kept, dropped = values[0], values[1:]
    if dropped:
        logger.warning(
            "Neptune cannot store multi-valued property %r%s: keeping "
            "the first value %r and DROPPING %d other value(s) %r. "
            "Neptune openCypher cannot write array values (known "
            "limitation); this copy is lossy for this property.",
            key,
            where,
            kept,
            len(dropped),
            dropped,
        )
    return kept


def _convert_unsupported_values(props: dict, context: str = None):
    """Normalize property values into forms Neptune openCypher can write.

    All values are sent as JSON parameters, but *valid JSON* is not the same as
    *writable by Neptune openCypher*. In particular, Neptune openCypher cannot
    write an array/list property value on EITHER backend:

    - Neptune Analytics rejects it with
      ``ValidationException: Unsupported property value provided``.
    - Neptune Database rejects it with
      ``BadRequestException: Property value must be a simple literal``.

    This is a documented Neptune constraint, not a plugin choice: a property
    value must be a simple scalar (Bool/Int/Long/Float/Double/String/DateTime).
    Genuine multi-valued (set-cardinality) properties can only be created via
    Gremlin or the bulk loader; openCypher ``SET`` has no array write path, and
    Neptune Analytics has no Gremlin endpoint at all. See the AWS openCypher
    data model docs and the ``neptune-no-array-props`` project memory.

    Multi-valued source properties reach this function as tuples (the type
    retriever converts source lists to tuples so nodestream core's debouncer can
    hash property values). Because they cannot be written faithfully, we perform
    a LOSSY collapse to the first value and log every occurrence so the data loss
    is visible in a copy run. This is a known limitation (see the Neptune plugin
    docs); use Gremlin or the bulk loader if faithful multi-valued fidelity is
    required.

    Every value that survives this function must be something the plugin can
    actually send: ``str``, ``bool``, ``int``, ``float`` or ``None`` (datetimes
    are converted to POSIX seconds here). Anything else (a source driver's
    own temporal, spatial or vector type, ``Decimal``, ``bytes``, ``date``,
    ``timedelta``, a ``dict``) cannot be serialized for either backend. It
    raises ``TypeError`` immediately, naming the property, rather than
    failing later inside the connection, where errors are logged and the
    batch is dropped while the run still exits 0.
    """
    where = f" on {context}" if context else ""
    for k, v in props.items():
        # Multi-valued (list/tuple) property: Neptune openCypher cannot write an
        # array value, so collapse to a single value (lossy) first. Everything
        # below then applies to the value that will actually be written.
        if isinstance(v, (tuple, list)):
            v = _collapse_multivalued(k, v, where)

        # Timestamp and datetime are not valid json. Convert to POSIX timestamp instead.
        if isinstance(v, (Timestamp, datetime)):
            v = v.timestamp()
        elif isinstance(v, numbers.Number) and not math.isfinite(v):
            raise ValueError("NaN and Infinity float values are not supported")
        elif v is not None and not isinstance(v, _WRITABLE_SCALARS):
            raise TypeError(
                f"Cannot write property {k!r}{where}: a value of type "
                f"{_type_name(v)} is not supported by the Neptune plugin. "
                f"{_SUPPORTED_TYPES_HINT}"
            )

        props[k] = v

    return props


class NeptuneIngestQueryBuilder:
    def __init__(self, include_label_in_id: bool = True):
        self.include_label_in_id = include_label_in_id

    @cache
    @correct_parameters
    def generate_update_node_operation_query_statement(
        self,
        operation: OperationOnNodeIdentity,
    ) -> str:
        """Generate a query to update a node in the database given a node type and a match strategy."""

        node_id_param_name = generate_id_param_name(GENERIC_NODE_REF_NAME)

        if operation.node_creation_rule == NodeCreationRule.EAGER:
            merge_node = str(
                _merge_node(
                    operation.node_identity.type,
                    node_id_param_name,
                    GENERIC_NODE_REF_NAME,
                )
            )
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
        else:
            query = str(_match_node(operation, node_id_param_name))
            query += f""" SET {GENERIC_NODE_REF_NAME} += removeKeyFromMap(param, "{node_id_param_name}")"""

        if operation.node_identity.additional_types:
            additional_types = ":".join(operation.node_identity.additional_types)
            query += f" SET {GENERIC_NODE_REF_NAME}:{additional_types}"

        return query

    def generate_update_node_operation_params(self, node: Node) -> dict:
        """Generate the parameters for a query to update a node in the database."""

        context = f"node of type {node.type!r}"
        node_props = {
            **self.generate_node_key_params(node),
            **_drop_reserved_properties(node.properties, context),
        }
        node_props = _convert_unsupported_values(node_props, context=context)

        return node_props

    def generate_node_key_params(self, node: Node, name=GENERIC_NODE_REF_NAME) -> dict:
        """Generate the parameters for a query to update a node in the database."""

        # Todo: What if no keys were given? Maybe let neptune decides.
        # On uniqueness and keys in Neptune, see Schema Constraints in https://docs.aws.amazon.com/neptune/latest/userguide/migration-compatibility.html
        composite_key = "_".join(
            [f"{k}:{node.key_values[k] or ''}" for k in sorted(node.key_values)]
        )

        if self.include_label_in_id:
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

        if operation.relationship_creation_rule == RelationshipCreationRule.CREATE:
            create_rel_segment = str(merge_rel_segment).replace("MERGE", "CREATE")
            set_properties_segment = f"""SET {RELATIONSHIP_REF_NAME} += removeKeyFromMap(removeKeyFromMap(param, "{from_node_id_param_name}"), "{to_node_id_param_name}")"""

            return f"{match_from_node_segment} {match_to_node_segment} {create_rel_segment} {set_properties_segment}"

        # At this time, Neptune doesn't support nested maps very well.
        # See comments in generate_update_node_operation_query_statement()
        on_create = f"""ON CREATE SET {RELATIONSHIP_REF_NAME} = removeKeyFromMap(removeKeyFromMap(param, "{from_node_id_param_name}"), "{to_node_id_param_name}")"""

        on_match = f"""ON MATCH SET {RELATIONSHIP_REF_NAME} += removeKeyFromMap(removeKeyFromMap(param, "{from_node_id_param_name}"), "{to_node_id_param_name}")"""

        return f"{match_from_node_segment} {match_to_node_segment} {merge_rel_segment} {on_create} {on_match}"

    def generate_update_rel_params(self, rel: Relationship) -> dict:
        """Generate the parameters for a query to update a relationship in the database."""

        context = f"relationship of type {rel.type!r}"
        return _convert_unsupported_values(
            {**rel.key_values, **_drop_reserved_properties(rel.properties, context)},
            context=context,
        )

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
        query = self.generate_update_node_operation_query_statement(operation=operation)

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
