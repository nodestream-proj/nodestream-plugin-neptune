import asyncio
from logging import getLogger
from typing import Iterable

from nodestream.databases.query_executor import (
    OperationOnNodeIdentity, OperationOnRelationshipIdentity, QueryExecutor)
from nodestream.model import (IngestionHook, Node, RelationshipWithNodes,
                              TimeToLiveConfiguration)

from .ingest_query_builder import NeptuneIngestQueryBuilder
from .neptune_connection import NeptuneConnection
from .query import Query, QueryBatch


class NeptuneQueryExecutor(QueryExecutor):
    def __init__(
        self,
        connection: NeptuneConnection,
        ingest_query_builder: NeptuneIngestQueryBuilder,
    ) -> None:
        self.database_connection = connection
        self.ingest_query_builder = ingest_query_builder
        self.logger = getLogger(self.__class__.__name__)

    async def upsert_nodes_in_bulk_with_same_operation(
        self, operation: OperationOnNodeIdentity, nodes: Iterable[Node]
    ):
        batched_query = (
            self.ingest_query_builder.generate_batch_update_node_operation_batch(
                operation, nodes
            )
        )
        await self.execute_batch(batched_query)

    async def upsert_relationships_in_bulk_of_same_operation(
        self,
        shape: OperationOnRelationshipIdentity,
        relationships: Iterable[RelationshipWithNodes],
    ):
        queries = (
            self.ingest_query_builder.generate_batch_update_relationship_query_batch(
                shape, relationships
            )
        )
        await self.execute_batch(queries)

    async def perform_ttl_op(self, config: TimeToLiveConfiguration):
        query = self.ingest_query_builder.generate_ttl_query_from_configuration(config)
        await self.execute(query)

    async def execute_hook(self, hook: IngestionHook):
        query_string, params = hook.as_cypher_query_and_parameters()
        await self.execute(Query(query_string, params))

    def _split_parameters(self, parameters: list):
        """
        Our current understanding is that a partition_size of 100 - 200
        per batch request will yield the best results. Though this is not a hard rule.

        More investigation on performance is needed.
        """
        partition_size = 150

        for i in range(0, len(parameters), partition_size):
            yield {"params": parameters[i : i + partition_size]}

    async def execute(self, query: Query, log_result: bool = False):
        query_stmt = query.query_statement

        result = await self.database_connection.execute(query_stmt, query.parameters)

        if log_result:
            for record in result.records:
                self.logger.info(
                    "Gathered Query Results",
                    extra=dict(**record, query=query.query_statement),
                )

    async def execute_batch(self, query_batch: QueryBatch, log_result: bool = False):
        query: Query = query_batch.as_query()

        query_stmt = query.query_statement
        requests = (
            self.database_connection.execute(query_stmt, parameters)
            for parameters in self._split_parameters(query.parameters["params"])
        )

        result = await asyncio.gather(*requests)

        if log_result:
            for record in result.records:
                self.logger.info(
                    "Gathered Query Results",
                    extra=dict(**record, query=query.query_statement),
                )
