from logging import getLogger
from typing import Iterable
from nodestream.model import IngestionHook, Node, RelationshipWithNodes, TimeToLiveConfiguration
from nodestream.schema.indexes import FieldIndex, KeyIndex
from nodestream.databases.query_executor import (
    OperationOnNodeIdentity,
    OperationOnRelationshipIdentity,
    QueryExecutor,
)
from .ingest_query_builder import NeptuneDBIngestQueryBuilder
from .query import Query, QueryBatch
from aiobotocore.session import get_session
import json
import asyncio
import math


class NeptuneQueryExecutor(QueryExecutor):
    def __init__(
        self,
        region,
        host,
        ingest_query_builder: NeptuneDBIngestQueryBuilder,
        async_partitions = 50
    ) -> None:
        self.session = get_session()
        self.region = region
        self.host = host
        self.ingest_query_builder = ingest_query_builder
        self.logger = getLogger(self.__class__.__name__)
        self.async_partitions = async_partitions

    async def upsert_nodes_in_bulk_with_same_operation(
        self, operation: OperationOnNodeIdentity, nodes: Iterable[Node]
    ):
        batched_query = (
            self.ingest_query_builder.generate_batch_update_node_operation_batch(
                operation, nodes
            )
        )
        await self.execute(batched_query.as_query())

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
        await self.execute(queries.as_query())

    async def upsert_key_index(self, index: KeyIndex):
        # Not needed for Neptune
        pass

    async def upsert_field_index(self, index: FieldIndex):
        # Not needed for Neptune
        pass

    async def perform_ttl_op(self, config: TimeToLiveConfiguration):
        # TODO: Implement as appropriate
        pass

    async def execute_hook(self, hook: IngestionHook):
        # TODO: Implement as appropriate
        pass

    def _split_parameters(self, parameters: list):
        """
        The intention is to split the parameters evenly so we can make multiple 
        async batch inserts to Neptune. However, I did not notice any meaningful 
        performance improvements. 

        It could be that we are doing something incorrect, or that the input
        size tested (1000 nodes, 2000 edges) weren't enough.
        """
        params_count = len(parameters)
        if not self.async_partitions or params_count < self.async_partitions:
            partition_size = len(parameters)
        else:
            partition_size = math.floor(params_count/self.async_partitions)

        """
        From Dave we found that the sweet spot is around 100 - 200
        per batch request. Though this is not a hard rule.

        More investigation on performance is needed.
        """
        partition_size = 150

        for i in range(0, len(parameters), partition_size):
            yield parameters[i: i+partition_size]

    async def query(self, query_stmt: str, parameters: list):
        response = None
        async with self.session.create_client("neptunedata", region_name=self.region, endpoint_url=self.host) as client:
            try:
                response = await client.execute_open_cypher_query(
                    openCypherQuery=query_stmt,
                    # Use json.dumps() to warp dict's key/values in double quotes.
                    parameters=json.dumps({"params": parameters})
                )

                code = response["ResponseMetadata"]["HTTPStatusCode"]
                if code != 200:
                    self.logger.error(f"Query `{query_stmt}` failed with response:\n{response}")
                
            except Exception:
                self.logger.exception(f'Unexpected error for query: {query_stmt}.', stack_info=True)
            
        return response

    async def execute(self, query: Query, log_result: bool = False):
        query_stmt = query.query_statement
        requests = (
            self.query(query_stmt, parameters) 
            for parameters
            in self._split_parameters(query.parameters["params"])
        )
        await asyncio.gather(*requests)

