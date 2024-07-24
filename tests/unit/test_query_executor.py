import pytest
from hamcrest import assert_that
from nodestream.model import TimeToLiveConfiguration
from nodestream.schema import GraphObjectType

from nodestream_plugin_neptune.neptune_connection import NeptuneConnection
from nodestream_plugin_neptune.neptune_query_executor import \
    NeptuneQueryExecutor
from nodestream_plugin_neptune.query import Query, QueryBatch

from .matchers import ran_query


@pytest.fixture
def some_query():
    return Query("MATCH (n) RETURN n LIMIT $limit", {"limit ": "10"})


@pytest.fixture
def some_query_batch(some_query):
    return QueryBatch(some_query.query_statement, [some_query.parameters] * 10)


@pytest.fixture
def query_executor(mocker):
    ingest_query_builder_mock = mocker.Mock()
    database_connection = mocker.AsyncMock(NeptuneConnection)
    return NeptuneQueryExecutor(database_connection, ingest_query_builder_mock)


@pytest.mark.asyncio
async def test_upsert_nodes_in_bulk_of_same_operation(query_executor, some_query_batch):
    query_executor.ingest_query_builder.generate_batch_update_node_operation_batch.return_value = (
        some_query_batch
    )
    await query_executor.upsert_nodes_in_bulk_with_same_operation(None, None)
    query_executor.ingest_query_builder.generate_batch_update_node_operation_batch.assert_called_once_with(
        None, None
    )

    expected_query = some_query_batch.as_query()
    assert_that(query_executor, ran_query(expected_query))


@pytest.mark.asyncio
async def test_upsert_rel_in_bulk_of_same_shape(
    mocker, query_executor, some_query_batch
):
    query_executor.ingest_query_builder.generate_batch_update_relationship_query_batch.return_value = (
        some_query_batch
    )
    query_executor.execute = mocker.AsyncMock()
    await query_executor.upsert_relationships_in_bulk_of_same_operation(None, None)
    query_executor.ingest_query_builder.generate_batch_update_relationship_query_batch.assert_called_once_with(
        None, None
    )

    expected_query = some_query_batch.as_query()
    assert_that(query_executor, ran_query(expected_query))


@pytest.mark.asyncio
async def test_perform_ttl_op(query_executor, some_query):
    ttl_config = TimeToLiveConfiguration(GraphObjectType.NODE, "NodeType")
    query_generator = (
        query_executor.ingest_query_builder.generate_ttl_query_from_configuration
    )
    query_generator.return_value = some_query
    await query_executor.perform_ttl_op(ttl_config)
    query_generator.assert_called_once_with(ttl_config)
    assert_that(query_executor, ran_query(some_query))


@pytest.mark.asyncio
async def test_execute_hook(query_executor, some_query, mocker):
    hook = mocker.Mock()
    hook.as_cypher_query_and_parameters = mocker.Mock(
        return_value=(some_query.query_statement, some_query.parameters)
    )
    await query_executor.execute_hook(hook)
    hook.as_cypher_query_and_parameters.assert_called_once()
    assert_that(query_executor, ran_query(some_query))
