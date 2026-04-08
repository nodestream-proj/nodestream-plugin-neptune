import pytest

from nodestream_plugin_neptune.extractor import NeptuneDBExtractor
from nodestream_plugin_neptune.neptune_connector import NeptuneConnector
from nodestream_plugin_neptune.neptune_query_executor import NeptuneQueryExecutor


@pytest.fixture
def query_executor(mocker):
    return mocker.AsyncMock(NeptuneQueryExecutor)


@pytest.fixture
def connector(mocker, query_executor):
    connector = mocker.Mock(NeptuneConnector)
    connector.make_query_executor.return_value = query_executor
    return connector


@pytest.mark.asyncio
async def test_extract_records_calls_query(connector, query_executor):
    """Verifies that the extractor can call executor.query() and get results back.

    This test will fail because NeptuneQueryExecutor does not have a query() method.
    The method was removed during the connector refactor (commit a62a1da, March 2024)
    but the extractor still calls it.
    """
    extractor = NeptuneDBExtractor(
        query="MATCH (n:Person) RETURN n SKIP $offset LIMIT $limit",
        connector=connector,
        limit=2,
    )

    query_executor.query.side_effect = [
        {"results": [{"n": {"name": "Alice"}}, {"n": {"name": "Bob"}}]},
        {"results": []},
    ]

    result = [item async for item in extractor.extract_records()]

    assert len(result) == 2
    assert result[0] == {"n": {"name": "Alice"}}
    assert query_executor.query.call_count == 2


@pytest.mark.asyncio
async def test_extract_records_paginates(connector, query_executor):
    """Verifies that the extractor fetches ALL pages, not just the first.

    The extractor should keep incrementing offset and fetching until it gets
    an empty result set — matching the Neo4j plugin's behavior. This test will
    fail because the current implementation only fetches a single page.
    """
    extractor = NeptuneDBExtractor(
        query="MATCH (n:Person) RETURN n SKIP $offset LIMIT $limit",
        connector=connector,
        limit=2,
    )

    # Three pages: 2 results, 1 result, empty (signals stop)
    query_executor.query.side_effect = [
        {"results": [{"n": {"name": "Alice"}}, {"n": {"name": "Bob"}}]},
        {"results": [{"n": {"name": "Charlie"}}]},
        {"results": []},
    ]

    result = [item async for item in extractor.extract_records()]

    assert len(result) == 3
    assert result[2] == {"n": {"name": "Charlie"}}
    assert query_executor.query.call_count == 3


@pytest.mark.asyncio
async def test_extract_records_empty_result(connector, query_executor):
    """Verifies the extractor handles an empty first page gracefully."""
    extractor = NeptuneDBExtractor(
        query="MATCH (n:Person) RETURN n SKIP $offset LIMIT $limit",
        connector=connector,
        limit=100,
    )

    query_executor.query.side_effect = [{"results": []}]

    result = [item async for item in extractor.extract_records()]

    assert len(result) == 0
