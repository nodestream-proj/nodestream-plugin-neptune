import pytest
from hamcrest import assert_that, equal_to, has_length

from nodestream_plugin_neptune.neptune_connector import NeptuneConnector
from nodestream_plugin_neptune.neptune_query_executor import NeptuneQueryExecutor
from nodestream_plugin_neptune.type_retriever import (
    FETCH_ALL_NODES_BY_TYPE_QUERY_FORMAT,
    FETCH_ALL_RELATIONSHIPS_BY_TYPE_QUERY_FORMAT,
    NeptuneDBTypeRetriever,
)


@pytest.fixture
def connector(mocker):
    return mocker.Mock(NeptuneConnector)


@pytest.fixture
def subject(connector):
    return NeptuneDBTypeRetriever(connector)


def test_get_node_type_extractor_query(subject):
    extractor = subject.get_node_type_extractor("Person")
    expected = FETCH_ALL_NODES_BY_TYPE_QUERY_FORMAT.format(type="Person")
    assert_that(extractor.query, equal_to(expected))


def test_get_relationship_type_extractor_query(subject):
    extractor = subject.get_relationship_type_extractor("Person", "Movie", "ACTED_IN")
    expected = FETCH_ALL_RELATIONSHIPS_BY_TYPE_QUERY_FORMAT.format(
        from_type="Person", rel_type="ACTED_IN", to_type="Movie"
    )
    assert_that(extractor.query, equal_to(expected))


async def async_generator(*items):
    for item in items:
        yield item


@pytest.mark.asyncio
async def test_get_nodes_of_type(subject, mocker):
    subject.map_neptune_node_to_nodestream_node = mocker.Mock()
    subject.get_node_type_extractor = mocker.Mock()
    extractor = subject.get_node_type_extractor.return_value
    extractor.extract_records.return_value = async_generator(
        {"n": {"~labels": ["Person"], "~properties": {"name": "Alice"}}},
        {"n": {"~labels": ["Person"], "~properties": {"name": "Bob"}}},
    )

    results = [r async for r in subject.get_nodes_of_type("Person")]

    assert_that(results, has_length(2))
    subject.get_node_type_extractor.assert_called_once_with("Person")


@pytest.mark.asyncio
async def test_get_relationships_of_type_between(subject, mocker):
    subject.map_neptune_node_to_nodestream_node = mocker.Mock()
    subject.map_neptune_relationship_to_nodestream_relationship = mocker.Mock()
    subject.get_relationship_type_extractor = mocker.Mock()
    extractor = subject.get_relationship_type_extractor.return_value
    extractor.extract_records.return_value = async_generator(
        {
            "a": {"~labels": ["Person"], "~properties": {"name": "Alice"}},
            "b": {"~labels": ["Movie"], "~properties": {"title": "X"}},
            "r": {"~type": "ACTED_IN", "~properties": {}},
        },
        {
            "a": {"~labels": ["Person"], "~properties": {"name": "Bob"}},
            "b": {"~labels": ["Movie"], "~properties": {"title": "Y"}},
            "r": {"~type": "ACTED_IN", "~properties": {}},
        },
    )

    results = [
        r
        async for r in subject.get_relationships_of_type_between(
            "Person", "Movie", "ACTED_IN"
        )
    ]

    assert_that(results, has_length(2))
    subject.get_relationship_type_extractor.assert_called_once_with(
        "Person", "Movie", "ACTED_IN"
    )


@pytest.mark.asyncio
async def test_preview_node_count(subject, mocker):
    """Verifies preview_node_count calls executor.query() and parses the count.

    This test will fail because NeptuneQueryExecutor has no query() method.
    """
    query_executor = mocker.AsyncMock(NeptuneQueryExecutor)
    query_executor.query.return_value = {"results": [{"count": 42}]}
    subject.connector.make_query_executor.return_value = query_executor

    result = await subject.preview_node_count("Person")

    assert_that(result, equal_to(42))
    query_executor.query.assert_called_once()


@pytest.mark.asyncio
async def test_preview_relationship_count(subject, mocker):
    """Verifies preview_relationship_count calls executor.query() and parses the count.

    This test will fail because NeptuneQueryExecutor has no query() method.
    """
    query_executor = mocker.AsyncMock(NeptuneQueryExecutor)
    query_executor.query.return_value = {"results": [{"count": 100}]}
    subject.connector.make_query_executor.return_value = query_executor

    result = await subject.preview_relationship_count("ACTED_IN")

    assert_that(result, equal_to(100))
    query_executor.query.assert_called_once()


@pytest.mark.asyncio
async def test_preview_node_count_empty(subject, mocker):
    """Verifies preview_node_count returns 0 when no results."""
    query_executor = mocker.AsyncMock(NeptuneQueryExecutor)
    query_executor.query.return_value = {"results": []}
    subject.connector.make_query_executor.return_value = query_executor

    result = await subject.preview_node_count("NonExistent")

    assert_that(result, equal_to(0))
