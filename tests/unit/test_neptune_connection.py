import pytest
from hamcrest import assert_that, equal_to
from nodestream_plugin_neptune.neptune_connection import (
    NeptuneAnalyticsConnection,
    NeptuneDBConnection,
)


@pytest.mark.asyncio
async def test_client_has_region_registered():
    connection: NeptuneDBConnection = NeptuneDBConnection(
        host="https://test-endpoint.com", region="test-region"
    )
    async with connection._create_boto_client() as client:
        assert_that(client.meta.region_name, equal_to("test-region"))


@pytest.mark.asyncio
async def test_client_has_region_registered_analytics():
    connection: NeptuneAnalyticsConnection = NeptuneAnalyticsConnection(
        graph_id="test_id", region="test-region"
    )
    async with connection._create_boto_client() as client:
        assert_that(client.meta.region_name, equal_to("test-region"))


@pytest.mark.asyncio
async def test_client_open_close_once_db(mocker):
    context_manager = mocker.AsyncMock()
    connection: NeptuneDBConnection = NeptuneDBConnection(
        host="https://test-endpoint.com", region="test-region"
    )
    connection._create_boto_client = mocker.Mock(return_value=context_manager)
    await connection.execute("test_query", "test_params")
    connection.boto_context_manager.__aenter__.assert_awaited_once()

    await connection.close()
    connection.boto_context_manager.__aexit__.assert_awaited_once()


@pytest.mark.asyncio
async def test_client_open_close_once_analytics(mocker):
    context_manager = mocker.AsyncMock()
    connection: NeptuneAnalyticsConnection = NeptuneAnalyticsConnection(
        graph_id="test_id", region="test-region"
    )
    connection._create_boto_client = mocker.Mock(return_value=context_manager)
    await connection.execute("test_query", "test_params")
    connection.boto_context_manager.__aenter__.assert_awaited_once()

    await connection.close()
    connection.boto_context_manager.__aexit__.assert_awaited_once()


@pytest.mark.asyncio
async def test_client_not_open_close_db(mocker):
    context_manager = mocker.AsyncMock()
    connection: NeptuneDBConnection = NeptuneDBConnection(
        host="https://test-endpoint.com", region="test-region"
    )
    connection._create_boto_client = mocker.Mock(return_value=context_manager)
    context_manager.__aenter__.assert_not_awaited()
    await connection.close()
    context_manager.__aexit__.assert_not_awaited()


@pytest.mark.asyncio
async def test_client_not_open_close_analytics(mocker):
    context_manager = mocker.AsyncMock()
    connection: NeptuneDBConnection = NeptuneDBConnection(
        host="https://test-endpoint.com", region="test-region"
    )
    connection._create_boto_client = mocker.Mock(return_value=context_manager)
    context_manager.__aenter__.assert_not_awaited()
    await connection.close()
    context_manager.__aexit__.assert_not_awaited()


@pytest.mark.asyncio
async def test_same_clients_db():
    connection: NeptuneDBConnection = NeptuneDBConnection(
        host="https://test-endpoint.com", region="test-region"
    )
    await connection.execute("test_query", "test_params")
    og_client = connection.client
    await connection.execute("test_query", "test_params")
    assert_that(og_client, equal_to(connection.client))


@pytest.mark.asyncio
async def test_same_clients_analytics():
    connection: NeptuneDBConnection = NeptuneDBConnection(
        host="https://test-endpoint.com", region="test-region"
    )
    await connection.execute("test_query", "test_params")
    og_client = connection.client
    await connection.execute("test_query", "test_params")
    assert_that(og_client, equal_to(connection.client))
