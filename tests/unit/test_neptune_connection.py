import pytest
from hamcrest import assert_that, equal_to
from nodestream_plugin_neptune.neptune_connection import (
    DEFAULT_MAX_POOL_CONNECTIONS,
    NeptuneAnalyticsConnection,
    NeptuneDBConnection,
    _with_pool_config,
)
import botocore


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


@pytest.mark.asyncio
async def test_concurrent_execute_creates_client_once(mocker):
    import asyncio

    connection: NeptuneDBConnection = NeptuneDBConnection(
        host="https://test-endpoint.com", region="test-region"
    )
    create_calls = 0
    real_cm = mocker.AsyncMock()

    def counting_create():
        nonlocal create_calls
        create_calls += 1
        return real_cm

    connection._create_boto_client = counting_create

    await asyncio.gather(*(connection.execute("q", "p") for _ in range(50)))

    assert create_calls == 1
    real_cm.__aenter__.assert_awaited_once()


def test_with_pool_config_sets_default_when_absent():
    kwargs = _with_pool_config({})
    assert_that(
        kwargs["config"].max_pool_connections,
        equal_to(DEFAULT_MAX_POOL_CONNECTIONS),
    )


def test_with_pool_config_raises_small_caller_pool():
    small = botocore.config.Config(max_pool_connections=5)
    kwargs = _with_pool_config({"config": small})
    assert_that(
        kwargs["config"].max_pool_connections,
        equal_to(DEFAULT_MAX_POOL_CONNECTIONS),
    )


def test_with_pool_config_preserves_large_caller_pool():
    large = botocore.config.Config(max_pool_connections=128)
    kwargs = _with_pool_config({"config": large})
    assert_that(kwargs["config"].max_pool_connections, equal_to(128))


def test_with_pool_config_preserves_other_config_settings():
    cfg = botocore.config.Config(max_pool_connections=5, read_timeout=42)
    kwargs = _with_pool_config({"config": cfg})
    assert_that(
        kwargs["config"].max_pool_connections,
        equal_to(DEFAULT_MAX_POOL_CONNECTIONS),
    )
    assert_that(kwargs["config"].read_timeout, equal_to(42))


def test_db_client_has_enlarged_pool():
    connection = NeptuneDBConnection(
        host="https://test-endpoint.com", region="test-region"
    )
    client_kwargs = _with_pool_config(connection.client_kwargs)
    assert_that(
        client_kwargs["config"].max_pool_connections,
        equal_to(DEFAULT_MAX_POOL_CONNECTIONS),
    )
