import pytest
from hamcrest import assert_that, equal_to, instance_of

from nodestream_plugin_neptune import NeptuneConnector
from nodestream_plugin_neptune.neptune_connection import (
    NeptuneAnalyticsConnection, NeptuneDBConnection)
from nodestream_plugin_neptune.neptune_query_executor import \
    NeptuneQueryExecutor
from nodestream_plugin_neptune.type_retriever import NeptuneDBTypeRetriever


@pytest.mark.asyncio
async def test_client_has_region_registered():
    connection: NeptuneDBConnection = NeptuneDBConnection(
        host="https://test-endpoint.com",
        region="test-region"
    )
    async with connection._create_boto_client() as client:
        assert_that(client.meta.region_name, equal_to("test-region"))


@pytest.mark.asyncio
async def test_client_has_region_registered_analytics():
    connection: NeptuneAnalyticsConnection = NeptuneAnalyticsConnection(
        graph_id="test_id",
        region="test-region"
    )
    async with connection._create_boto_client() as client:
        assert_that(client.meta.region_name, equal_to("test-region"))
