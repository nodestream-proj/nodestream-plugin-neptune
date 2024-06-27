import pytest
from hamcrest import assert_that, equal_to, instance_of

from nodestream_plugin_neptune import NeptuneConnector
from nodestream_plugin_neptune.neptune_connection import (
    NeptuneAnalyticsConnection, NeptuneDBConnection)
from nodestream_plugin_neptune.neptune_query_executor import \
    NeptuneQueryExecutor
from nodestream_plugin_neptune.type_retriever import NeptuneDBTypeRetriever


def test_make_neptune_db_query_executor(mocker):
    connector: NeptuneConnector = NeptuneConnector(
        mode="database",
        host="testEndpoint.com",
        ingest_query_builder=mocker.Mock(),
    )
    executor: NeptuneQueryExecutor = connector.make_query_executor()
    assert_that(executor.database_connection, instance_of(NeptuneDBConnection))
    assert_that(executor.database_connection.host, equal_to(connector.host))
    assert_that(executor.ingest_query_builder, equal_to(connector.ingest_query_builder))


def test_make_neptune_analytics_query_executor(mocker):
    connector: NeptuneConnector = NeptuneConnector(
        mode="analytics",
        graph_id="graph_identifier",
        ingest_query_builder=mocker.Mock(),
    )
    executor: NeptuneQueryExecutor = connector.make_query_executor()
    assert_that(executor.database_connection, instance_of(NeptuneAnalyticsConnection))
    assert_that(executor.database_connection.graph_id, equal_to(connector.graph_id))
    assert_that(executor.ingest_query_builder, equal_to(connector.ingest_query_builder))


def test_make_type_retriever(mocker):
    connector: NeptuneConnector = NeptuneConnector(
        mode="database",
        host="testEndpoint.com",
        ingest_query_builder=mocker.Mock(),
    )
    retriever: NeptuneDBTypeRetriever = connector.make_type_retriever()
    assert_that(retriever.connector, equal_to(connector))


def test_from_file_data_host():
    connector: NeptuneConnector = NeptuneConnector.from_file_data(
        mode="database",
        host="testEndpoint.com",
    )
    assert_that(connector.host, equal_to("testEndpoint.com"))


def test_database_must_have_host():
    with pytest.raises(ValueError) as valErr:
        NeptuneConnector.from_file_data(
            mode="database",
        )
    assert_that(
        str(valErr.value),
        equal_to("A `host` must be specified when `mode` is 'database'."),
    )


def test_database_must_not_have_graph_id():
    with pytest.raises(ValueError) as valErr:
        NeptuneConnector.from_file_data(
            mode="database",
            host="testHost",
            graph_id="testID",
        )
    assert_that(
        str(valErr.value),
        equal_to(
            "A `graph_id` should not be used with Neptune Database, `host=<Neptune Endpoint>` should be used instead. If using Neptune Analytics, set `mode='analytics'."
        ),
    )


def test_analytics_must_have_graph_id():
    with pytest.raises(ValueError) as valErr:
        NeptuneConnector.from_file_data(
            mode="analytics",
        )
    assert_that(
        str(valErr.value),
        equal_to("A `graph_id` must be specified when `mode` is 'analytics'."),
    )


def test_analytics_must_not_have_host():
    with pytest.raises(ValueError) as valErr:
        NeptuneConnector.from_file_data(
            mode="analytics",
            host="testHost",
            graph_id="testID",
        )
    assert_that(
        str(valErr.value),
        equal_to(
            "A `host` should not be used with Neptune Analytics, `graph_id=<Graph Identifier>` should be used instead. If using Neptune Database, set `mode='database'."
        ),
    )


def test_include_label_in_id():
    connector: NeptuneConnector = NeptuneConnector.from_file_data(
        mode="database",
        host="testEndpoint.com",
        include_label_in_id=False
    )
    assert_that(connector.ingest_query_builder.include_label_in_id, equal_to(False))


def test_include_label_in_id_default_true():
    connector: NeptuneConnector = NeptuneConnector.from_file_data(
        mode="database",
        host="testEndpoint.com"
    )
    assert_that(connector.ingest_query_builder.include_label_in_id, equal_to(True))

def test_region_passed_to_client():
    connector: NeptuneConnector = NeptuneConnector.from_file_data(
        mode="database",
        host="testEndpoint.com",
        region="test-region"
    )
    assert_that(connector.connection.region, equal_to("test-region"))
    assert_that(connector.region, equal_to("test-region"))

def test_default_region():
    connector: NeptuneConnector = NeptuneConnector.from_file_data(
        mode="database",
        host="testEndpoint.com"
    )
    assert_that(connector.connection.region, equal_to(None))
    assert_that(connector.region, equal_to(None))

def test_region_passed_to_client_analytics():
    connector: NeptuneConnector = NeptuneConnector.from_file_data(
        mode="analytics",
        graph_id="testEndpoint.com",
        region="test-region"
    )
    assert_that(connector.connection.region, equal_to("test-region"))
    assert_that(connector.region, equal_to("test-region"))

def test_default_region_analytics():
    connector: NeptuneConnector = NeptuneConnector.from_file_data(
        mode="analytics",
        graph_id="testEndpoint.com"
    )
    assert_that(connector.connection.region, equal_to(None))
    assert_that(connector.region, equal_to(None))