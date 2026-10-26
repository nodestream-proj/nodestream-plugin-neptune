import json

import pytest
from hamcrest import assert_that, equal_to, has_key, is_not
from nodestream_plugin_neptune.neptune_connection import NeptuneAnalyticsConnection


class FakeStreamingBody:
    """Stands in for botocore's StreamingBody: readable once, then closable."""

    def __init__(self, data: bytes):
        self._data = data
        self.closed = False

    async def read(self):
        data, self._data = self._data, b""
        return data

    def close(self):
        self.closed = True


def make_client(response):
    class FakeExceptions:
        class ConflictException(Exception):
            pass

        class AccessDeniedException(Exception):
            pass

    class FakeClient:
        exceptions = FakeExceptions

        async def execute_query(self, **kwargs):
            self.kwargs = kwargs
            return response

    return FakeClient()


@pytest.fixture
def connection():
    return NeptuneAnalyticsConnection(graph_id="g-1234567890", region="us-west-2")


@pytest.mark.asyncio
async def test_results_are_decoded_from_payload(connection):
    body = json.dumps({"results": [{"count": 9892}]}).encode()
    response = {"ResponseMetadata": {}, "payload": FakeStreamingBody(body)}
    client = make_client(response)

    result = await connection._execute_query(client, "MATCH (n) RETURN count(n)", {})

    assert_that(result["results"], equal_to([{"count": 9892}]))
    assert_that(client.kwargs["graphIdentifier"], equal_to("g-1234567890"))


@pytest.mark.asyncio
async def test_empty_result_set_decodes_to_empty_list(connection):
    body = json.dumps({"results": []}).encode()
    client = make_client({"payload": FakeStreamingBody(body)})

    result = await connection._execute_query(client, "MATCH (n) RETURN n", {})

    assert_that(result["results"], equal_to([]))


@pytest.mark.asyncio
async def test_empty_body_decodes_to_empty_list(connection):
    client = make_client({"payload": FakeStreamingBody(b"")})

    result = await connection._execute_query(client, "CREATE (n)", {})

    assert_that(result["results"], equal_to([]))


@pytest.mark.asyncio
async def test_response_without_payload_is_unchanged(connection):
    client = make_client({"ResponseMetadata": {}})

    result = await connection._execute_query(client, "RETURN 1", {})

    assert_that(result, is_not(has_key("results")))


@pytest.mark.asyncio
async def test_execute_returns_results_and_closes_payload(connection):
    """End to end through `execute`: results survive the payload being closed."""
    body = json.dumps({"results": [{"count": 3}]}).encode()
    payload = FakeStreamingBody(body)
    connection.client = make_client(
        {"ResponseMetadata": {"HTTPStatusCode": 200}, "payload": payload}
    )

    result = await connection.execute("MATCH (n) RETURN count(n) AS count", {})

    assert_that(result["results"], equal_to([{"count": 3}]))
    assert_that(payload.closed, equal_to(True))
