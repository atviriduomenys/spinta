import json
from collections.abc import Iterator

import pytest
import requests
from _pytest.capture import CaptureFixture
from requests import ConnectTimeout, HTTPError, JSONDecodeError, ReadTimeout, Timeout
from responses import GET, RequestsMock

from spinta.cli.helpers.errors import ErrorCounter
from spinta.utils.response import (
    RequestResult,
    async_response_iterator,
    format_request_error,
    get_request_with_retries,
    request,
)


def test_request_error_counter(responses: RequestsMock):
    server = "https://www.example.com"
    error_counter = ErrorCounter(max_count=10)
    responses.add(GET, server, body="RESULT", status=400)

    assert not error_counter.has_errors()
    assert not error_counter.has_reached_max()
    assert error_counter.count == 0

    client = requests.Session()
    result = request(client, server, "GET", error_counter=error_counter)
    assert isinstance(result, RequestResult)
    assert result.status_code == 400
    assert result.data is None
    assert result.text == "RESULT"
    assert result.ok is False
    assert isinstance(result.exception, requests.JSONDecodeError)

    assert error_counter.has_errors()
    assert not error_counter.has_reached_max()
    assert error_counter.count == 1


def test_format_request_error(responses: RequestsMock):
    server = "https://www.example.com"
    responses.add(GET, server, body="RESULT", status=400)

    client = requests.Session()
    result = request(client, server, "GET")
    assert isinstance(result, RequestResult)
    assert result.status_code == 400
    assert result.data is None
    assert result.text == "RESULT"
    assert result.ok is False
    assert isinstance(result.exception, requests.JSONDecodeError)

    assert format_request_error(result, server, (5, 300)) == (
        "Given response is not in JSON format.\nServer (https://www.example.com) response (status=400):\n    RESULT"
    )


def test_request_ignore_status(responses: RequestsMock):
    server = "https://www.example.com"
    responses.add(GET, server, body="RESULT", status=400)

    client = requests.Session()
    result = request(client, server, "GET", ignore_statuses=[400])
    assert isinstance(result, RequestResult)
    assert result.status_code == 400
    assert result.data is None
    assert result.text == "RESULT"
    assert isinstance(result.exception, requests.JSONDecodeError)
    assert result.ok is True
    assert result.ignored is True


def test_request_stop_on_error_http_error(responses: RequestsMock):
    server = "https://www.example.com"
    responses.add(
        GET,
        server,
        json={"_errors": ["SpintaError"]},
        status=400,
    )

    client = requests.Session()
    with pytest.raises(HTTPError):
        request(client, server, "GET", stop_on_error=True)


def test_request_stop_on_error_json_decode_error(responses: RequestsMock):
    server = "https://www.example.com"
    responses.add(
        GET,
        server,
        body="TEST",
        status=400,
    )

    client = requests.Session()
    with pytest.raises(JSONDecodeError):
        request(client, server, "GET", stop_on_error=True)


@pytest.mark.parametrize("timeout_type", [ReadTimeout, ConnectTimeout])
def test_request_stop_on_error_timeout_error(responses: RequestsMock, timeout_type: type[Timeout]):
    server = "https://www.example.com"
    responses.add(
        GET,
        server,
        body=timeout_type(),
    )

    client = requests.Session()
    with pytest.raises(timeout_type):
        request(client, server, "GET", stop_on_error=True)


def test_request_json_response(responses: RequestsMock):
    server = "https://www.example.com"
    responses.add(
        GET,
        server,
        json={
            "test": {"data": 1},
        },
    )

    client = requests.Session()
    result = request(client, server, "GET")
    assert isinstance(result, RequestResult)
    assert result.status_code == 200
    assert result.data == {"test": {"data": 1}}


def test_request_non_json_response(responses: RequestsMock):
    server = "https://www.example.com"
    responses.add(GET, server, body="RESULT", status=400)

    client = requests.Session()
    result = request(client, server, "GET")
    assert isinstance(result, RequestResult)
    assert result.status_code == 400
    assert result.data is None
    assert result.text == "RESULT"
    assert result.ok is False
    assert isinstance(result.exception, requests.JSONDecodeError)


@pytest.mark.parametrize("timeout_type", [ReadTimeout, ConnectTimeout])
def test_request_timeout(responses: RequestsMock, timeout_type: type[Timeout]):
    server = "https://www.example.com"
    responses.add(GET, server, body=timeout_type())

    client = requests.Session()
    result = request(client, server, "GET")
    assert isinstance(result, RequestResult)
    assert result.status_code is None
    assert result.data is None
    assert result.text is None
    assert result.ok is False
    assert isinstance(result.exception, timeout_type)


def test_request_http_error(responses: RequestsMock):
    server = "https://www.example.com"
    responses.add(
        GET,
        server,
        json={
            "_errors": ["TestError"],
        },
        status=400,
    )

    client = requests.Session()
    result = request(client, server, "GET")
    assert isinstance(result, RequestResult)
    assert result.status_code == 400
    assert result.data == {"_errors": ["TestError"]}
    assert result.ok is False
    assert result.exception is None


def test_get_retry_non_json_response_message(responses: RequestsMock, capsys: CaptureFixture):
    server = "https://www.example.com"
    responses.add(GET, server, body="RESULT", status=400)

    client = requests.Session()
    result = get_request_with_retries(client, server, timeout=(5, 300), retries=0, delay_range=tuple())
    assert result.status_code == 400
    assert result.data is None
    assert not result.ok

    cap = capsys.readouterr()
    assert cap.err == (
        "Given response is not in JSON format.\nServer (https://www.example.com) response (status=400):\n    RESULT\n"
    )


@pytest.mark.parametrize("retries", [0, 2])
@pytest.mark.parametrize("http_status", [200, 201])
def test_get_retry_non_json_success_exhausted(
    responses: RequestsMock, capsys: CaptureFixture, retries: int, http_status: int
):
    server = "https://www.example.com"
    responses.add(GET, server, body="INVALID JSON", status=http_status)
    error_counter = ErrorCounter(max_count=10)

    result = get_request_with_retries(
        requests.Session(), server, timeout=(5, 300), retries=retries, delay_range=(0,), error_counter=error_counter
    )

    assert result.status_code == http_status
    assert result.data is None
    assert not result.ok
    assert isinstance(result.exception, JSONDecodeError)
    assert len(responses.calls) == retries + 1
    assert error_counter.count == 1
    assert f"response (status={http_status}):" in capsys.readouterr().err


def test_get_retry_non_json_success_recovers(responses: RequestsMock):
    server = "https://www.example.com"
    responses.add(GET, server, body="INVALID JSON", status=200)
    responses.add(GET, server, json={"_data": []}, status=200)
    error_counter = ErrorCounter(max_count=10)

    result = get_request_with_retries(
        requests.Session(), server, timeout=(5, 300), retries=2, delay_range=(0,), error_counter=error_counter
    )

    assert result.status_code == 200
    assert result.data == {"_data": []}
    assert result.ok
    assert len(responses.calls) == 2
    assert error_counter.count == 0


def test_get_retry_read_timeout_message(responses: RequestsMock, capsys: CaptureFixture):
    server = "https://www.example.com"
    responses.add(GET, server, body=ReadTimeout())

    client = requests.Session()
    result = get_request_with_retries(client, server, timeout=(5, 300), retries=0, delay_range=tuple())
    assert result.status_code is None
    assert result.data is None
    assert not result.ok

    cap = capsys.readouterr()
    assert cap.err == "Read timeout occurred. Current timeout settings are (connect: 5s, read: 300s).\n"


def test_get_retry_connect_timeout_message(responses: RequestsMock, capsys: CaptureFixture):
    server = "https://www.example.com"
    responses.add(GET, server, body=ConnectTimeout())

    client = requests.Session()
    result = get_request_with_retries(client, server, timeout=(5, 300), retries=0, delay_range=tuple())
    assert result.status_code is None
    assert result.data is None
    assert not result.ok

    cap = capsys.readouterr()
    assert cap.err == "Connect timeout occurred. Current timeout settings are (connect: 5s, read: 300s).\n"


def test_get_retry_io_error(responses: RequestsMock, capsys: CaptureFixture):
    server = "https://www.example.com"
    responses.add(GET, server, body=IOError("IO Error"))

    client = requests.Session()
    result = get_request_with_retries(client, server, timeout=(5, 300), retries=0, delay_range=tuple())
    assert result.status_code is None
    assert result.data is None
    assert not result.ok

    cap = capsys.readouterr()
    assert cap.err == "Server (https://www.example.com) response (status=None):\n    IO Error\n"


def test_get_retry_spinta_error(responses: RequestsMock, capsys: CaptureFixture):
    server = "https://www.example.com"
    responses.add(GET, server, body=json.dumps({"_errors": ["SpintaError"]}), status=400)

    client = requests.Session()
    result = get_request_with_retries(client, server, timeout=(5, 300), retries=0, delay_range=tuple())
    assert result.status_code == 400
    assert result.data == {"_errors": ["SpintaError"]}
    assert not result.ok

    cap = capsys.readouterr()
    assert cap.err == ("Server (https://www.example.com) response (status=400):\n    {'_errors': ['SpintaError']}\n")


def test_get_retry_http_errors_count_once(responses: RequestsMock):
    server = "https://www.example.com"
    responses.add(GET, server, json={"errors": []}, status=500)
    error_counter = ErrorCounter(max_count=10)

    result = get_request_with_retries(
        requests.Session(), server, timeout=(5, 300), retries=2, delay_range=(0,), error_counter=error_counter
    )

    assert result.status_code == 500
    assert not result.ok
    assert len(responses.calls) == 3
    assert error_counter.count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("values", [[], [1, 2]])
async def test_response_iterator_does_not_close_exhausted_source(values: list[int]) -> None:
    class Stream(Iterator[int]):
        def __init__(self) -> None:
            self.items = iter(values)

        def __next__(self) -> int:
            return next(self.items)

        def close(self) -> None:
            raise AssertionError("An exhausted source should not be closed again")

    assert [item async for item in async_response_iterator(Stream())] == values


@pytest.mark.asyncio
@pytest.mark.parametrize("use_iterable", [False, True], ids=["iterator", "iterable"])
async def test_response_iterator_closes_actual_iterator_when_stopped_early(use_iterable: bool) -> None:
    closed = False

    def data() -> Iterator[int]:
        nonlocal closed
        try:
            yield 1
            yield 2
        finally:
            closed = True

    # Keep a reference so garbage collection cannot perform the cleanup.
    source = data()

    class IterableStream:
        def __iter__(self) -> Iterator[int]:
            return source

    try:
        response = async_response_iterator(IterableStream() if use_iterable else source)
        assert await response.__anext__() == 1
        assert not closed
        await response.aclose()
        assert closed
    finally:
        source.close()
