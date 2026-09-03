import json
import sys

import pytest
import requests
from _pytest.capture import CaptureFixture
from requests import ConnectTimeout, HTTPError, JSONDecodeError, ReadTimeout, Timeout
from responses import GET, RequestsMock

from spinta.cli.helpers.errors import ErrorCounter
from spinta.utils.response import RequestResult, get_request_with_retries, request


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


def test_request_on_error(responses: RequestsMock, capsys: CaptureFixture):
    def _on_error(response: RequestResult):
        print("ON ERROR INTERCEPTION", file=sys.stderr)

    server = "https://www.example.com"
    responses.add(GET, server, body="RESULT", status=400)

    client = requests.Session()
    result = request(client, server, "GET", on_error=_on_error)
    assert isinstance(result, RequestResult)
    assert result.status_code == 400
    assert result.data is None
    assert result.text == "RESULT"
    assert result.ok is False
    assert isinstance(result.exception, requests.JSONDecodeError)

    cap = capsys.readouterr()
    assert cap.err == "ON ERROR INTERCEPTION\n"


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
    status_code, result = get_request_with_retries(client, server, timeout=(5, 300), retries=0, delay_range=tuple())
    assert status_code == 400
    assert result is None

    cap = capsys.readouterr()
    assert cap.err == "ERROR (400): Given response from https://www.example.com is not in JSON format:\n    RESULT\n"


def test_get_retry_read_timeout_message(responses: RequestsMock, capsys: CaptureFixture):
    server = "https://www.example.com"
    responses.add(GET, server, body=ReadTimeout())

    client = requests.Session()
    status_code, result = get_request_with_retries(client, server, timeout=(5, 300), retries=0, delay_range=tuple())
    assert status_code is None
    assert result is None

    cap = capsys.readouterr()
    assert cap.err == "Read timeout occurred. Current timeout settings are (connect: 5s, read: 300s).\n"


def test_get_retry_connect_timeout_message(responses: RequestsMock, capsys: CaptureFixture):
    server = "https://www.example.com"
    responses.add(GET, server, body=ConnectTimeout())

    client = requests.Session()
    status_code, result = get_request_with_retries(client, server, timeout=(5, 300), retries=0, delay_range=tuple())
    assert status_code is None
    assert result is None

    cap = capsys.readouterr()
    assert cap.err == "Connect timeout occurred. Current timeout settings are (connect: 5s, read: 300s).\n"


def test_get_retry_io_error(responses: RequestsMock, capsys: CaptureFixture):
    server = "https://www.example.com"
    responses.add(GET, server, body=IOError("IO Error"))

    client = requests.Session()
    status_code, result = get_request_with_retries(client, server, timeout=(5, 300), retries=0, delay_range=tuple())
    assert status_code is None
    assert result is None

    cap = capsys.readouterr()
    assert cap.err == "ERROR: Failed to fetch data from https://www.example.com:\n    IO Error\n"


def test_get_retry_spinta_error(responses: RequestsMock, capsys: CaptureFixture):
    server = "https://www.example.com"
    responses.add(GET, server, body=json.dumps({"_errors": ["SpintaError"]}), status=400)

    client = requests.Session()
    status_code, result = get_request_with_retries(client, server, timeout=(5, 300), retries=0, delay_range=tuple())
    assert status_code == 400
    assert result == {"_errors": ["SpintaError"]}

    cap = capsys.readouterr()
    assert cap.err == (
        "ERROR (400): Failed to fetch data from https://www.example.com:\n    {'_errors': ['SpintaError']}\n"
    )
