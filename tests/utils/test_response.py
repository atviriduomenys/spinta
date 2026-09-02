import json

import pytest
import requests
from _pytest.capture import CaptureFixture
from requests import ConnectTimeout, ReadTimeout, Timeout
from responses import GET, RequestsMock

from spinta.utils.response import RequestResult, get_request_with_retries, request


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
