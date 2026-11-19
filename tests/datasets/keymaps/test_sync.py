from urllib.parse import unquote

import pytest
import requests
from responses import GET, RequestsMock

from spinta.cli.helpers.errors import ErrorCounter
from spinta.components import Config, Model
from spinta.core.context import load_commands
from spinta.datasets.keymaps.sync import _fetch_changelog_data
from spinta.exceptions import ExceededMaximumLimit, error_response


@pytest.mark.parametrize("status", [200, 201])
def test_fetch_changelog_data_non_json_response(responses: RequestsMock, capsys: pytest.CaptureFixture, status: int):
    server = "https://example.com"
    model = Model()
    model.name = "example/City"
    config = Config()
    config.sync_page_size = 10
    responses.add(GET, f"{server}/{model.name}/:changes?limit(10)", body="INVALID JSON", status=status)
    error_counter = ErrorCounter(max_count=10)

    rows = list(
        _fetch_changelog_data(
            config,
            model,
            requests.Session(),
            server,
            offset_cid=0,
            error_counter=error_counter,
            timeout=(5, 300),
            retries=2,
            delay_range=(0,),
        )
    )

    assert rows == []
    assert len(responses.calls) == 3
    assert error_counter.count == 1
    assert "ERROR: Failed to fetch changelog data for model example/City." in capsys.readouterr().err


def test_fetch_changelog_data_successful_201(responses: RequestsMock):
    server = "https://example.com"
    model = Model()
    model.name = "example/City"
    config = Config()
    config.sync_page_size = 10
    responses.add(GET, f"{server}/{model.name}/:changes?limit(10)", json={"_data": [{"_cid": 1}]}, status=201)
    error_counter = ErrorCounter(max_count=10)

    rows = list(
        _fetch_changelog_data(
            config,
            model,
            requests.Session(),
            server,
            offset_cid=0,
            error_counter=error_counter,
            timeout=(5, 300),
            retries=0,
            delay_range=(0,),
        )
    )

    assert rows == [{"_cid": 1}]
    assert len(responses.calls) == 1
    assert error_counter.count == 0


@pytest.mark.parametrize("retries", [0, 2])
def test_fetch_changelog_data_server_limit(responses: RequestsMock, retries: int):
    load_commands(["spinta.types.model", "spinta.manifests.commands.error"])
    server = "https://example.com"
    model = Model()
    model.name = "example/City"
    model.eid = "example/City"
    config = Config()
    config.sync_page_size = 10
    limit_error = ExceededMaximumLimit(model, model_name=model.name, maximum_limit=2, given_limit=config.sync_page_size)
    responses.add(
        GET,
        f"{server}/{model.name}/:changes?limit(10)",
        json={"errors": [error_response(limit_error)]},
        status=limit_error.status_code,
    )
    responses.add(
        GET,
        f"{server}/{model.name}/:changes?limit(2)",
        json={"_data": [{"_cid": 1}, {"_cid": 2}]},
    )
    responses.add(GET, f"{server}/{model.name}/:changes/3?limit(2)", json={"_data": [{"_cid": 3}]})
    error_counter = ErrorCounter(max_count=10)
    rows = list(
        _fetch_changelog_data(
            config,
            model,
            requests.Session(),
            server,
            offset_cid=0,
            error_counter=error_counter,
            timeout=(5, 300),
            retries=retries,
            delay_range=(0,),
        )
    )
    assert rows == [{"_cid": 1}, {"_cid": 2}, {"_cid": 3}]
    assert [unquote(call.request.url) for call in responses.calls] == [
        f"{server}/{model.name}/:changes?limit(10)",
        f"{server}/{model.name}/:changes?limit(2)",
        f"{server}/{model.name}/:changes/3?limit(2)",
    ]
    assert error_counter.count == 0


@pytest.mark.parametrize("maximum", [None, 0, -1, 10, 20, True, "2"])
def test_fetch_changelog_data_invalid_server_limit(responses: RequestsMock, maximum):
    load_commands(["spinta.types.model", "spinta.manifests.commands.error"])
    server = "https://example.com"
    model = Model()
    model.name = "example/City"
    model.eid = "example/City"
    config = Config()
    config.sync_page_size = 10
    limit_error = ExceededMaximumLimit(
        model, model_name=model.name, maximum_limit=maximum, given_limit=config.sync_page_size
    )
    responses.add(
        GET,
        f"{server}/{model.name}/:changes?limit(10)",
        json={"errors": [error_response(limit_error)]},
        status=limit_error.status_code,
    )
    error_counter = ErrorCounter(max_count=10)
    rows = list(
        _fetch_changelog_data(
            config,
            model,
            requests.Session(),
            server,
            offset_cid=0,
            error_counter=error_counter,
            timeout=(5, 300),
            retries=0,
            delay_range=(0,),
        )
    )
    assert rows == []
    assert len(responses.calls) == 1
    assert error_counter.count == 1
