import pytest
import requests
from responses import GET, RequestsMock

from spinta.cli.helpers.errors import ErrorCounter
from spinta.components import Config, Model
from spinta.datasets.keymaps.sync import _fetch_changelog_data


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
