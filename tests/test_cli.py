import logging
import re
from pathlib import Path

import pytest
from _pytest.logging import LogCaptureFixture

from responses import GET

from spinta.core.config import RawConfig
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.utils import create_manifest_files
from spinta.testing.utils import update_manifest_files


def test_version(rc: RawConfig, cli: SpintaCliRunner):
    result = cli.invoke(rc, ["--version"])
    version = result.stdout.strip()
    version = re.sub(r"\d+", "x", version)
    version = version.replace(".devx", "")
    assert version in ("x.x.x", "x.x")


def test_empty_config_path(rc: RawConfig, cli: SpintaCliRunner):
    result = cli.invoke(rc, ["-o", "config_path=", "--version"])
    version = result.stdout.strip()
    version = re.sub(r"\d+", "x", version)
    version = version.replace(".devx", "")
    assert version in ("x.x.x", "x.x")


@pytest.mark.skip("datasets")
def test_pull(responses, rc, cli: SpintaCliRunner, app, tmp_path):
    responses.add(
        GET,
        "http://example.com/countries.csv",
        status=200,
        content_type="text/plain; charset=utf-8",
        body=("kodas,šalis\nlt,Lietuva\nlv,Latvija\nee,Estija"),
    )

    create_manifest_files(
        tmp_path,
        {
            "datasets/csv.yml": None,
            "datasets/csv/country.yml": None,
        },
    )
    update_manifest_files(
        tmp_path,
        {
            "datasets/csv.yml": [
                {"op": "add", "path": "/resources/countries/backend", "value": "csv"},
            ],
        },
    )
    rc = rc.fork(
        {
            "manifests.default": {
                "type": "yaml",
                "path": str(tmp_path),
            },
            "backends.csv": {
                "type": "csv",
            },
        }
    )

    result = cli.invoke(rc, ["pull", "datasets/csv"])
    assert result.output == (
        "\n"
        "\n"
        "Table: country/:dataset/csv/:resource/countries\n"
        "                  _id                      code    title     _op                         _where                    \n"
        "===================================================================================================================\n"
        '552c4c243ec8c98c313255ea9bf16ee286591f8c   lt     Lietuva   upsert   _id="552c4c243ec8c98c313255ea9bf16ee286591f8c"\n'
        'b5dcb86880816fb966cdfbbacd1f3406739464f4   lv     Latvija   upsert   _id="b5dcb86880816fb966cdfbbacd1f3406739464f4"\n'
        '68de1c04d49aeefabb7081a5baf81c055f235be3   ee     Estija    upsert   _id="68de1c04d49aeefabb7081a5baf81c055f235be3"'
    )

    app.authmodel("country/:dataset/csv/:resource/countries", ["getall"])

    assert app.get("/country/:dataset/csv/:resource/countries").json() == {
        "_data": [],
    }

    result = cli.invoke(rc, ["pull", "csv", "--push"])
    assert "csv:" in result.output

    rows = sorted(
        (row["code"], row["title"], row["_type"])
        for row in app.get("/country/:dataset/csv/:resource/countries").json()["_data"]
    )
    assert rows == [
        ("ee", "Estija", "country/:dataset/csv/:resource/countries"),
        ("lt", "Lietuva", "country/:dataset/csv/:resource/countries"),
        ("lv", "Latvija", "country/:dataset/csv/:resource/countries"),
    ]


def test_log_file(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    caplog: LogCaptureFixture,
):
    log_file = tmp_path / "spinta.log"
    with caplog.at_level(logging.DEBUG, "spinta.cli.main"):
        cli.invoke(
            rc,
            [
                "--log-file",
                log_file,
                "--log-level",
                "debug",
                "--version",
            ],
        )
    # XXX: Can't test if log_file exists, because pytest overrides logging
    #      handlers and I don't know how to tell pytest not to do that.
    assert f"log file set to: {log_file}" in caplog.text
    assert "log level set to: debug" in caplog.text
