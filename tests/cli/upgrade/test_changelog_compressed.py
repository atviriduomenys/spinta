from datetime import datetime

import pytest
import sqlalchemy as sa

from spinta.cli.helpers.script.components import ScriptStatus
from spinta.cli.helpers.script.helpers import script_check_status_message
from spinta.cli.helpers.upgrade.components import Script
from spinta.testing.cli import result_contains
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.tabular import create_tabular_manifest


@pytest.mark.parametrize("missing", [True, False])
def test_upgrade_changelog_compressed(context, tmp_path, rc, postgresql, request, cli, missing):
    manifest = tmp_path / "manifest.csv"
    create_tabular_manifest(
        context,
        manifest,
        """
        d | r | b | m | property | type   | ref | prepare | access
        datasets/compressed/cli |        |     |         |
          |   |   | Country     |        |     |         |
          |   |   |   | name    | string |     |         | open
          |   |   | City        |        |     |         |
          |   |   |   | name    | string |     |         | open
        """,
    )
    context = bootstrap_manifest(rc, manifest, backend=postgresql, tmp_path=tmp_path, request=request, full_load=True)
    backend = context.get("store").manifest.backend
    script = Script.POSTGRESQL_CHANGELOG_COMPRESSED.value

    # Bootstrap keeps existing tables, including tables left by a failed upgrade.
    with backend.begin() as conn:
        for model in ["Country", "City"]:
            conn.execute(
                f'ALTER TABLE "datasets/compressed/cli"."{model}/:changelog" '
                "ADD COLUMN IF NOT EXISTS _compressed TIMESTAMP"
            )
            conn.execute(
                f'COMMENT ON COLUMN "datasets/compressed/cli"."{model}/:changelog"._compressed IS \'_compressed\''
            )

    if missing:
        with backend.begin() as conn:
            conn.execute('ALTER TABLE "datasets/compressed/cli"."Country/:changelog" DROP COLUMN _compressed')

    table = sa.Table(
        "Country/:changelog", sa.MetaData(), schema="datasets/compressed/cli", autoload_with=backend.engine
    )
    existing_row = {"_id": 987654, "action": "patch", "data": {"name": "Lithuania"}}
    if not missing:
        existing_row["_compressed"] = datetime(2020, 1, 1)
    with backend.begin() as conn:
        conn.execute(table.delete())
        conn.execute(table.insert().values(existing_row))

    def columns(model):
        return {
            column["name"]: column
            for column in sa.inspect(backend.engine).get_columns(
                f"{model}/:changelog", schema="datasets/compressed/cli"
            )
        }

    result = cli.invoke(context.get("rc"), ["upgrade", script, "--check"])
    status = ScriptStatus.REQUIRED if missing else ScriptStatus.PASSED
    assert result_contains(result, script_check_status_message(script, status))
    assert ("_compressed" in columns("Country")) is not missing

    result = cli.invoke(context.get("rc"), ["upgrade", script])
    assert result.exit_code == 0
    for model in ["Country", "City"]:
        column = columns(model)["_compressed"]
        assert isinstance(column["type"], sa.DateTime)
        assert column["nullable"] is True
        assert column["default"] is None
        assert column["comment"] == "_compressed"

    result = cli.invoke(context.get("rc"), ["upgrade", script])
    assert result_contains(result, script_check_status_message(script, ScriptStatus.PASSED))
    assert result.exit_code == 0
    table = sa.Table(
        "Country/:changelog", sa.MetaData(), schema="datasets/compressed/cli", autoload_with=backend.engine
    )
    with backend.begin() as conn:
        row = conn.execute(sa.select(table)).one()
    assert row._id == 987654
    assert row.data == {"name": "Lithuania"}
    assert row._compressed == (None if missing else datetime(2020, 1, 1))
