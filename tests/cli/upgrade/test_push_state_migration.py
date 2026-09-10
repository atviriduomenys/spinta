import pytest
import sqlalchemy as sa

from spinta import commands
from spinta.cli.helpers.push.components import PUSH_STATE_PATH, PushState
from spinta.cli.helpers.push.state import init_push_state
from spinta.cli.helpers.script.components import ScriptStatus, ScriptTarget
from spinta.cli.helpers.script.helpers import script_check_status_message
from spinta.cli.helpers.upgrade.components import Script
from spinta.core.config import RawConfig
from spinta.exceptions import PushStateMigrationRequired
from spinta.testing.cli import SpintaCliRunner, result_contains
from spinta.testing.manifest import load_manifest_and_context


def test_upgrade_push_state_without_path_does_not_apply_migrations(
    context, rc: RawConfig, cli: SpintaCliRunner, tmp_path
):
    push_state_path = str(tmp_path / "tmp_push_state.db")
    push_state_dsn = f"sqlite+spinta:///{push_state_path}"

    push_state: PushState = init_push_state(context, push_state_dsn, [])
    with push_state:
        migration_table = push_state.get_table(push_state.migration_table_name)
        push_state.conn.execute(migration_table.delete())

    result = cli.invoke(rc, ["upgrade", "--target", ScriptTarget.PUSH_STATE_DB.value])

    assert result.exit_code == 0
    with push_state:
        migration_table = push_state.get_table(push_state.migration_table_name)
        migrations = push_state.conn.execute(sa.select([migration_table.c.migration])).fetchall()
    assert migrations == []


def test_upgrade_missing_initial_migration(context, rc: RawConfig, cli: SpintaCliRunner, responses, tmp_path):
    push_state_path = str(tmp_path / "tmp_push_state.db")
    push_state_dsn = f"sqlite+spinta:///{push_state_path}"

    push_state: PushState = init_push_state(context, push_state_dsn, [])
    with push_state:
        migration_table = push_state.get_table(push_state.migration_table_name)
        push_state.conn.execute(migration_table.delete())

    with pytest.raises(PushStateMigrationRequired):
        init_push_state(context, push_state_dsn, [])

    result = cli.invoke(
        rc.fork({PUSH_STATE_PATH: push_state_path}),
        ["upgrade", "--target", ScriptTarget.PUSH_STATE_DB.value],
    )
    assert result.exit_code == 0
    assert result_contains(result, script_check_status_message(Script.PUSH_STATE_INITIAL.value, ScriptStatus.REQUIRED))

    assert isinstance(init_push_state(context, push_state_dsn, []), PushState)


def test_upgrade_missing_rev_rename_migration(
    context,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property   | type    | ref       | level | access
    example                    |         |           |       |
      |   |   | Continent      |         |           | 4     |
      |   |   |   | id         | integer |           | 3     | open
      |   |   |   | name       | string  |           | 3     | open
    """,
    )
    model = commands.get_model(context, manifest, "example/Continent")

    push_state_path = str(tmp_path / "tmp_push_state.db")
    push_state_dsn = f"sqlite+spinta:///{push_state_path}"

    push_state: PushState = init_push_state(context, push_state_dsn, [])
    with push_state:
        migration_table = push_state.get_table(push_state.migration_table_name)
        push_state.conn.execute(migration_table.delete(migration_table.c.migration == Script.PUSH_REV_RENAME.value))
        push_state.get_table(model.name, model=model)
        push_state.conn.execute(sa.text('ALTER TABLE "example/Continent" RENAME COLUMN checksum TO rev'))

    with pytest.raises(PushStateMigrationRequired):
        init_push_state(context, push_state_dsn, [])

    result = cli.invoke(
        rc.fork({PUSH_STATE_PATH: push_state_path}),
        ["upgrade", "--target", ScriptTarget.PUSH_STATE_DB.value],
    )
    assert result.exit_code == 0
    assert result_contains(result, script_check_status_message(Script.PUSH_REV_RENAME.value, ScriptStatus.REQUIRED))

    assert isinstance(init_push_state(context, push_state_dsn, []), PushState)

    push_state.metadata.reflect()
    columns = [column.name for column in push_state.get_table(model.name, model=model).columns]
    assert "rev" not in columns
    assert "checksum" in columns
