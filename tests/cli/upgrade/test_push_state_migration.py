import pytest
import sqlalchemy as sa

from spinta import commands
from spinta.cli.helpers.push.components import PushState
from spinta.cli.helpers.push.state import init_push_state
from spinta.cli.helpers.script.components import ScriptStatus, ScriptTarget
from spinta.cli.helpers.script.helpers import script_check_status_message
from spinta.cli.helpers.upgrade.components import Script
from spinta.core.config import RawConfig
from spinta.exceptions import PushStateMigrationRequired
from spinta.testing.cli import SpintaCliRunner, result_contains
from spinta.testing.manifest import load_manifest_and_context


def _create_push_state_without_migrations(context, path) -> PushState:
    push_state = init_push_state(context, f"sqlite+spinta:///{path}", [])
    with push_state:
        migration_table = push_state.db.get_table(push_state.migrations.migration_table_name)
        push_state.db.conn.execute(migration_table.delete())
    return push_state


def _get_migrations(push_state: PushState) -> list[str]:
    with push_state:
        migration_table = push_state.db.get_table(push_state.migrations.migration_table_name)
        migrations = push_state.db.conn.execute(sa.select([migration_table.c.migration])).fetchall()
    return [migration for (migration,) in migrations]


def test_upgrade_push_state_without_path_does_not_apply_migrations(
    context, rc: RawConfig, cli: SpintaCliRunner, tmp_path
):
    push_state_path = str(tmp_path / "tmp_push_state.db")
    push_state_dsn = f"sqlite+spinta:///{push_state_path}"

    push_state: PushState = init_push_state(context, push_state_dsn, [])
    with push_state:
        migration_table = push_state.db.get_table(push_state.migrations.migration_table_name)
        push_state.db.conn.execute(migration_table.delete())

    result = cli.invoke(rc, ["upgrade", "--target", ScriptTarget.PUSH_STATE_DB.value])

    assert result.exit_code == 0
    with push_state:
        migration_table = push_state.db.get_table(push_state.migrations.migration_table_name)
        migrations = push_state.db.conn.execute(sa.select([migration_table.c.migration])).fetchall()
    assert migrations == []


def test_upgrade_initial_migration_for_multiple_targeted_push_states(
    context,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path,
):
    push_states = [
        _create_push_state_without_migrations(context, tmp_path / f"push_state_{index}.db") for index in range(3)
    ]

    result = cli.invoke(
        rc,
        [
            "upgrade",
            Script.PUSH_STATE_INITIAL.value,
            "--target",
            f"{ScriptTarget.PUSH_STATE_DB.value}={tmp_path / 'push_state_0.db'}",
            "--target",
            f"{ScriptTarget.PUSH_STATE_DB.value}={tmp_path / 'push_state_1.db'}",
        ],
    )

    assert result.exit_code == 0
    assert [_get_migrations(push_state) for push_state in push_states] == [
        [Script.PUSH_STATE_INITIAL.value],
        [Script.PUSH_STATE_INITIAL.value],
        [],
    ]


def test_upgrade_initial_migration_discovers_all_default_push_states(
    context,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path,
):
    data_path = tmp_path / "data"
    push_state_dir = data_path / "push"
    push_state_dir.mkdir(parents=True)
    localrc = rc.fork({"data_path": data_path})
    push_states = [
        _create_push_state_without_migrations(context, push_state_dir / f"push_state_{index}.db") for index in range(3)
    ]

    result = cli.invoke(
        localrc,
        [
            "upgrade",
            Script.PUSH_STATE_INITIAL.value,
            "--target",
            ScriptTarget.PUSH_STATE_DB.value,
        ],
    )

    assert result.exit_code == 0
    assert [_get_migrations(push_state) for push_state in push_states] == [
        [Script.PUSH_STATE_INITIAL.value],
        [Script.PUSH_STATE_INITIAL.value],
        [Script.PUSH_STATE_INITIAL.value],
    ]


def test_upgrade_missing_initial_migration(context, rc: RawConfig, cli: SpintaCliRunner, responses, tmp_path):
    push_state_path = str(tmp_path / "tmp_push_state.db")
    push_state_dsn = f"sqlite+spinta:///{push_state_path}"

    push_state: PushState = init_push_state(context, push_state_dsn, [])
    with push_state:
        migration_table = push_state.db.get_table(push_state.migrations.migration_table_name)
        push_state.db.conn.execute(migration_table.delete())

    with pytest.raises(PushStateMigrationRequired):
        init_push_state(context, push_state_dsn, [])

    result = cli.invoke(
        rc,
        ["upgrade", "--target", f"{ScriptTarget.PUSH_STATE_DB.value}={push_state_path}"],
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
        migration_table = push_state.db.get_table(push_state.migrations.migration_table_name)
        push_state.db.conn.execute(migration_table.delete(migration_table.c.migration == Script.PUSH_REV_RENAME.value))
        push_state.db.get_table(model.name, model=model)
        push_state.db.conn.execute(sa.text('ALTER TABLE "example/Continent" RENAME COLUMN checksum TO rev'))

    with pytest.raises(PushStateMigrationRequired):
        init_push_state(context, push_state_dsn, [])

    result = cli.invoke(
        rc,
        ["upgrade", "--target", f"{ScriptTarget.PUSH_STATE_DB.value}={push_state_path}"],
    )
    assert result.exit_code == 0
    assert result_contains(result, script_check_status_message(Script.PUSH_REV_RENAME.value, ScriptStatus.REQUIRED))

    assert isinstance(init_push_state(context, push_state_dsn, []), PushState)

    push_state.db.metadata.reflect()
    columns = [column.name for column in push_state.db.get_table(model.name, model=model).columns]
    assert "rev" not in columns
    assert "checksum" in columns
