from copy import deepcopy
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path
from typing import Any, Iterator
from uuid import UUID

import pytest
import sqlalchemy as sa
from _pytest.fixtures import FixtureRequest
from ruamel.yaml import YAML
from sqlalchemy.engine import Connection
from tqdm import tqdm

from spinta import commands
from spinta.backends.constants import TableType
from spinta.backends.postgresql.components import PostgreSQL
from spinta.cli.helpers.admin.components import Script
from spinta.cli.helpers.admin.scripts import compress_changelogs as compression
from spinta.cli.helpers.script.components import ScriptStatus
from spinta.cli.helpers.script.helpers import script_check_status_message
from spinta.cli.helpers.upgrade.components import Script as UpgradeScript
from spinta.components import Context, Model
from spinta.core.config import RawConfig
from spinta.testing.cli import SpintaCliRunner, result_contains
from spinta.testing.client import create_test_client
from spinta.testing.data import send
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.tabular import create_tabular_manifest

CREATED_AT = datetime(2020, 1, 1)
COMPRESSED_AT = datetime(2020, 2, 1)

ChangelogFixture = tuple[Context, PostgreSQL, Model, sa.Table]


@pytest.fixture
def changelog(
    context: Context, rc: RawConfig, postgresql: str, tmp_path: Path, request: FixtureRequest
) -> Iterator[ChangelogFixture]:
    manifest_path = tmp_path / "manifest.csv"
    create_tabular_manifest(
        context,
        manifest_path,
        """
        d | r | b | m | property | type   | access
        datasets/compression/tests |    |
          |   |   | Country     |        |
          |   |   |   | name    | string | open
          |   |   | City        |        |
          |   |   |   | name    | string | open
        datasets/compression/disabled | |
          |   |   | Country     |        |
          |   |   |   | name    | string | open
        """,
    )
    context = bootstrap_manifest(
        rc.fork(
            {
                "default_retention_periodicity": "1d",
                "datasets": {"datasets/compression/disabled": {"enable_retention_policy": False}},
            }
        ),
        manifest_path,
        backend=postgresql,
        tmp_path=tmp_path,
        request=request,
        full_load=True,
    )
    store = context.get("store")
    model = commands.get_model(context, store.manifest, "datasets/compression/tests/Country")
    backend = model.backend
    table = backend.get_table(model, TableType.CHANGELOG)
    with backend.begin() as conn:
        conn.execute(table.delete())
    yield context, backend, model, table


def changelog_row(
    cid: int, action: str, data: dict[str, Any] | None = None, *, rid: int = 1, compressed: datetime | None = None
) -> dict[str, Any]:
    return {
        "_id": cid,
        "_rid": str(UUID(int=rid)),
        "_revision": str(UUID(int=cid + 100)),
        "_txn": str(UUID(int=cid + 1000)),
        "datetime": CREATED_AT + timedelta(minutes=cid),
        "action": action,
        "data": {} if data is None else data,
        "_compressed": compressed,
    }


def read_changelog(backend: PostgreSQL, table: sa.Table) -> list[dict[str, Any]]:
    with backend.begin() as conn:
        return [dict(row._mapping) for row in conn.execute(sa.select(table).order_by(table.c._id))]


def compressed_row(row: dict[str, Any], data: dict[str, Any] | None = None) -> dict[str, Any]:
    return {**row, "data": row["data"] if data is None else data, "_compressed": COMPRESSED_AT}


@pytest.mark.parametrize(
    "manifest_options, overrides, expected_period, enabled",
    [
        ({}, {}, timedelta(days=10), True),
        ({"enable_retention_policy": False}, {}, timedelta(days=10), False),
        ({}, {"enable_retention_policy": "false"}, timedelta(days=10), False),
        ({"enable_retention_policy": False}, {"enable_retention_policy": "true"}, timedelta(days=10), True),
        ({"accrual_periodicity": "1d"}, {}, timedelta(days=30), True),
        ({"accrual_periodicity": "20d"}, {}, timedelta(days=60), True),
        ({"accrual_periodicity": "1d"}, {"accrual_periodicity": "20d"}, timedelta(days=60), True),
        ({"accrual_periodicity": "20d"}, {"accrual_periodicity": None}, timedelta(days=10), True),
    ],
)
def test_admin_compress_changelogs_respects_dataset_retention_settings(
    rc: RawConfig,
    cli: SpintaCliRunner,
    postgresql: str,
    tmp_path: Path,
    request: FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
    manifest_options: dict[str, Any],
    overrides: dict[str, Any],
    expected_period: timedelta,
    enabled: bool,
) -> None:
    dataset_name = "datasets/compression/policy"
    manifest_path = tmp_path / "manifest"
    manifest_path.mkdir()
    schemas = {
        "dataset.yml": {"type": "dataset", "name": dataset_name, **manifest_options},
        "item.yml": {
            "type": "model",
            "name": f"{dataset_name}/Item",
            "access": "open",
            "external": {"dataset": dataset_name},
            "properties": {"name": {"type": "string"}},
        },
    }
    yaml = YAML(typ="safe")
    for filename, schema in schemas.items():
        with (manifest_path / filename).open("w", encoding="utf-8") as file:
            yaml.dump(schema, file)
    context = bootstrap_manifest(
        rc.fork(
            {
                "default_retention_periodicity": "10d",
                "datasets": {dataset_name: overrides},
                "manifests.default": {"type": "yaml", "path": manifest_path, "backend": "default", "keymap": "default"},
            }
        ),
        None,
        backend=postgresql,
        tmp_path=tmp_path,
        request=request,
        full_load=True,
    )
    model = commands.get_model(context, context.get("store").manifest, f"{dataset_name}/Item")
    assert model.external.dataset.retention_periodicity == expected_period
    assert model.external.dataset.enable_retention_policy is enabled

    now = datetime(2020, 3, 1, 0, 5)

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> datetime:
            if tz is None:
                return now + timedelta(hours=2)
            return now.replace(tzinfo=timezone.utc).astimezone(tz)

    monkeypatch.setattr(compression, "datetime", FixedDatetime)
    cutoff = now - expected_period
    rows = [
        changelog_row(1, "insert"),
        changelog_row(2, "patch", {"old": 1}),
        changelog_row(3, "delete"),
        changelog_row(4, "upsert"),
        changelog_row(5, "patch", {"a": 1}),
        changelog_row(6, "patch", {"b": 2}),
    ]
    for row in rows:
        row["datetime"] = cutoff + timedelta(minutes=row["_id"] - 5)
    backend = model.backend
    table = backend.get_table(model, TableType.CHANGELOG)
    with backend.begin() as conn:
        conn.execute(table.insert(), rows)

    result = cli.invoke(context.get("rc"), ["admin", Script.COMPRESS_CHANGELOGS.value])
    status = ScriptStatus.REQUIRED if enabled else ScriptStatus.PASSED
    assert result_contains(result, script_check_status_message(Script.COMPRESS_CHANGELOGS.value, status))
    expected = [rows[3], {**rows[4], "_compressed": now}, rows[5]] if enabled else rows
    assert read_changelog(backend, table) == expected


@pytest.mark.parametrize("batch_size", [0, -1, "invalid"])
def test_admin_compress_changelogs_rejects_invalid_batch_size(
    rc: RawConfig, cli: SpintaCliRunner, batch_size: int | str
) -> None:
    rc = rc.fork({"changelog_compression_batch_size": batch_size})
    result = cli.invoke(rc, ["admin", Script.COMPRESS_CHANGELOGS.value], fail=False)
    assert result.exit_code == 1
    assert isinstance(result.exception, ValueError)


def test_admin_compress_changelogs_requires_changelog_upgrade(
    changelog: ChangelogFixture, cli: SpintaCliRunner
) -> None:
    context, backend, _, table = changelog
    rows = [
        changelog_row(1, "insert"),
        changelog_row(2, "patch", {"a": 1}),
        changelog_row(3, "patch", {"b": 2}),
    ]
    with backend.begin() as conn:
        conn.execute(table.insert(), rows)
        conn.execute('ALTER TABLE "datasets/compression/tests"."Country/:changelog" DROP COLUMN _compressed')

    rc = context.get("rc")
    result = cli.invoke(rc, ["admin", Script.COMPRESS_CHANGELOGS.value])
    assert result_contains(result, script_check_status_message(Script.COMPRESS_CHANGELOGS.value, ScriptStatus.SKIPPED))
    legacy_table = sa.Table(table.name, sa.MetaData(), schema=table.schema, autoload_with=backend.engine)
    assert "_compressed" not in legacy_table.c
    assert read_changelog(backend, legacy_table) == [
        {key: value for key, value in row.items() if key != "_compressed"} for row in rows
    ]

    result = cli.invoke(rc, ["upgrade", UpgradeScript.POSTGRESQL_CHANGELOG_COMPRESSED.value])
    assert result_contains(
        result, script_check_status_message(UpgradeScript.POSTGRESQL_CHANGELOG_COMPRESSED.value, ScriptStatus.REQUIRED)
    )
    assert "_compressed" in sa.Table(table.name, sa.MetaData(), schema=table.schema, autoload_with=backend.engine).c

    started_at = datetime.now(timezone.utc).replace(tzinfo=None)
    result = cli.invoke(rc, ["admin", Script.COMPRESS_CHANGELOGS.value])
    finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
    assert result_contains(result, script_check_status_message(Script.COMPRESS_CHANGELOGS.value, ScriptStatus.REQUIRED))
    actual = read_changelog(backend, table)
    compressed_at = actual[1]["_compressed"]
    assert started_at <= compressed_at <= finished_at
    assert actual == [rows[0], {**rows[2], "data": {"a": 1, "b": 2}, "_compressed": compressed_at}]


@pytest.mark.parametrize("action", ["patch", "update", "upsert"])
def test_compress_single_update_only_marks_processing(changelog: ChangelogFixture, action: str) -> None:
    context, backend, _, table = changelog
    rows = [changelog_row(1, "insert", {"anchor": True}), changelog_row(2, action, {"name": "LT"})]
    with backend.begin() as conn:
        conn.execute(table.insert(), rows)
    compression.compress_changelogs(context, cutoff=COMPRESSED_AT, compressed_at=COMPRESSED_AT)
    assert read_changelog(backend, table) == [rows[0], compressed_row(rows[1])]


def test_compress_patches_preserves_target_metadata(changelog: ChangelogFixture) -> None:
    context, backend, _, table = changelog
    rows = [
        changelog_row(1, "insert", {"anchor": "untouched"}),
        changelog_row(2, "patch", {"a": 1, "shared": "old"}),
        changelog_row(3, "patch", {"b": 2, "shared": "new"}),
        changelog_row(4, "delete"),
    ]
    with backend.begin() as conn:
        conn.execute(table.insert(), rows)
    compression.compress_changelogs(context, cutoff=COMPRESSED_AT, compressed_at=COMPRESSED_AT)
    assert read_changelog(backend, table) == [
        rows[0],
        compressed_row(rows[2], {"a": 1, "b": 2, "shared": "new"}),
        rows[3],
    ]


@pytest.mark.parametrize(
    "cutoff_minute, expected_target, expected_data",
    [
        (0, None, None),
        (1, None, None),
        (2, 2, {"a": 1}),
        (3, 3, {"a": 1, "b": 2}),
        (4, 4, {"a": 1, "b": 2, "c": 3}),
        (10, 4, {"a": 1, "b": 2, "c": 3}),
    ],
)
def test_compress_respects_cutoff_including_exact_boundary(
    changelog: ChangelogFixture, cutoff_minute: int, expected_target: int | None, expected_data: dict[str, Any] | None
) -> None:
    context, backend, _, table = changelog
    rows = [
        changelog_row(1, "insert"),
        changelog_row(2, "patch", {"a": 1}),
        changelog_row(3, "patch", {"b": 2}),
        changelog_row(4, "patch", {"c": 3}),
    ]
    with backend.begin() as conn:
        conn.execute(table.insert(), rows)
    compression.compress_changelogs(
        context, cutoff=CREATED_AT + timedelta(minutes=cutoff_minute), compressed_at=COMPRESSED_AT
    )
    if expected_target is None:
        expected = rows
    else:
        expected = [rows[0], compressed_row(rows[expected_target - 1], expected_data), *rows[expected_target:]]
    assert read_changelog(backend, table) == expected


def test_compress_already_processed_target_leaves_history_unchanged(changelog: ChangelogFixture) -> None:
    context, backend, _, table = changelog
    rows = [
        changelog_row(1, "insert"),
        changelog_row(3, "patch", {"a": 1, "b": 2}, compressed=datetime(2020, 1, 2)),
        changelog_row(4, "patch", {"c": 3}),
    ]
    with backend.begin() as conn:
        conn.execute(table.insert(), rows)
    compression.compress_changelogs(context, cutoff=rows[1]["datetime"], compressed_at=COMPRESSED_AT)
    assert read_changelog(backend, table) == rows


@pytest.mark.parametrize("target_action", ["patch", "upsert", "update"])
def test_compress_incrementally_includes_previously_compressed_base(
    changelog: ChangelogFixture, target_action: str
) -> None:
    context, backend, _, table = changelog
    rows = [
        changelog_row(1, "insert"),
        changelog_row(2, "patch", {"a": 1}),
        changelog_row(3, "patch", {"b": 2}),
        changelog_row(4, "patch", {"c": 3}),
        changelog_row(5, target_action, {"d": 4}),
    ]
    with backend.begin() as conn:
        conn.execute(table.insert(), rows)
    compression.compress_changelogs(context, cutoff=rows[2]["datetime"], compressed_at=datetime(2020, 1, 2))
    assert read_changelog(backend, table) == [
        rows[0],
        {**rows[2], "data": {"a": 1, "b": 2}, "_compressed": datetime(2020, 1, 2)},
        rows[3],
        rows[4],
    ]
    compression.compress_changelogs(context, cutoff=COMPRESSED_AT, compressed_at=COMPRESSED_AT)
    expected = [rows[0], compressed_row(rows[4], {"a": 1, "b": 2, "c": 3, "d": 4})]
    assert read_changelog(backend, table) == expected
    compression.compress_changelogs(context, cutoff=COMPRESSED_AT, compressed_at=datetime(2020, 3, 1))
    assert read_changelog(backend, table) == expected


@pytest.mark.parametrize("target_action", ["patch", "upsert", "update"])
def test_compress_update_preserves_accumulated_payload(changelog: ChangelogFixture, target_action: str) -> None:
    context, backend, _, table = changelog
    rows = [
        changelog_row(1, "insert"),
        changelog_row(2, "patch", {"a": 1, "x": 1}),
        changelog_row(3, "update", {"b": 2}),
        changelog_row(4, target_action, {"c": 3}),
    ]
    with backend.begin() as conn:
        conn.execute(table.insert(), rows)
    compression.compress_changelogs(context, cutoff=COMPRESSED_AT, compressed_at=COMPRESSED_AT)
    expected_data = {"a": 1, "x": 1, "b": 2, "c": 3}
    assert read_changelog(backend, table) == [rows[0], compressed_row(rows[3], expected_data)]


@pytest.mark.parametrize(
    "update_data, expected_change, expected_data",
    [
        ({"a": 1, "b": 2}, {"b": 2}, {"a": 1, "b": 2}),
        ({"b": 2}, {"a": None, "b": 2}, {"a": None, "b": 2}),
        ({"a": None, "b": 2}, {"a": None, "b": 2}, {"a": None, "b": 2}),
    ],
)
def test_compress_update_deltas_from_api(
    rc: RawConfig,
    postgresql: str,
    tmp_path: Path,
    request: FixtureRequest,
    update_data: dict[str, int | None],
    expected_change: dict[str, int | None],
    expected_data: dict[str, int | None],
) -> None:
    context = bootstrap_manifest(
        rc.fork({"default_retention_periodicity": "1d"}),
        """
        d | r | b | m | property | type    | access
        datasets/compression/update |     |
          |   |   | Item        |         | open
          |   |   |   | a       | integer |
          |   |   |   | b       | integer |
        """,
        backend=postgresql,
        tmp_path=tmp_path,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    model_name = "datasets/compression/update/Item"
    app.authmodel(model_name, ["insert", "patch", "update"])
    obj = send(app, model_name, "insert", {"a": 0, "b": 0})
    obj = send(app, model_name, "patch", obj, {"a": 1})
    response = app.put(f"/{model_name}/{obj.id}", json={"_revision": obj.rev, **update_data})
    assert response.status_code == 200, response.json()

    model = commands.get_model(context, context.get("store").manifest, model_name)
    backend = model.backend
    table = backend.get_table(model, TableType.CHANGELOG)
    rows = read_changelog(backend, table)
    assert [(row["action"], row["data"]) for row in rows] == [
        ("insert", {"a": 0, "b": 0}),
        ("patch", {"a": 1}),
        ("update", expected_change),
    ]

    compressed_at = datetime(2035, 1, 1)
    compression.compress_changelogs(context, cutoff=compressed_at, compressed_at=compressed_at)
    assert read_changelog(backend, table) == [
        rows[0],
        {**rows[2], "data": expected_data, "_compressed": compressed_at},
    ]


@pytest.mark.parametrize("target_action", ["patch", "upsert", "update"])
def test_compress_nested_payloads_merge_recursively(changelog: ChangelogFixture, target_action: str) -> None:
    context, backend, _, table = changelog
    rows = [
        changelog_row(1, "insert"),
        changelog_row(2, "patch", {"nested": {"a": 1, "child": {"b": 2, "shared": "old"}}, "keep": True}),
        changelog_row(3, target_action, {"nested": {"c": 3, "child": {"d": 4, "shared": "new"}}}),
    ]
    with backend.begin() as conn:
        conn.execute(table.insert(), rows)
    compression.compress_changelogs(context, cutoff=COMPRESSED_AT, compressed_at=COMPRESSED_AT)
    assert read_changelog(backend, table) == [
        rows[0],
        compressed_row(rows[2], {"nested": {"a": 1, "c": 3, "child": {"b": 2, "d": 4, "shared": "new"}}, "keep": True}),
    ]


@pytest.mark.parametrize("target_action", ["patch", "upsert", "update"])
@pytest.mark.parametrize(
    "previous, replacement, expected",
    [
        ({"a": 1}, {}, {"a": 1}),
        ({"a": 1}, None, None),
        (None, {"b": 2}, {"b": 2}),
        ({"a": 1}, "new", "new"),
        ("old", {"b": 2}, {"b": 2}),
        ({"a": 1}, False, False),
        ({"a": 1}, [], []),
        ([{"a": 1}], [{"b": 2}], [{"b": 2}]),
    ],
)
def test_compress_nested_value_replacements(
    changelog: ChangelogFixture, target_action: str, previous: Any, replacement: Any, expected: Any
) -> None:
    context, backend, _, table = changelog
    rows = [
        changelog_row(1, "insert"),
        changelog_row(2, "patch", {"nested": {"value": previous}, "keep": True}),
        changelog_row(3, target_action, {"nested": {"value": replacement}}),
    ]
    with backend.begin() as conn:
        conn.execute(table.insert(), rows)
    compression.compress_changelogs(context, cutoff=COMPRESSED_AT, compressed_at=COMPRESSED_AT)
    assert read_changelog(backend, table) == [
        rows[0],
        compressed_row(rows[2], {"nested": {"value": expected}, "keep": True}),
    ]


@pytest.mark.parametrize("target_action", ["patch", "upsert", "update"])
def test_compress_update_applies_explicit_nested_resets(changelog: ChangelogFixture, target_action: str) -> None:
    context, backend, _, table = changelog
    rows = [
        changelog_row(1, "insert"),
        changelog_row(2, "patch", {"nested": {"discard": 1}, "discard": True}),
        changelog_row(3, "update", {"nested": {"discard": None, "keep": 2}, "discard": None}),
        changelog_row(4, target_action, {"nested": {"new": 3}}),
    ]
    with backend.begin() as conn:
        conn.execute(table.insert(), rows)
    compression.compress_changelogs(context, cutoff=COMPRESSED_AT, compressed_at=COMPRESSED_AT)
    expected_data = {"nested": {"discard": None, "keep": 2, "new": 3}, "discard": None}
    assert read_changelog(backend, table) == [rows[0], compressed_row(rows[3], expected_data)]


@pytest.mark.parametrize("target_action", ["patch", "upsert", "update"])
def test_compress_incremental_nested_patches_preserve_previously_compressed_keys(
    changelog: ChangelogFixture, target_action: str
) -> None:
    context, backend, _, table = changelog
    rows = [
        changelog_row(1, "insert"),
        changelog_row(2, "patch", {"nested": {"a": 1}}),
        changelog_row(3, "patch", {"nested": {"b": 2}}),
        changelog_row(4, target_action, {"nested": {"c": 3}}),
    ]
    with backend.begin() as conn:
        conn.execute(table.insert(), rows)
    compression.compress_changelogs(context, cutoff=rows[2]["datetime"], compressed_at=COMPRESSED_AT)
    assert read_changelog(backend, table) == [rows[0], compressed_row(rows[2], {"nested": {"a": 1, "b": 2}}), rows[3]]
    compression.compress_changelogs(context, cutoff=COMPRESSED_AT, compressed_at=COMPRESSED_AT)
    assert read_changelog(backend, table) == [rows[0], compressed_row(rows[3], {"nested": {"a": 1, "b": 2, "c": 3}})]


@pytest.mark.parametrize("first_action", ["patch", "update"])
def test_merge_nested_compression_payloads_does_not_mutate_source_rows(first_action: str) -> None:
    rows = [
        compression.CompressionRow(1, first_action, {"nested": {"a": 1}}, False),
        compression.CompressionRow(2, "update", {"nested": {"b": 2}}, False),
        compression.CompressionRow(3, "upsert", {"nested": {"c": 3}}, False),
    ]
    original = deepcopy(rows)
    assert compression.merge_compression_rows(rows) == {"nested": {"a": 1, "b": 2, "c": 3}}
    assert rows == original


@pytest.mark.parametrize("action", ["insert", "delete", "move"])
def test_merge_compression_rows_rejects_non_update_actions(action: str) -> None:
    with pytest.raises(ValueError, match="Unexpected compression operation"):
        compression.merge_compression_rows([compression.CompressionRow(1, action, {}, False)])


def test_merge_compression_rows_rejects_empty_history() -> None:
    with pytest.raises(ValueError, match="Cannot compress an empty group"):
        compression.merge_compression_rows([])


def test_compress_first_upsert_is_an_anchor(changelog: ChangelogFixture) -> None:
    context, backend, _, table = changelog
    rows = [
        changelog_row(1, "upsert", {"anchor": True}),
        changelog_row(2, "upsert", {"a": 1}),
        changelog_row(3, "patch", {"b": 2}),
    ]
    with backend.begin() as conn:
        conn.execute(table.insert(), rows)
    compression.compress_changelogs(context, cutoff=COMPRESSED_AT, compressed_at=COMPRESSED_AT)
    assert read_changelog(backend, table) == [rows[0], compressed_row(rows[2], {"a": 1, "b": 2})]


@pytest.mark.parametrize(
    "terminal, next_anchor",
    [("delete", "upsert"), ("move", "upsert"), ("delete", "insert"), (None, "insert")],
)
@pytest.mark.parametrize("cutoff_minute", [2, 3, 20])
def test_compress_never_merges_across_lifecycle_boundaries(
    changelog: ChangelogFixture, terminal: str | None, next_anchor: str, cutoff_minute: int
) -> None:
    context, backend, _, table = changelog
    rows = [
        changelog_row(1, "insert"),
        changelog_row(2, "patch", {"old": 1}),
        changelog_row(3, "patch", {"old_extra": 2}),
    ]
    if terminal:
        rows.append(changelog_row(4, terminal))
    rows.extend(
        [
            changelog_row(5, next_anchor, {"anchor": True}),
            changelog_row(6, "patch", {"new": 3}),
            changelog_row(7, "upsert", {"new_extra": 4}),
        ]
    )
    with backend.begin() as conn:
        conn.execute(table.insert(), rows)
    compression.compress_changelogs(
        context, cutoff=CREATED_AT + timedelta(minutes=cutoff_minute), compressed_at=COMPRESSED_AT
    )
    if cutoff_minute == 2:
        expected = [rows[0], compressed_row(rows[1]), *rows[2:]]
    elif cutoff_minute == 3 and terminal:
        expected = [rows[0], compressed_row(rows[2], {"old": 1, "old_extra": 2}), *rows[3:]]
    elif cutoff_minute == 3:
        expected = rows[-3:]
    else:
        expected = [rows[-3], compressed_row(rows[-1], {"new": 3, "new_extra": 4})]
    assert read_changelog(backend, table) == expected


def test_retention_consecutive_terminals_do_not_create_a_lifecycle(changelog: ChangelogFixture) -> None:
    context, backend, _, table = changelog
    rows = [
        changelog_row(1, "insert"),
        changelog_row(2, "patch", {"a": 1}),
        changelog_row(3, "delete"),
        changelog_row(4, "move"),
    ]
    with backend.begin() as conn:
        conn.execute(table.insert(), rows)
    compression.compress_changelogs(context, cutoff=COMPRESSED_AT, compressed_at=COMPRESSED_AT)
    assert read_changelog(backend, table) == [rows[0], compressed_row(rows[1]), *rows[2:]]
    new_anchor = changelog_row(5, "upsert", {"new": True})
    with backend.begin() as conn:
        conn.execute(table.insert(), new_anchor)
    compression.compress_changelogs(context, cutoff=COMPRESSED_AT, compressed_at=COMPRESSED_AT)
    assert read_changelog(backend, table) == [new_anchor]


def test_retention_and_compression_ignore_unanchored_history(changelog: ChangelogFixture) -> None:
    context, backend, _, table = changelog
    rows = [
        changelog_row(1, "patch", {"malformed": 1}),
        changelog_row(2, "update", {"malformed": 2}),
        changelog_row(3, "insert"),
        changelog_row(4, "delete"),
        changelog_row(5, "upsert"),
        changelog_row(6, "patch", {"a": 1}),
        changelog_row(7, "patch", {"b": 2}),
    ]
    with backend.begin() as conn:
        conn.execute(table.insert(), rows)
    compression.compress_changelogs(context, cutoff=COMPRESSED_AT, compressed_at=COMPRESSED_AT)
    assert read_changelog(backend, table) == [rows[0], rows[1], rows[4], compressed_row(rows[6], {"a": 1, "b": 2})]


@pytest.mark.parametrize("terminal", [None, "delete", "move"])
def test_retention_keeps_latest_lifecycle_regardless_of_age(changelog: ChangelogFixture, terminal: str | None) -> None:
    context, backend, _, table = changelog
    rows = [changelog_row(1, "insert"), changelog_row(2, "patch", {"a": 1})]
    if terminal:
        rows.append(changelog_row(3, terminal))
    with backend.begin() as conn:
        conn.execute(table.insert(), rows)
    compression.compress_changelogs(context, cutoff=COMPRESSED_AT, compressed_at=COMPRESSED_AT)
    assert read_changelog(backend, table) == [rows[0], compressed_row(rows[1]), *rows[2:]]


@pytest.mark.parametrize(
    "cutoff_minute, first_retained_cid, processed_cids",
    [
        (0, 1, []),
        (2, 1, [2]),
        (3, 4, []),
        (5, 4, [5]),
        (6, 7, []),
        (20, 7, [8]),
    ],
)
def test_retention_protects_first_recent_lifecycle_and_every_later_lifecycle(
    changelog: ChangelogFixture, cutoff_minute: int, first_retained_cid: int, processed_cids: list[int]
) -> None:
    context, backend, _, table = changelog
    rows = [
        changelog_row(1, "insert"),
        changelog_row(2, "patch", {"a": 1}),
        changelog_row(3, "delete"),
        changelog_row(4, "upsert"),
        changelog_row(5, "patch", {"b": 2}),
        changelog_row(6, "move"),
        changelog_row(7, "upsert"),
        changelog_row(8, "patch", {"c": 3}),
        changelog_row(9, "delete"),
    ]
    with backend.begin() as conn:
        conn.execute(table.insert(), rows)
    compression.compress_changelogs(
        context, cutoff=CREATED_AT + timedelta(minutes=cutoff_minute), compressed_at=COMPRESSED_AT
    )
    expected = [compressed_row(row) if row["_id"] in processed_cids else row for row in rows[first_retained_cid - 1 :]]
    assert read_changelog(backend, table) == expected


def test_retention_compressed_timestamp_does_not_protect_expired_lifecycle(changelog: ChangelogFixture) -> None:
    context, backend, _, table = changelog
    rows = [
        changelog_row(1, "insert"),
        changelog_row(2, "patch", {"a": 1}, compressed=datetime(2020, 3, 1)),
        changelog_row(3, "delete"),
        changelog_row(4, "upsert"),
    ]
    with backend.begin() as conn:
        conn.execute(table.insert(), rows)
    compression.compress_changelogs(context, cutoff=COMPRESSED_AT, compressed_at=COMPRESSED_AT)
    assert read_changelog(backend, table) == [rows[3]]


@pytest.mark.parametrize("batch_size", [1, 2, 10000])
def test_retention_and_compression_keep_resources_independent_across_batches(
    changelog: ChangelogFixture, cli: SpintaCliRunner, batch_size: int
) -> None:
    context, backend, _, table = changelog
    rows = [
        changelog_row(1, "insert", rid=1),
        changelog_row(2, "insert", rid=2),
        changelog_row(3, "patch", {"old": 1}, rid=1),
        changelog_row(4, "patch", {"a": 2}, rid=2),
        changelog_row(5, "delete", rid=1),
        changelog_row(6, "patch", {"b": 3}, rid=2),
        changelog_row(7, "upsert", rid=1),
        changelog_row(8, "patch", {"new": 4}, rid=1),
        changelog_row(9, "upsert", {"extra": 5}, rid=1),
        changelog_row(10, "insert", rid=3),
        changelog_row(11, "update", {"single": 6}, rid=3),
    ]
    with backend.begin() as conn:
        conn.execute(table.insert(), rows)
    rc = context.get("rc").fork({"changelog_compression_batch_size": batch_size})
    started_at = datetime.now(timezone.utc).replace(tzinfo=None)
    result = cli.invoke(rc, ["admin", Script.COMPRESS_CHANGELOGS.value])
    finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
    assert result_contains(result, script_check_status_message(Script.COMPRESS_CHANGELOGS.value, ScriptStatus.REQUIRED))
    actual = read_changelog(backend, table)
    compressed_at = actual[1]["_compressed"]
    assert started_at <= compressed_at <= finished_at
    assert actual == [
        rows[1],
        {**rows[5], "data": {"a": 2, "b": 3}, "_compressed": compressed_at},
        rows[6],
        {**rows[8], "data": {"new": 4, "extra": 5}, "_compressed": compressed_at},
        rows[9],
        {**rows[10], "_compressed": compressed_at},
    ]


@pytest.mark.parametrize("failure_phase", ["after_retention", "after_compression"])
def test_model_compression_rolls_back_retention_and_compression_on_error(
    changelog: ChangelogFixture, monkeypatch: pytest.MonkeyPatch, failure_phase: str
) -> None:
    context, backend, _, table = changelog
    rows = [
        changelog_row(1, "insert"),
        changelog_row(2, "patch", {"old": 1}),
        changelog_row(3, "delete"),
        changelog_row(4, "upsert"),
        changelog_row(5, "patch", {"a": 2}),
        changelog_row(6, "patch", {"b": 3}),
    ]
    with backend.begin() as conn:
        conn.execute(table.insert(), rows)
    compress_lifecycles = compression.compress_lifecycles

    def fail_during_model_processing(
        conn: Connection,
        changelog_table: sa.Table,
        *,
        cutoff: datetime,
        compressed_at: datetime,
        batch_size: int,
        progress_bar: tqdm,
    ) -> None:
        if failure_phase == "after_compression":
            compress_lifecycles(
                conn,
                changelog_table,
                cutoff=cutoff,
                compressed_at=compressed_at,
                batch_size=batch_size,
                progress_bar=progress_bar,
            )
        actual = [dict(row._mapping) for row in conn.execute(sa.select(table).order_by(table.c._id))]
        expected = (
            [rows[3], compressed_row(rows[5], {"a": 2, "b": 3})] if failure_phase == "after_compression" else rows[3:]
        )
        assert actual == expected
        raise RuntimeError("Compression failed before commit")

    # Keep the real database writes; inject failure at the model transaction boundary.
    monkeypatch.setattr(compression, "compress_lifecycles", fail_during_model_processing)
    with pytest.raises(RuntimeError, match="Compression failed before commit"):
        compression.compress_changelogs(context, cutoff=COMPRESSED_AT, compressed_at=COMPRESSED_AT)
    assert read_changelog(backend, table) == rows


def test_compress_changelogs_processes_enabled_models_and_skips_disabled_dataset(changelog: ChangelogFixture) -> None:
    context, backend, _, table = changelog
    manifest = context.get("store").manifest
    city = commands.get_model(context, manifest, "datasets/compression/tests/City")
    disabled = commands.get_model(context, manifest, "datasets/compression/disabled/Country")
    city_table = backend.get_table(city, TableType.CHANGELOG)
    disabled_table = backend.get_table(disabled, TableType.CHANGELOG)
    rows = [
        changelog_row(1, "insert"),
        changelog_row(2, "patch", {"old": 1}),
        changelog_row(3, "delete"),
        changelog_row(4, "upsert"),
        changelog_row(5, "patch", {"a": 2}),
        changelog_row(6, "patch", {"b": 3}),
    ]
    city_rows = [
        changelog_row(1, "insert"),
        changelog_row(2, "patch", {"name": "Vilnius"}),
        changelog_row(3, "upsert", {"country": "LT"}),
    ]
    with backend.begin() as conn:
        conn.execute(table.insert(), rows)
        conn.execute(city_table.insert(), city_rows)
        conn.execute(disabled_table.insert(), rows)
    compression.compress_changelogs(context, cutoff=COMPRESSED_AT, compressed_at=COMPRESSED_AT)
    assert read_changelog(backend, table) == [rows[3], compressed_row(rows[5], {"a": 2, "b": 3})]
    assert read_changelog(backend, city_table) == [
        city_rows[0],
        compressed_row(city_rows[2], {"name": "Vilnius", "country": "LT"}),
    ]
    assert read_changelog(backend, disabled_table) == rows


@pytest.mark.parametrize("explicit_cutoff, explicit_compressed_at", [(False, False), (False, True), (True, False)])
def test_compress_changelogs_uses_retention_policy_and_clock_when_timestamps_omitted(
    changelog: ChangelogFixture, monkeypatch: pytest.MonkeyPatch, explicit_cutoff: bool, explicit_compressed_at: bool
) -> None:
    context, backend, model, table = changelog
    now = datetime(2020, 1, 2, 0, 5)

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> datetime:
            if tz is None:
                return now + timedelta(hours=2)
            return now.replace(tzinfo=timezone.utc).astimezone(tz)

    monkeypatch.setattr(compression, "datetime", FixedDatetime)
    assert model.external.dataset.retention_periodicity == timedelta(days=1)
    rows = [
        changelog_row(1, "insert"),
        changelog_row(2, "patch", {"old": 1}),
        changelog_row(3, "delete"),
        changelog_row(4, "upsert"),
        changelog_row(5, "patch", {"a": 2}),
        changelog_row(6, "patch", {"b": 3}),
    ]
    with backend.begin() as conn:
        conn.execute(table.insert(), rows)
    compression.compress_changelogs(
        context,
        cutoff=CREATED_AT + timedelta(minutes=5) if explicit_cutoff else None,
        compressed_at=COMPRESSED_AT if explicit_compressed_at else None,
    )
    expected_timestamp = COMPRESSED_AT if explicit_compressed_at else now
    assert read_changelog(backend, table) == [rows[3], {**rows[4], "_compressed": expected_timestamp}, rows[5]]
