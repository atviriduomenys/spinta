from pathlib import Path

import pytest
from sqlalchemy.engine import Engine

from spinta import commands
from spinta.backends.helpers import get_table_identifier
from spinta.cli.helpers.admin.components import Script
from spinta.cli.helpers.script.components import ScriptStatus
from spinta.cli.helpers.script.helpers import script_check_status_message
from spinta.core.config import RawConfig
from spinta.testing.citus import gather_citus_state, bootstrap_distribute_manifest
from spinta.testing.cli import SpintaCliRunner


@pytest.mark.parametrize(
    "default_distribution_strategy, default_distribution_property, result_mapping",
    [
        ("schema", None, (("distribute/example", "distribute/data"), None, None, None)),
        ("copy", None, (None, ("distribute/example/Test", "distribute/data/Data"), None, None)),
        ("table", "_id", (None, None, (("distribute/example/Test", "_id"), ("distribute/data/Data", "_id")), None)),
        ("undistributed", None, (None, None, None, ("distribute/example/Test", "distribute/data/Data"))),
    ],
)
def test_default_distribution_schema(
    default_distribution_strategy,
    default_distribution_property,
    result_mapping,
    migration_db: Engine,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
):
    initial_manifest = """
     d                  | r | b | m    | property | type
     distribute/example |   |   |      |          |
                        |   |   | Test |          |
                        |   |   |      | id       | integer
     distribute/data    |   |   |      |          |
                        |   |   | Data |          |
                        |   |   |      | id       | integer
    """
    context, rc = bootstrap_distribute_manifest(
        rc=rc,
        path=tmp_path,
        manifest=initial_manifest,
        default_distribution_strategy=default_distribution_strategy,
        default_distribution_property=default_distribution_property,
    )

    manifest = context.get("store").manifest
    backend = manifest.backend
    current_citus_state = gather_citus_state(context, backend)

    test_model = commands.get_model(context, manifest, "distribute/example/Test")
    data_model = commands.get_model(context, manifest, "distribute/data/Data")

    test_table_identifier = get_table_identifier(test_model)
    data_table_identifier = get_table_identifier(data_model)
    identifier_mapping = {
        test_table_identifier.logical_qualified_name: test_table_identifier,
        data_table_identifier.logical_qualified_name: data_table_identifier,
    }

    assert not current_citus_state.schemas
    assert not current_citus_state.references
    assert not current_citus_state.distributed
    assert {test_table_identifier, data_table_identifier}.issubset(current_citus_state.local)

    result = cli.invoke(context.get("rc"), ["admin", Script.CITUS_DISTRIBUTION.value])
    assert result.exit_code == 0
    updated_citus_state = gather_citus_state(context, backend)

    if result_mapping[0] is not None:
        assert updated_citus_state.schemas == set(result_mapping[0])
    else:
        assert not updated_citus_state.schemas

    if result_mapping[1] is not None:
        assert updated_citus_state.references == set(identifier_mapping[result] for result in result_mapping[1])
    else:
        assert not updated_citus_state.references

    if result_mapping[2] is not None:
        assert updated_citus_state.distributed == set(
            (identifier_mapping[result], prop) for result, prop in result_mapping[2]
        )
    else:
        assert not updated_citus_state.distributed

    if result_mapping[3] is not None:
        assert {identifier_mapping[result] for result in result_mapping[3]}.issubset(updated_citus_state.local)
    else:
        assert not {test_table_identifier, data_table_identifier}.issubset(updated_citus_state.local)


@pytest.mark.parametrize(
    "default_distribution_strategy, default_distribution_property, result_mapping",
    [
        ("schema", None, (("distribute/example", "distribute/data"), None, None, None)),
        ("copy", None, (None, ("distribute/example/Test", "distribute/data/Data"), None, None)),
        ("table", "_id", (None, None, (("distribute/example/Test", "_id"), ("distribute/data/Data", "_id")), None)),
        ("undistributed", None, (None, None, None, ("distribute/example/Test", "distribute/data/Data"))),
    ],
)
def test_undistribute_from_default_distribution(
    default_distribution_strategy,
    default_distribution_property,
    result_mapping,
    migration_db: Engine,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
):
    initial_manifest = """
     d                  | r | b | m    | property | type
     distribute/example |   |   |      |          |
                        |   |   | Test |          |
                        |   |   |      | id       | integer
     distribute/data    |   |   |      |          |
                        |   |   | Data |          |
                        |   |   |      | id       | integer
    """
    context, rc = bootstrap_distribute_manifest(
        rc=rc,
        path=tmp_path,
        manifest=initial_manifest,
        default_distribution_strategy=default_distribution_strategy,
        default_distribution_property=default_distribution_property,
    )

    manifest = context.get("store").manifest
    backend = manifest.backend

    test_model = commands.get_model(context, manifest, "distribute/example/Test")
    data_model = commands.get_model(context, manifest, "distribute/data/Data")

    test_table_identifier = get_table_identifier(test_model)
    data_table_identifier = get_table_identifier(data_model)
    identifier_mapping = {
        test_table_identifier.logical_qualified_name: test_table_identifier,
        data_table_identifier.logical_qualified_name: data_table_identifier,
    }

    result = cli.invoke(context.get("rc"), ["admin", Script.CITUS_DISTRIBUTION.value])
    assert result.exit_code == 0
    updated_citus_state = gather_citus_state(context, backend)

    if result_mapping[0] is not None:
        assert updated_citus_state.schemas == set(result_mapping[0])
    else:
        assert not updated_citus_state.schemas

    if result_mapping[1] is not None:
        assert updated_citus_state.references == set(identifier_mapping[result] for result in result_mapping[1])
    else:
        assert not updated_citus_state.references

    if result_mapping[2] is not None:
        assert updated_citus_state.distributed == set(
            (identifier_mapping[result], prop) for result, prop in result_mapping[2]
        )
    else:
        assert not updated_citus_state.distributed

    if result_mapping[3] is not None:
        assert {identifier_mapping[result] for result in result_mapping[3]}.issubset(updated_citus_state.local)
    else:
        assert not {test_table_identifier, data_table_identifier}.issubset(updated_citus_state.local)

    result = cli.invoke(
        context.get("rc").fork({"default_distribution_strategy": "undistributed"}),
        ["admin", Script.CITUS_DISTRIBUTION.value],
    )
    assert result.exit_code == 0
    updated_citus_state = gather_citus_state(context, backend)

    assert not updated_citus_state.schemas
    assert not updated_citus_state.references
    assert not updated_citus_state.distributed
    assert {test_table_identifier, data_table_identifier}.issubset(updated_citus_state.local)


def test_mixed_distribution(
    migration_db: Engine,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
):
    initial_manifest = """
     d                  | r | b | m    | property | type
     distribute/example |   |   |      |          |
                        |   |   | Test |          |
                        |   |   |      | id       | integer
     distribute/data    |   |   |      |          |
                        |   |   | Data |          |
                        |   |   |      | id       | integer
                        |   |   | New  |          |
                        |   |   |      | id       | integer
    """
    context, rc = bootstrap_distribute_manifest(
        rc=rc,
        path=tmp_path,
        manifest=initial_manifest,
        default_distribution_strategy="schema",
        model_distribution={
            "distribute/data/New": {
                "distribute": "copy",
            },
            "distribute/data/Data": {
                "distribute": {
                    "type": "table",
                    "property": "_id",
                },
            },
        },
    )

    manifest = context.get("store").manifest
    backend = manifest.backend

    test_model = commands.get_model(context, manifest, "distribute/example/Test")
    data_model = commands.get_model(context, manifest, "distribute/data/Data")
    new_model = commands.get_model(context, manifest, "distribute/data/New")

    test_table_identifier = get_table_identifier(test_model)
    data_table_identifier = get_table_identifier(data_model)
    new_table_identifier = get_table_identifier(new_model)

    current_citus_state = gather_citus_state(context, backend)
    assert not current_citus_state.schemas
    assert not current_citus_state.references
    assert not current_citus_state.distributed
    assert {test_table_identifier, data_table_identifier, new_table_identifier}.issubset(current_citus_state.local)

    result = cli.invoke(context.get("rc"), ["admin", Script.CITUS_DISTRIBUTION.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.CITUS_DISTRIBUTION.value, ScriptStatus.REQUIRED) in result.stdout

    updated_citus_state = gather_citus_state(context, backend)
    assert updated_citus_state.schemas == {"distribute/example"}
    assert updated_citus_state.references == {new_table_identifier}
    assert updated_citus_state.distributed == {(data_table_identifier, "_id")}
    assert not {test_table_identifier, data_table_identifier, new_table_identifier}.issubset(updated_citus_state.local)


def test_default_schema_distribution_invalidation(
    migration_db: Engine,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
):
    initial_manifest = """
     d                  | r | b | m    | property | type    | ref
     distribute/example |   |   |      |          |         |
                        |   |   | Test |          |         |
                        |   |   |      | id       | integer |
                        |   |   |      | data     | ref     | distribute/data/Data
     distribute/data    |   |   |      |          |         |
                        |   |   | Data |          |         |
                        |   |   |      | id       | integer |
     distribute/new     |   |   |      |          |         |
                        |   |   | New  |          |         |
                        |   |   |      | id       | integer |
    """
    context, rc = bootstrap_distribute_manifest(
        rc=rc,
        path=tmp_path,
        manifest=initial_manifest,
        default_distribution_strategy="schema",
    )

    manifest = context.get("store").manifest
    backend = manifest.backend

    test_model = commands.get_model(context, manifest, "distribute/example/Test")
    data_model = commands.get_model(context, manifest, "distribute/data/Data")
    new_model = commands.get_model(context, manifest, "distribute/new/New")

    test_table_identifier = get_table_identifier(test_model)
    data_table_identifier = get_table_identifier(data_model)
    new_table_identifier = get_table_identifier(new_model)

    current_citus_state = gather_citus_state(context, backend)
    assert not current_citus_state.schemas
    assert not current_citus_state.references
    assert not current_citus_state.distributed
    assert {test_table_identifier, data_table_identifier}.issubset(current_citus_state.local)

    result = cli.invoke(rc, ["admin", Script.CITUS_DISTRIBUTION.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.CITUS_DISTRIBUTION.value, ScriptStatus.REQUIRED) in result.stdout

    updated_citus_state = gather_citus_state(context, backend)
    assert updated_citus_state.schemas == {"distribute/new"}
    assert not updated_citus_state.references
    assert not updated_citus_state.distributed
    assert {data_table_identifier, test_table_identifier}.issubset(updated_citus_state.local)
    assert not {new_table_identifier}.issubset(updated_citus_state.local)

    result = cli.invoke(
        rc.fork({"models": {"distribute/data/Data": {"distribute": "copy"}}}),
        ["admin", Script.CITUS_DISTRIBUTION.value],
    )
    assert result.exit_code == 0
    assert script_check_status_message(Script.CITUS_DISTRIBUTION.value, ScriptStatus.REQUIRED) in result.stdout

    updated_citus_state = gather_citus_state(context, backend)
    assert updated_citus_state.schemas == {"distribute/new", "distribute/example"}
    assert updated_citus_state.references == {data_table_identifier}
    assert not updated_citus_state.distributed
    assert not {new_table_identifier, data_table_identifier, test_table_identifier}.issubset(updated_citus_state.local)
