from pathlib import Path

from ruamel.yaml import YAML
from sqlalchemy.engine import Engine

from spinta import commands
from spinta.backends.helpers import get_table_identifier
from spinta.backends.postgresql.helpers.migrate.citus import gather_current_sharding_plan
from spinta.cli.helpers.admin.components import Script
from spinta.cli.helpers.script.components import ScriptStatus
from spinta.cli.helpers.script.helpers import script_check_status_message
from spinta.core.config import RawConfig
from spinta.testing.citus import bootstrap_distribute_manifest
from spinta.testing.cli import SpintaCliRunner, result_contains

yaml = YAML(typ="safe")


SIMPLE_CROSS_SCHEMA_REFERENCE_MANIFEST = """
     d                  | r | b | m    | property | type    | ref
     distribute/example |   |   |      |          |         |
                        |   |   | Test |          |         |
                        |   |   |      | id       | integer |
     distribute/data    |   |   |      |          |         |
                        |   |   | Data |          |         |
                        |   |   |      | id       | integer |
                        |   |   |      | test     | ref     | distribute/example/Test
"""


def test_citus_reference_config_output_stdout(
    migration_db: Engine,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
):
    context, rc = bootstrap_distribute_manifest(
        rc=rc,
        path=tmp_path,
        manifest=SIMPLE_CROSS_SCHEMA_REFERENCE_MANIFEST,
        default_distribution_strategy="schema",
    )

    manifest = context.get("store").manifest
    backend = manifest.backend

    test_model = commands.get_model(context, manifest, "distribute/example/Test")
    data_model = commands.get_model(context, manifest, "distribute/data/Data")

    test_table_identifier = get_table_identifier(test_model)
    data_table_identifier = get_table_identifier(data_model)

    current_citus_state = gather_current_sharding_plan(context, backend)
    assert not current_citus_state.schemas
    assert not current_citus_state.references
    assert not current_citus_state.distributed
    assert {test_table_identifier, data_table_identifier}.issubset(current_citus_state.local)

    result = cli.invoke(context.get("rc"), ["admin", Script.CITUS_REFERENCE_CONFIG.value])
    assert result.exit_code == 0
    assert result_contains(
        result, script_check_status_message(Script.CITUS_REFERENCE_CONFIG.value, ScriptStatus.REQUIRED)
    )
    yml_output = yaml.load(result.stdout)
    assert yml_output == {"models": {"distribute/example/Test": {"distribute": "copy"}}}


def test_citus_reference_config_output_yml_file(
    migration_db: Engine,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
):
    context, rc = bootstrap_distribute_manifest(
        rc=rc,
        path=tmp_path,
        manifest=SIMPLE_CROSS_SCHEMA_REFERENCE_MANIFEST,
        default_distribution_strategy="schema",
    )

    manifest = context.get("store").manifest
    backend = manifest.backend

    test_model = commands.get_model(context, manifest, "distribute/example/Test")
    data_model = commands.get_model(context, manifest, "distribute/data/Data")

    test_table_identifier = get_table_identifier(test_model)
    data_table_identifier = get_table_identifier(data_model)

    current_citus_state = gather_current_sharding_plan(context, backend)
    assert not current_citus_state.schemas
    assert not current_citus_state.references
    assert not current_citus_state.distributed
    assert {test_table_identifier, data_table_identifier}.issubset(current_citus_state.local)

    output_path = tmp_path / "citus_reference_config.yml"
    result = cli.invoke(context.get("rc"), ["admin", Script.CITUS_REFERENCE_CONFIG.value, "--output", str(output_path)])
    assert result.exit_code == 0
    assert result_contains(
        result, script_check_status_message(Script.CITUS_REFERENCE_CONFIG.value, ScriptStatus.REQUIRED)
    )
    assert not result.stdout
    assert output_path.exists()
    with output_path.open("r") as f:
        yml_output = yaml.load(f)
    assert yml_output == {"models": {"distribute/example/Test": {"distribute": "copy"}}}


def test_citus_reference_config_output_invalid_file_type(
    migration_db: Engine,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
):
    context, rc = bootstrap_distribute_manifest(
        rc=rc,
        path=tmp_path,
        manifest=SIMPLE_CROSS_SCHEMA_REFERENCE_MANIFEST,
        default_distribution_strategy="schema",
    )

    manifest = context.get("store").manifest
    backend = manifest.backend

    test_model = commands.get_model(context, manifest, "distribute/example/Test")
    data_model = commands.get_model(context, manifest, "distribute/data/Data")

    test_table_identifier = get_table_identifier(test_model)
    data_table_identifier = get_table_identifier(data_model)

    current_citus_state = gather_current_sharding_plan(context, backend)
    assert not current_citus_state.schemas
    assert not current_citus_state.references
    assert not current_citus_state.distributed
    assert {test_table_identifier, data_table_identifier}.issubset(current_citus_state.local)

    output_path = tmp_path / "citus_reference_config.txt"
    result = cli.invoke(
        context.get("rc"), ["admin", Script.CITUS_REFERENCE_CONFIG.value, "--output", str(output_path)], fail=False
    )
    assert result.exit_code == 1
    assert result_contains(
        result, script_check_status_message(Script.CITUS_REFERENCE_CONFIG.value, ScriptStatus.REQUIRED)
    )
    assert not result.stdout
    assert not output_path.exists()
    assert f"Output file '{output_path}' has unsupported format. Use '.yml' or '.yaml'." in result.stderr


def test_citus_reference_config_output_existing_file(
    migration_db: Engine,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
):
    context, rc = bootstrap_distribute_manifest(
        rc=rc,
        path=tmp_path,
        manifest=SIMPLE_CROSS_SCHEMA_REFERENCE_MANIFEST,
        default_distribution_strategy="schema",
    )

    manifest = context.get("store").manifest
    backend = manifest.backend

    test_model = commands.get_model(context, manifest, "distribute/example/Test")
    data_model = commands.get_model(context, manifest, "distribute/data/Data")

    test_table_identifier = get_table_identifier(test_model)
    data_table_identifier = get_table_identifier(data_model)

    current_citus_state = gather_current_sharding_plan(context, backend)
    assert not current_citus_state.schemas
    assert not current_citus_state.references
    assert not current_citus_state.distributed
    assert {test_table_identifier, data_table_identifier}.issubset(current_citus_state.local)

    output_path = tmp_path / "citus_reference_config.yml"
    with open(output_path, "w") as f:
        f.write("test")

    assert output_path.exists()
    result = cli.invoke(
        context.get("rc"), ["admin", Script.CITUS_REFERENCE_CONFIG.value, "--output", str(output_path)], fail=False
    )
    assert result.exit_code == 1
    assert result_contains(
        result, script_check_status_message(Script.CITUS_REFERENCE_CONFIG.value, ScriptStatus.REQUIRED)
    )
    assert not result.stdout
    assert f"Output file '{output_path}' already exists. Use --destructive to overwrite." in result.stderr

    assert output_path.exists()
    result = cli.invoke(
        context.get("rc"), ["admin", Script.CITUS_REFERENCE_CONFIG.value, "--output", str(output_path), "--destructive"]
    )
    assert result.exit_code == 0
    assert result_contains(
        result, script_check_status_message(Script.CITUS_REFERENCE_CONFIG.value, ScriptStatus.REQUIRED)
    )
    assert not result.stdout
    with output_path.open("r") as f:
        yml_output = yaml.load(f)
    assert yml_output == {"models": {"distribute/example/Test": {"distribute": "copy"}}}
