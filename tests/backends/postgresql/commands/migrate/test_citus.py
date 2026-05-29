from pathlib import Path

from sqlalchemy.engine import Engine

from spinta import commands
from spinta.backends.helpers import get_table_identifier, TableIdentifier
from spinta.backends.postgresql.helpers.migrate.citus import gather_current_sharding_plan
from spinta.core.config import RawConfig
from spinta.testing.citus import bootstrap_distribute_manifest
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.migration import (
    add_schema,
    add_index,
    add_column_comment,
    add_table_comment,
    add_changelog_table,
    add_redirect_table,
    add_schema_distribution,
    add_reference_distribution,
    add_table_distribution,
    remove_schema_distribution,
    remove_table_distribution,
)
from tests.backends.postgresql.commands.migrate.test_migrations import override_manifest

EMPTY_DISTRIBUTE_MANIFEST = """
     d | r | b | m | property | type
"""

BASIC_DISTRIBUTE_MANIFEST = """
     d                  | r | b | m    | property | type
     distribute/example |   |   |      |          |
                        |   |   | Test |          |
                        |   |   |      | id       | integer
     distribute/data    |   |   |      |          |
                        |   |   | Data |          |
                        |   |   |      | id       | integer
"""


def _distribute_example_test_create(table_identifier: TableIdentifier) -> str:
    return (
        'CREATE TABLE "distribute/example"."Test" (\n'
        "    _txn UUID, \n"
        "    _created TIMESTAMP WITHOUT TIME ZONE, \n"
        "    _updated TIMESTAMP WITHOUT TIME ZONE, \n"
        "    _id UUID NOT NULL, \n"
        "    _revision TEXT, \n"
        "    id INTEGER, \n"
        '    CONSTRAINT "pk_Test" PRIMARY KEY (_id)\n'
        ");\n\n"
        f"{add_index(table_identifier=table_identifier, index_name='ix_Test__txn', columns=['_txn'])}"
        f"{add_column_comment(table_identifier=table_identifier, column='_txn')}"
        f"{add_column_comment(table_identifier=table_identifier, column='_created')}"
        f"{add_column_comment(table_identifier=table_identifier, column='_updated')}"
        f"{add_column_comment(table_identifier=table_identifier, column='_id')}"
        f"{add_column_comment(table_identifier=table_identifier, column='_revision')}"
        f"{add_column_comment(table_identifier=table_identifier, column='id')}"
        f"{add_table_comment(table_identifier=table_identifier, comment='distribute/example/Test')}"
    )


def _distribute_data_data_create(table_identifier: TableIdentifier) -> str:
    return (
        'CREATE TABLE "distribute/data"."Data" (\n'
        "    _txn UUID, \n"
        "    _created TIMESTAMP WITHOUT TIME ZONE, \n"
        "    _updated TIMESTAMP WITHOUT TIME ZONE, \n"
        "    _id UUID NOT NULL, \n"
        "    _revision TEXT, \n"
        "    id INTEGER, \n"
        '    CONSTRAINT "pk_Data" PRIMARY KEY (_id)\n'
        ");\n\n"
        f"{add_index(table_identifier=table_identifier, index_name='ix_Data__txn', columns=['_txn'])}"
        f"{add_column_comment(table_identifier=table_identifier, column='_txn')}"
        f"{add_column_comment(table_identifier=table_identifier, column='_created')}"
        f"{add_column_comment(table_identifier=table_identifier, column='_updated')}"
        f"{add_column_comment(table_identifier=table_identifier, column='_id')}"
        f"{add_column_comment(table_identifier=table_identifier, column='_revision')}"
        f"{add_column_comment(table_identifier=table_identifier, column='id')}"
        f"{add_table_comment(table_identifier=table_identifier, comment='distribute/data/Data')}"
    )


def test_migrate_default_distribution_schema(
    migration_db: Engine,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
):
    context, rc = bootstrap_distribute_manifest(
        rc=rc,
        path=tmp_path,
        manifest=EMPTY_DISTRIBUTE_MANIFEST,
        default_distribution_strategy="schema",
    )

    manifest = context.get("store").manifest
    backend = manifest.backend

    current_citus_state = gather_current_sharding_plan(context, backend)
    assert not current_citus_state.schemas
    assert not current_citus_state.references
    assert not current_citus_state.distributed

    override_manifest(
        context,
        tmp_path,
        BASIC_DISTRIBUTE_MANIFEST,
    )

    result = cli.invoke(context.get("rc"), ["migrate", "-p"])

    example_table_identifier = get_table_identifier("distribute/example/Test")
    data_table_identifier = get_table_identifier("distribute/data/Data")

    assert result.output.endswith(
        "BEGIN;\n\n"
        f"{add_schema(schema='distribute/example')}"
        f"{add_schema(schema='distribute/data')}"
        f"{_distribute_example_test_create(table_identifier=example_table_identifier)}"
        f"{add_changelog_table(table_identifier=example_table_identifier, comment='distribute/example/Test/:changelog')}"
        f"{add_redirect_table(table_identifier=example_table_identifier, comment='distribute/example/Test/:redirect')}"
        f"{_distribute_data_data_create(table_identifier=data_table_identifier)}"
        f"{add_changelog_table(table_identifier=data_table_identifier, comment='distribute/data/Data/:changelog')}"
        f"{add_redirect_table(table_identifier=data_table_identifier, comment='distribute/data/Data/:redirect')}"
        f"{add_schema_distribution(schema='distribute/data')}"
        f"{add_schema_distribution(schema='distribute/example')}"
        "COMMIT;\n\n"
    )

    result = cli.invoke(context.get("rc"), ["migrate"])
    assert result.exit_code == 0

    updated_citus_state = gather_current_sharding_plan(context, backend)

    assert updated_citus_state.schemas == {"distribute/example", "distribute/data"}
    assert not updated_citus_state.references
    assert not updated_citus_state.distributed
    assert not {example_table_identifier, data_table_identifier}.issubset(updated_citus_state.local)

    result = cli.invoke(
        context.get("rc").fork({"default_distribution_strategy": "undistributed"}),
        ["migrate", "-p"],
    )
    assert result.output.endswith(
        "BEGIN;\n\n"
        f"{remove_schema_distribution(schema='distribute/data')}"
        f"{remove_schema_distribution(schema='distribute/example')}"
        "COMMIT;\n\n"
    )
    result = cli.invoke(
        context.get("rc").fork({"default_distribution_strategy": "undistributed"}),
        ["migrate"],
    )
    assert result.exit_code == 0

    updated_citus_state = gather_current_sharding_plan(context, backend)

    assert not updated_citus_state.schemas
    assert not updated_citus_state.references
    assert not updated_citus_state.distributed


def test_migrate_default_distribution_copy(
    migration_db: Engine,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
):
    context, rc = bootstrap_distribute_manifest(
        rc=rc,
        path=tmp_path,
        manifest=EMPTY_DISTRIBUTE_MANIFEST,
        default_distribution_strategy="copy",
    )

    manifest = context.get("store").manifest
    backend = manifest.backend

    current_citus_state = gather_current_sharding_plan(context, backend)
    assert not current_citus_state.schemas
    assert not current_citus_state.references
    assert not current_citus_state.distributed

    override_manifest(
        context,
        tmp_path,
        BASIC_DISTRIBUTE_MANIFEST,
    )

    result = cli.invoke(context.get("rc"), ["migrate", "-p"])

    example_table_identifier = get_table_identifier("distribute/example/Test")
    data_table_identifier = get_table_identifier("distribute/data/Data")

    assert result.output.endswith(
        "BEGIN;\n\n"
        f"{add_schema(schema='distribute/example')}"
        f"{add_schema(schema='distribute/data')}"
        f"{_distribute_example_test_create(table_identifier=example_table_identifier)}"
        f"{add_reference_distribution(table_identifier=example_table_identifier)}"
        f"{add_changelog_table(table_identifier=example_table_identifier, comment='distribute/example/Test/:changelog')}"
        f"{add_redirect_table(table_identifier=example_table_identifier, comment='distribute/example/Test/:redirect')}"
        f"{_distribute_data_data_create(table_identifier=data_table_identifier)}"
        f"{add_reference_distribution(table_identifier=data_table_identifier)}"
        f"{add_changelog_table(table_identifier=data_table_identifier, comment='distribute/data/Data/:changelog')}"
        f"{add_redirect_table(table_identifier=data_table_identifier, comment='distribute/data/Data/:redirect')}"
        "COMMIT;\n\n"
    )

    result = cli.invoke(context.get("rc"), ["migrate"])
    assert result.exit_code == 0

    updated_citus_state = gather_current_sharding_plan(context, backend)

    assert not updated_citus_state.schemas
    assert updated_citus_state.references == {data_table_identifier, example_table_identifier}
    assert not updated_citus_state.distributed
    assert not {example_table_identifier, data_table_identifier}.issubset(updated_citus_state.local)

    result = cli.invoke(
        context.get("rc").fork({"default_distribution_strategy": "undistributed"}),
        ["migrate", "-p"],
    )
    assert result.output.endswith(
        "BEGIN;\n\n"
        f"{remove_table_distribution(table_identifier=data_table_identifier)}"
        f"{remove_table_distribution(table_identifier=example_table_identifier)}"
        "COMMIT;\n\n"
    )
    result = cli.invoke(
        context.get("rc").fork({"default_distribution_strategy": "undistributed"}),
        ["migrate"],
    )
    assert result.exit_code == 0

    updated_citus_state = gather_current_sharding_plan(context, backend)

    assert not updated_citus_state.schemas
    assert not updated_citus_state.references
    assert not updated_citus_state.distributed


def test_migrate_default_distribution_table(
    migration_db: Engine,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
):
    context, rc = bootstrap_distribute_manifest(
        rc=rc,
        path=tmp_path,
        manifest=EMPTY_DISTRIBUTE_MANIFEST,
        default_distribution_strategy="table",
        default_distribution_property="_id",
    )

    manifest = context.get("store").manifest
    backend = manifest.backend

    current_citus_state = gather_current_sharding_plan(context, backend)
    assert not current_citus_state.schemas
    assert not current_citus_state.references
    assert not current_citus_state.distributed

    override_manifest(
        context,
        tmp_path,
        BASIC_DISTRIBUTE_MANIFEST,
    )

    result = cli.invoke(context.get("rc"), ["migrate", "-p"])

    example_table_identifier = get_table_identifier("distribute/example/Test")
    data_table_identifier = get_table_identifier("distribute/data/Data")

    assert result.output.endswith(
        "BEGIN;\n\n"
        f"{add_schema(schema='distribute/example')}"
        f"{add_schema(schema='distribute/data')}"
        f"{_distribute_example_test_create(table_identifier=example_table_identifier)}"
        f"{add_changelog_table(table_identifier=example_table_identifier, comment='distribute/example/Test/:changelog')}"
        f"{add_redirect_table(table_identifier=example_table_identifier, comment='distribute/example/Test/:redirect')}"
        f"{_distribute_data_data_create(table_identifier=data_table_identifier)}"
        f"{add_changelog_table(table_identifier=data_table_identifier, comment='distribute/data/Data/:changelog')}"
        f"{add_redirect_table(table_identifier=data_table_identifier, comment='distribute/data/Data/:redirect')}"
        f"{add_table_distribution(table_identifier=data_table_identifier, column='_id')}"
        f"{add_table_distribution(table_identifier=example_table_identifier, column='_id')}"
        "COMMIT;\n\n"
    )

    result = cli.invoke(context.get("rc"), ["migrate"])
    assert result.exit_code == 0

    updated_citus_state = gather_current_sharding_plan(context, backend)

    assert not updated_citus_state.schemas
    assert not updated_citus_state.references
    assert updated_citus_state.distributed == {data_table_identifier: "_id", example_table_identifier: "_id"}
    assert not {example_table_identifier, data_table_identifier}.issubset(updated_citus_state.local)

    result = cli.invoke(
        context.get("rc").fork({"default_distribution_strategy": "undistributed"}),
        ["migrate", "-p"],
    )
    assert result.output.endswith(
        "BEGIN;\n\n"
        f"{remove_table_distribution(table_identifier=example_table_identifier)}"
        f"{remove_table_distribution(table_identifier=data_table_identifier)}"
        "COMMIT;\n\n"
    )
    result = cli.invoke(
        context.get("rc").fork({"default_distribution_strategy": "undistributed"}),
        ["migrate"],
    )
    assert result.exit_code == 0

    updated_citus_state = gather_current_sharding_plan(context, backend)

    assert not updated_citus_state.schemas
    assert not updated_citus_state.references
    assert not updated_citus_state.distributed


def test_migrate_mixed_distribution(
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

    current_citus_state = gather_current_sharding_plan(context, backend)
    assert not current_citus_state.schemas
    assert not current_citus_state.references
    assert not current_citus_state.distributed
    assert {test_table_identifier, data_table_identifier, new_table_identifier}.issubset(current_citus_state.local)

    result = cli.invoke(context.get("rc"), ["migrate", "-p"])
    assert result.output.endswith(
        "BEGIN;\n\n"
        f"{add_reference_distribution(table_identifier=new_table_identifier)}"
        f"{add_table_distribution(table_identifier=data_table_identifier, column='_id')}"
        f"{add_schema_distribution(schema='distribute/example')}"
        "COMMIT;\n\n"
    )
    result = cli.invoke(context.get("rc"), ["migrate"])
    assert result.exit_code == 0

    updated_citus_state = gather_current_sharding_plan(context, backend)
    assert updated_citus_state.schemas == {"distribute/example"}
    assert updated_citus_state.references == {new_table_identifier}
    assert updated_citus_state.distributed == {data_table_identifier: "_id"}
    assert not {test_table_identifier, data_table_identifier, new_table_identifier}.issubset(updated_citus_state.local)


def test_migrate_default_schema_distribution_invalidation(
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

    current_citus_state = gather_current_sharding_plan(context, backend)
    assert not current_citus_state.schemas
    assert not current_citus_state.references
    assert not current_citus_state.distributed
    assert {test_table_identifier, data_table_identifier}.issubset(current_citus_state.local)

    result = cli.invoke(rc, ["migrate", "-p"])
    assert result.output.endswith(f"BEGIN;\n\n{add_schema_distribution(schema='distribute/new')}COMMIT;\n\n")
    result = cli.invoke(rc, ["migrate"])
    assert result.exit_code == 0

    updated_citus_state = gather_current_sharding_plan(context, backend)
    assert updated_citus_state.schemas == {"distribute/new"}
    assert not updated_citus_state.references
    assert not updated_citus_state.distributed
    assert {data_table_identifier, test_table_identifier}.issubset(updated_citus_state.local)
    assert not {new_table_identifier}.issubset(updated_citus_state.local)

    result = cli.invoke(
        rc.fork({"models": {"distribute/data/Data": {"distribute": "copy"}}}),
        ["migrate", "-p"],
    )
    assert result.output.endswith(
        "BEGIN;\n\n"
        f"{add_reference_distribution(table_identifier=data_table_identifier)}"
        f"{add_schema_distribution(schema='distribute/example')}"
        "COMMIT;\n\n"
    )

    result = cli.invoke(
        rc.fork({"models": {"distribute/data/Data": {"distribute": "copy"}}}),
        ["migrate"],
    )
    assert result.exit_code == 0

    updated_citus_state = gather_current_sharding_plan(context, backend)
    assert updated_citus_state.schemas == {"distribute/new", "distribute/example"}
    assert updated_citus_state.references == {data_table_identifier}
    assert not updated_citus_state.distributed
    assert not {new_table_identifier, data_table_identifier, test_table_identifier}.issubset(updated_citus_state.local)
