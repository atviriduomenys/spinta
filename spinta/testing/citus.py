import dataclasses

from sqlalchemy.engine.url import make_url

from spinta import commands
from spinta.backends.helpers import TableIdentifier, get_table_identifier
from spinta.backends.postgresql.components import PostgreSQL
from spinta.cli.helpers.store import load_store
from spinta.components import Context

import sqlalchemy as sa

from spinta.manifests.sql.helpers import is_internal_schema
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.context import create_test_context
from spinta.testing.pytest import MIGRATION_DATABASE
from spinta.testing.tabular import create_tabular_manifest


@dataclasses.dataclass
class CitusState:
    local: set[TableIdentifier] = dataclasses.field(default_factory=set)
    distributed: set[tuple[TableIdentifier, str]] = dataclasses.field(default_factory=set)
    references: set[TableIdentifier] = dataclasses.field(default_factory=set)
    schemas: set[str] = dataclasses.field(default_factory=set)


def gather_citus_state(context: Context, backend: PostgreSQL):
    citus_state = CitusState()
    with backend.begin() as conn:
        result = conn.execute(
            sa.text("""
                    SELECT
                        n.nspname AS schema_name,
                        c.relname AS table_name,
                        format('%I.%I', n.nspname, c.relname) AS full_table_name,
                        d.description AS table_comment,

                        COALESCE(ct.citus_table_type, 'local') AS distribution_type,
                        ct.distribution_column

                    FROM pg_class c
                             JOIN pg_namespace n ON n.oid = c.relnamespace

                             LEFT JOIN citus_tables ct
                                       ON ct.table_name = c.oid::regclass

    LEFT JOIN pg_description d
                    ON d.objoid = c.oid
                        AND d.objsubid = 0

                    WHERE c.relkind = 'r'
                      AND n.nspname NOT LIKE 'pg_%'
                      AND n.nspname <> 'information_schema'

                    ORDER BY n.nspname, c.relname;""")
        ).fetchall()

        for row in result:
            if is_internal_schema(backend.engine, row["schema_name"]):
                continue

            if not row["table_comment"]:
                continue

            table_identifier = get_table_identifier(row["table_comment"])
            match row["distribution_type"]:
                case "schema":
                    citus_state.schemas.add(row["schema_name"])
                case "distributed":
                    citus_state.distributed.add((table_identifier, row["distribution_column"]))
                case "reference":
                    citus_state.references.add(table_identifier)
                case _:
                    citus_state.local.add(table_identifier)
    return citus_state


def configure_distribute(
    rc,
    path,
    manifest,
    *,
    default_distribution_strategy: str | None = None,
    default_distribution_property: str | None = None,
    model_distribution: dict | None = None,
):
    url = make_url(rc.get("backends", "default", "dsn", required=True))
    # Reusing migration database, since it gets recreated each new test
    url = url.set(database=MIGRATION_DATABASE)

    updated_dict = {
        "manifests": {
            "default": {
                "type": "tabular",
                "path": str(path / "manifest.csv"),
                "backend": "default",
                "keymap": "default",
                "mode": "internal",
            },
        },
        "backends": {
            "default": {"type": "postgresql", "dsn": url},
        },
    }
    if default_distribution_strategy is not None:
        updated_dict["default_distribution_strategy"] = default_distribution_strategy
    if default_distribution_property is not None:
        updated_dict["default_distribution_property"] = default_distribution_property
    if model_distribution is not None:
        updated_dict["models"] = model_distribution
    rc = rc.fork(updated_dict)
    context = create_test_context(rc, name="pytest/cli")
    create_tabular_manifest(context, f"{path}/manifest.csv", striptable(manifest))
    return context, rc


def bootstrap_distribute_manifest(
    rc,
    manifest,
    path,
    *,
    default_distribution_strategy: str | None = None,
    default_distribution_property: str | None = None,
    model_distribution: dict | None = None,
):
    context, rc = configure_distribute(
        rc=rc,
        path=path,
        manifest=manifest,
        default_distribution_strategy=default_distribution_strategy,
        default_distribution_property=default_distribution_property,
        model_distribution=model_distribution,
    )
    store = load_store(context, verbose=False)
    commands.load(context, store.manifest)
    commands.link(context, store.manifest)
    commands.check(context, store.manifest)
    commands.prepare(context, store.manifest)
    commands.bootstrap(context, store.manifest)
    context.loaded = True
    return context, rc
