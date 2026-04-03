from collections import defaultdict

from tqdm import tqdm

from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_pg_name
from spinta.cli.helpers.message import cli_message
from spinta.cli.helpers.script.helpers import ensure_store_is_loaded
from spinta.components import Context

import sqlalchemy as sa


def create_sharding_plan(context: Context, verbose: bool = True, **kwargs) -> dict[str, set[str]]:
    store = context.get("store")
    schemas: dict[str, set[str]] = defaultdict(set)
    # disallowed_schemas = set()
    if verbose:
        cli_message("Creating schema-based sharding plan from manifest")
    for model in commands.get_models(context, store.manifest).values():
        if not isinstance(model.backend, PostgreSQL):
            continue

        if not model.external or not model.external.dataset:
            continue

        schemas[model.backend.name].add(get_pg_name(model.external.dataset.name))

        # dataset_name = model.external.dataset.name
        # if dataset_name in disallowed_schemas:
        #     continue
        #
        # schemas.add(dataset_name)
        #
        # if model.base and (base_dataset := model.base.parent.external.dataset.name) != dataset_name:
        #     disallowed_schemas.add(base_dataset)
        #     schemas.discard(base_dataset)
        #
        # for prop in model.flatprops.values():
        #     if isinstance(prop.dtype, Ref) and not prop.dtype.inherited:
        #         if (ref_dataset := prop.dtype.model.external.dataset.name) == dataset_name:
        #             continue
        #
        #         disallowed_schemas.add(ref_dataset)
        #         schemas.discard(ref_dataset)
        #
        #         # Currently, we do not support reference tables, so cannot shard root schema aswell
        #         disallowed_schemas.add(dataset_name)
        #         schemas.discard(dataset_name)

    return schemas


def gather_current_sharding_state(context: Context, verbose: bool = True, **kwargs):
    store = context.get("store")
    backends = store.backends

    if verbose:
        cli_message("Extracting already distributed schemas")
    schemas: dict[str, set[str]] = defaultdict(set)
    for backend_name, backend in backends.items():
        if not isinstance(backend, PostgreSQL):
            continue

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
                if row["schema_name"] in schemas[backend_name]:
                    continue

                if row["distribution_type"] != "local":
                    schemas[backend_name].add(row["schema_name"])
    return schemas


def invalidate_incorrect_schema_shards(
    context: Context, schemas: dict[str, set[str]], verbose: bool = True, **kwargs
) -> dict[str, set[str]]:
    if not schemas:
        return schemas

    store = context.get("store")
    invalid_schemas = set()

    inspectors = {backend_name: sa.inspect(backend.engine) for backend_name, backend in store.backends.items()}
    if verbose:
        cli_message("Invalidating incorrect schema shards")
    for backend_name, backend_schemas in schemas.items():
        inspector = inspectors[backend_name]

        for schema in backend_schemas:
            tables = inspector.get_table_names(schema=schema)

            for table in tables:
                foreign_keys = inspector.get_foreign_keys(table, schema=schema)
                if not foreign_keys:
                    continue

                for key in foreign_keys:
                    if key["referred_schema"] == schema:
                        continue

                    invalid_schemas.add(key["referred_schema"])
                    invalid_schemas.add(schema)

    return {backend_name: backend_schemas - invalid_schemas for backend_name, backend_schemas in schemas.items()}


def migrate_citus_distributions(context: Context, destructive: bool, **kwargs):
    store = ensure_store_is_loaded(context)

    required_schemas = create_sharding_plan(context)
    sharded_schemas = gather_current_sharding_state(context)
    schemas_diff = {
        backend_name: required_schemas - sharded_schemas.get(backend_name, set())
        for backend_name, required_schemas in required_schemas.items()
    }
    validated_schemas = invalidate_incorrect_schema_shards(context, schemas_diff)

    for backend_name, schemas in validated_schemas.items():
        progress_bar = tqdm(schemas, desc=f"Adding schema distributions {backend_name}", ascii=True, total=len(schemas))
        backend = store.backends[backend_name]
        with backend.begin() as conn:
            for schema in progress_bar:
                cli_message(schema, progress_bar=progress_bar)
                conn.execute(sa.text(f"SELECT citus_schema_distribute('\"{schema}\"')"))


def cli_requires_deduplicate_migrations(context: Context, **kwargs):
    ensure_store_is_loaded(context)
    required_schemas = create_sharding_plan(context, verbose=False)
    sharded_schemas = gather_current_sharding_state(context, verbose=False)

    schemas_diff = {
        backend_name: required_schemas - sharded_schemas.get(backend_name, set())
        for backend_name, required_schemas in required_schemas.items()
    }
    return sum(len(schemas) for schemas in schemas_diff.values()) > 0
