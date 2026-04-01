from tqdm import tqdm

from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_pg_name
from spinta.cli.helpers.message import cli_message
from spinta.cli.helpers.script.helpers import ensure_store_is_loaded
from spinta.components import Context
from spinta.types.datatype import Ref

import sqlalchemy as sa


def create_sharding_plan(context: Context, verbose: bool = True, **kwargs):
    store = ensure_store_is_loaded(context)
    schemas = set()
    disallowed_schemas = set()
    if verbose:
        cli_message("Creating schema-based sharding plan from manifest")
    for model in commands.get_models(context, store.manifest).values():
        if not model.external or not model.external.dataset:
            continue

        dataset_name = model.external.dataset.name
        if dataset_name in disallowed_schemas:
            continue

        schemas.add(dataset_name)

        if model.base and (base_dataset := model.base.parent.external.dataset.name) != dataset_name:
            disallowed_schemas.add(base_dataset)
            schemas.discard(base_dataset)

        for prop in model.flatprops.values():
            if isinstance(prop.dtype, Ref) and not prop.dtype.inherited:
                if (ref_dataset := prop.dtype.model.external.dataset.name) == dataset_name:
                    continue

                disallowed_schemas.add(ref_dataset)
                schemas.discard(ref_dataset)

                # Currently, we do not support reference tables, so cannot shard root schema aswell
                disallowed_schemas.add(dataset_name)
                schemas.discard(dataset_name)

    return set(get_pg_name(schema) for schema in schemas)


def gather_current_sharding_state(context: Context, verbose: bool = True, **kwargs):
    store = ensure_store_is_loaded(context)
    backend = store.manifest.backend
    if not isinstance(backend, PostgreSQL):
        raise Exception("Gathering sharding state is only supported for PostgreSQL backend.")

    if verbose:
        cli_message("Extracting already distributed schemas")
    sharded_schema = set()
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
            if row["schema_name"] in sharded_schema:
                continue

            if row["distribution_type"] != "local":
                sharded_schema.add(row["schema_name"])
    return sharded_schema


def migrate_citus_distributions(context: Context, destructive: bool, **kwargs):
    required_schemas = create_sharding_plan(context)
    sharded_schemas = gather_current_sharding_state(context)

    missing_schemas = required_schemas - sharded_schemas
    progress_bar = tqdm(missing_schemas, desc="Adding schema distributions", ascii=True, total=len(missing_schemas))
    store = ensure_store_is_loaded(context)
    backend = store.manifest.backend
    with backend.begin() as conn:
        for schema in progress_bar:
            cli_message(schema, progress_bar=progress_bar)
            conn.execute(sa.text(f"SELECT citus_schema_distribute('\"{schema}\"')"))


def cli_requires_deduplicate_migrations(context: Context, **kwargs):
    required_schemas = create_sharding_plan(context, verbose=False)
    sharded_schemas = gather_current_sharding_state(context, verbose=False)

    missing_schemas = required_schemas - sharded_schemas
    return len(missing_schemas) > 0
