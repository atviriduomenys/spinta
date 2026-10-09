import pathlib
import sys
from collections import defaultdict
from contextlib import nullcontext
from dataclasses import dataclass, replace

from ruamel.yaml import YAML
from sqlalchemy import text

from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.cli.helpers.message import cli_error, cli_message
from spinta.cli.helpers.script.helpers import ensure_store_is_loaded
from spinta.components import Context
from spinta.exceptions import ModelNotFound

FOREIGN_KEY_METADATA_QUERY = text("""
WITH table_metadata AS (
    SELECT
        rel.oid AS relation_oid,
        ns.nspname AS schema_name,
        rel.relname AS table_name,
        format('%s/%s', ns.nspname, rel.relname) AS qualified_name,
        obj_description(rel.oid, 'pg_class') AS table_comment,
        pg_total_relation_size(rel.oid) AS table_bytes,
        COALESCE(ct.citus_table_type, 'local') AS citus_table_type,
        COALESCE(ct.citus_table_type = 'local', TRUE) AS is_unmanaged_local
    FROM pg_class rel
    JOIN pg_namespace ns
        ON ns.oid = rel.relnamespace
    LEFT JOIN citus_tables ct
        ON ct.table_name = rel.oid::regclass
    WHERE rel.relkind IN ('r', 'p')
      AND ns.nspname NOT LIKE 'pg_%'
      AND ns.nspname NOT IN (
          'information_schema',
          'tiger',
          'tiger_data',
          'topology',
          'citus',
          'citus_internal'
      )
)
SELECT DISTINCT
    src.schema_name AS source_schema,
    src.table_name AS source_table,
    tgt.schema_name AS target_schema,
    tgt.table_name AS target_table,
    src.schema_name <> tgt.schema_name AS is_cross_schema,

    src.qualified_name AS source_qualified_name,
    src.table_comment AS source_table_comment,
    src.table_bytes AS source_table_bytes,
    src.citus_table_type AS source_citus_table_type,
    src.is_unmanaged_local AS source_is_unmanaged_local,

    tgt.qualified_name AS target_qualified_name,
    tgt.table_comment AS target_table_comment,
    tgt.table_bytes AS target_table_bytes,
    tgt.citus_table_type AS target_citus_table_type,
    tgt.is_unmanaged_local AS target_is_unmanaged_local
FROM pg_constraint con
JOIN table_metadata src
    ON src.relation_oid = con.conrelid
JOIN table_metadata tgt
    ON tgt.relation_oid = con.confrelid
WHERE con.contype = 'f'
ORDER BY
    source_schema,
    source_table,
    target_schema,
    target_table;
""")

yaml = YAML(typ="safe")
yaml.default_flow_style = False

TableKey = tuple[str, str]


@dataclass(frozen=True)
class ReferenceTable:
    schema: str
    table: str
    qualified_name: str
    comment: str | None
    table_bytes: int
    citus_table_type: str
    is_unmanaged_local: bool
    eligible: bool = False

    @property
    def key(self) -> TableKey:
        return self.schema, self.table

    @property
    def is_reference(self) -> bool:
        return self.citus_table_type == "reference"


@dataclass(frozen=True)
class ForeignKey:
    source: TableKey
    target: TableKey
    is_cross_schema: bool


def _dependency_closure(
    seeds: set[TableKey],
    dependencies: dict[TableKey, set[TableKey]],
    tables: dict[TableKey, ReferenceTable],
    available: set[TableKey] | None = None,
) -> set[TableKey]:
    closure: set[TableKey] = set()
    pending = list(seeds)

    while pending:
        table_key = pending.pop()
        if table_key in closure:
            continue
        if available is not None and table_key not in available:
            continue

        table = tables[table_key]
        if not table.is_unmanaged_local:
            continue

        closure.add(table_key)
        for dependency in dependencies[table_key]:
            dependent_table = tables.get(dependency)
            if (
                dependent_table is not None
                and dependent_table.is_unmanaged_local
                and (available is None or dependency in available)
            ):
                pending.append(dependency)

    return closure


def plan_reference_tables(
    tables: dict[TableKey, ReferenceTable], foreign_keys: set[ForeignKey]
) -> list[ReferenceTable]:
    dependencies: dict[TableKey, set[TableKey]] = defaultdict(set)
    seeds: set[TableKey] = set()
    for foreign_key in foreign_keys:
        dependencies[foreign_key.source].add(foreign_key.target)
        target = tables[foreign_key.target]
        if foreign_key.is_cross_schema and target.is_unmanaged_local:
            seeds.add(foreign_key.target)

    available = {table_key for table_key, table in tables.items() if table.eligible}
    while True:
        candidates = _dependency_closure(seeds & available, dependencies, tables, available)
        blocked = set()
        for table_key in candidates:
            for dependency in dependencies[table_key]:
                dependent_table = tables.get(dependency)
                if dependent_table is not None and dependent_table.is_reference:
                    continue
                if dependency not in candidates:
                    blocked.add(table_key)
                    break
        if blocked:
            available -= blocked
            continue

        return [tables[table_key] for table_key in sorted(candidates)]


def _reference_table_closure(tables: dict[TableKey, ReferenceTable], foreign_keys: set[ForeignKey]) -> set[TableKey]:
    dependencies: dict[TableKey, set[TableKey]] = defaultdict(set)
    seeds: set[TableKey] = set()
    for foreign_key in foreign_keys:
        dependencies[foreign_key.source].add(foreign_key.target)
        target = tables[foreign_key.target]
        if foreign_key.is_cross_schema and target.is_unmanaged_local:
            seeds.add(foreign_key.target)

    return _dependency_closure(seeds, dependencies, tables)


def _table_from_row(row: dict, prefix: str) -> ReferenceTable:
    return ReferenceTable(
        schema=row[f"{prefix}_schema"],
        table=row[f"{prefix}_table"],
        qualified_name=row[f"{prefix}_qualified_name"],
        comment=row[f"{prefix}_table_comment"],
        table_bytes=row[f"{prefix}_table_bytes"],
        citus_table_type=row[f"{prefix}_citus_table_type"],
        is_unmanaged_local=row[f"{prefix}_is_unmanaged_local"],
    )


def _is_eligible_reference_table(context: Context, store, table: ReferenceTable, max_size: int) -> bool:
    if not table.is_unmanaged_local or table.table_bytes >= max_size:
        return False

    model_name = table.comment or table.qualified_name
    try:
        model = commands.get_model(context, store.manifest, model_name)

    except ModelNotFound:
        cli_message(f"Could not find {model_name} model.")
        return False

    return (model.external and model.external.dataset) and (
        model.distribution_strategy is None or model.distribution_strategy.default
    )


def generate_citus_reference_shard_config(
    context: Context, output_path: pathlib.Path | None, destructive: bool, **kwargs
) -> None:
    store = ensure_store_is_loaded(context)

    if output_path and output_path.suffix not in {".yml", ".yaml"}:
        cli_error(f"Output file '{output_path}' has unsupported format. Use '.yml' or '.yaml'.")

    if output_path and output_path.exists() and not destructive:
        cli_error(f"Output file '{output_path}' already exists. Use --destructive to overwrite.")

    config = context.get("config")
    max_size = config.citus_reference_script_size

    config_output = {"models": {}}
    config_models = config_output["models"]
    for backend_name, backend in store.backends.items():
        if not isinstance(backend, PostgreSQL):
            cli_message(f"Skipping '{backend_name}' backend, it's not PostgreSQL backend")
            continue

        with backend.begin() as connection:
            rows = connection.execute(FOREIGN_KEY_METADATA_QUERY).mappings()
            rows = list(rows)

        tables: dict[TableKey, ReferenceTable] = {}
        foreign_keys: set[ForeignKey] = set()
        for row in rows:
            source = _table_from_row(row, "source")
            target = _table_from_row(row, "target")
            tables[source.key] = source
            tables[target.key] = target
            foreign_keys.add(ForeignKey(source=source.key, target=target.key, is_cross_schema=row["is_cross_schema"]))

        candidate_keys = _reference_table_closure(tables, foreign_keys)
        eligible_tables = {
            table_key: replace(
                table,
                eligible=table_key in candidate_keys and _is_eligible_reference_table(context, store, table, max_size),
            )
            for table_key, table in tables.items()
        }

        for table in plan_reference_tables(eligible_tables, foreign_keys):
            model_name = table.comment or table.qualified_name
            config_models[model_name] = {"distribute": "copy"}

    output_stream = nullcontext(sys.stdout) if output_path is None else output_path.open("w")
    with output_stream as out:
        yaml.dump(config_output, out)
