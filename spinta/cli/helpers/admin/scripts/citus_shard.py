import dataclasses
from collections import defaultdict
from copy import deepcopy

import sqlalchemy as sa
from multipledispatch import dispatch
from tqdm import tqdm

from spinta import commands
from spinta.backends import Backend
from spinta.backends.constants import DistributionType
from spinta.backends.helpers import TableIdentifier
from spinta.backends.helpers import get_table_identifier
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.migrate.actions import (
    MigrationHandler,
    UndistributeSchema,
    UndistributeTable,
    DistributeReference,
    DistributeTable,
    DistributeSchema,
)
from spinta.cli.helpers.message import cli_message
from spinta.cli.helpers.script.helpers import ensure_store_is_loaded
from spinta.cli.helpers.upgrade.scripts.backends.postgresql.comments import migrate_comments
from spinta.components import Context
from spinta.exceptions import NotImplementedFeature


@dataclasses.dataclass
class ShardingPlan:
    schemas: set[str] = dataclasses.field(default_factory=set)
    references: set[TableIdentifier] = dataclasses.field(default_factory=set)
    tables: set[tuple[TableIdentifier, str]] = dataclasses.field(default_factory=set)

    def __sub__(self, other):
        return ShardingPlan(
            schemas=self.schemas - other.schemas,
            references=self.references - other.references,
            tables=self.tables - other.tables,
        )


@dispatch(Context)
def gather_current_sharding_state(context: Context, verbose: bool = True, **kwargs):
    store = context.get("store")
    backends = store.backends

    if verbose:
        cli_message("Extracting already distributed schemas")

    plans: dict[str, ShardingPlan] = defaultdict(ShardingPlan)
    for backend_name, backend in backends.items():
        plans[backend_name] = gather_current_sharding_state(context, backend, verbose=verbose, **kwargs)
    return plans


@dispatch(Context, Backend)
def gather_current_sharding_state(context: Context, backend: Backend, **kwargs):
    # Currently, only postgresql backend supports citus distribution, instead of erroring, return empty plan.
    return ShardingPlan()


@dispatch(Context, PostgreSQL)
def gather_current_sharding_state(context: Context, backend: PostgreSQL, verbose: bool = True, **kwargs):
    plan = ShardingPlan()
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
            if not row["table_comment"]:
                continue

            table_identifier = get_table_identifier(row["table_comment"])
            match row["distribution_type"]:
                case "schema":
                    plan.schemas.add(row["schema_name"])
                case "distributed":
                    plan.tables.add((table_identifier, row["distribution_column"]))
                case "reference":
                    plan.references.add(table_identifier)
                case _:
                    pass
    return plan


def create_sharding_plan(context: Context, verbose: bool = True, **kwargs) -> dict[str, ShardingPlan]:
    store = context.get("store")
    if verbose:
        cli_message("Creating distribution plan from manifest")

    plans = defaultdict(ShardingPlan)
    for model in commands.get_models(context, store.manifest).values():
        if not isinstance(model.backend, PostgreSQL):
            continue

        if not model.external or not model.external.dataset:
            continue

        distribution_strategy = model.distribution_strategy
        plan = plans[model.backend.name]
        table_identifier = get_table_identifier(model)
        match distribution_strategy.distribution_type:
            case DistributionType.SCHEMA:
                plan.schemas.add(table_identifier.pg_schema_name)
            case DistributionType.TABLE:
                plan.tables.add((table_identifier, distribution_strategy.property))
            case DistributionType.COPY:
                plan.references.add(table_identifier)
            case _:
                pass

    return plans


def invalidate_default_distribution(
    context: Context, backend: PostgreSQL, plan: ShardingPlan, verbose: bool = True, **kwargs
) -> ShardingPlan:
    if verbose:
        cli_message("Invalidating default distribution")

    default_distribution = context.get("config").default_distribution_strategy
    if not default_distribution:
        return plan

    match default_distribution.distribution_type:
        case DistributionType.SCHEMA:
            return invalidate_default_schema_distributions(context, backend, plan)
        case _:
            return plan


def valid_schema_distribution_foreign_key(plan: ShardingPlan, schema: str, foreign_key: dict) -> bool:
    if foreign_key["referred_schema"] == schema:
        return True

    for reference in plan.references:
        if (
            reference.pg_schema_name == foreign_key["referred_schema"]
            and reference.pg_table_name == foreign_key["referred_table"]
        ):
            return True

    return False


@dispatch(Context, Backend, ShardingPlan)
def invalidate_default_schema_distributions(
    context: Context, backend: Backend, plan: ShardingPlan, **kwargs
) -> ShardingPlan:
    raise NotImplementedFeature(f"Ability to invalidate default schema distribution for {backend.type!r} backend type")


@dispatch(Context, PostgreSQL, ShardingPlan)
def invalidate_default_schema_distributions(
    context: Context, backend: PostgreSQL, plan: ShardingPlan, **kwargs
) -> ShardingPlan:
    if not plan.schemas:
        return plan

    invalid_schemas = set()

    inspector = sa.inspect(backend.engine)

    plan_copy = deepcopy(plan)
    for schema in plan.schemas:
        tables = inspector.get_table_names(schema=schema)

        for table in tables:
            foreign_keys = inspector.get_foreign_keys(table, schema=schema)
            if not foreign_keys:
                continue

            for key in foreign_keys:
                if not valid_schema_distribution_foreign_key(plan, schema, key):
                    invalid_schemas.add(key["referred_schema"])
                    invalid_schemas.add(schema)

    if not invalid_schemas:
        return plan

    for invalid_schema in invalid_schemas:
        plan_copy.schemas.discard(invalid_schema)

    return plan_copy


@dispatch(Context, Backend, ShardingPlan, ShardingPlan, MigrationHandler)
def generate_citus_migrations(
    context: Context,
    backend: Backend,
    existing_plan: ShardingPlan,
    new_plan: ShardingPlan,
    handler: MigrationHandler,
    **kwargs,
) -> None:
    raise NotImplementedFeature(f"Ability to generate citus migrations for {backend.type!r} backend type")


@dispatch(Context, PostgreSQL, ShardingPlan, ShardingPlan, MigrationHandler)
def generate_citus_migrations(
    context: Context,
    backend: PostgreSQL,
    existing_plan: ShardingPlan,
    new_plan: ShardingPlan,
    handler: MigrationHandler,
    **kwargs,
) -> None:
    undistributed_plan = existing_plan - new_plan
    diff_plan = new_plan - existing_plan

    progress_bar = tqdm(desc="Generating citus migrations")

    for schema in undistributed_plan.schemas:
        handler.add_action(UndistributeSchema(schema_name=schema))
        progress_bar.update(1)

    if undistributed_plan.references or undistributed_plan.tables:
        component_map = {}
        processed = set()
        undistributed_tables = undistributed_plan.references | set(table for table, _ in undistributed_plan.tables)
        with backend.begin() as conn:
            component_map = build_fk_components(conn, undistributed_tables)

        for table, _ in undistributed_plan.tables:
            if table in processed:
                continue

            handler.add_action(UndistributeTable(table_identifier=table))
            progress_bar.update(1)
            component = component_map[table]
            processed.update(component)

        for table in undistributed_plan.references:
            if table in processed:
                continue

            handler.add_action(UndistributeTable(table_identifier=table))
            progress_bar.update(1)
            component = component_map[table]
            processed.update(component)

    for table in diff_plan.references:
        handler.add_action(DistributeReference(table_identifier=table))
        progress_bar.update(1)

    for table, column in diff_plan.tables:
        handler.add_action(DistributeTable(table_identifier=table, column=column))
        progress_bar.update(1)

    for schema in diff_plan.schemas:
        handler.add_action(DistributeSchema(schema_name=schema))
        progress_bar.update(1)


def migrate_citus_distributions(context: Context, destructive: bool, **kwargs) -> None:
    from alembic.operations import Operations
    from alembic.runtime.migration import MigrationContext

    store = ensure_store_is_loaded(context)
    required_plan = create_sharding_plan(context, verbose=True)

    sharded_plan = gather_current_sharding_state(context, verbose=True)
    backends = store.backends
    for backend_name, plan in required_plan.items():
        backend = backends[backend_name]
        if not isinstance(backend, PostgreSQL):
            cli_message(f"Skipping '{backend_name}' backend, it's not PostgreSQL backend")
            continue

        validated_plan = invalidate_default_distribution(context, backend, plan, verbose=True)
        handler = MigrationHandler()
        generate_citus_migrations(context, backend, sharded_plan[backend_name], validated_plan, handler)
        progress_bar = tqdm(desc=f"Updating distributions for '{backend_name}' backend", total=handler.count())
        with backend.begin() as conn:
            ctx = MigrationContext.configure(conn)
            operations = Operations(ctx)
            for migration in handler.gather_migrations():
                migration.execute(operations)
                progress_bar.update(1)
        progress_bar.close()
    cli_message("Reapplying removed comments")
    migrate_comments(context, verbose=False)


def cli_requires_citus_distribution(context: Context, **kwargs) -> bool:
    store = ensure_store_is_loaded(context)
    required_plans = create_sharding_plan(context, verbose=False)

    existing_plans = gather_current_sharding_state(context, verbose=False)
    backends = store.backends
    for backend_name, required_plan in required_plans.items():
        backend = backends[backend_name]
        existing_plan = existing_plans[backend_name]
        validated_plan = invalidate_default_distribution(context, backend, required_plan, verbose=False)
        diff_plan = existing_plan - validated_plan
        if diff_plan.tables or diff_plan.references or diff_plan.schemas:
            return True

        req_plan = validated_plan - existing_plan
        if req_plan.tables or req_plan.references or req_plan.schemas:
            return True
    return False


def build_fk_components(conn, tables: set[TableIdentifier]) -> dict[TableIdentifier, set[TableIdentifier]]:
    """
    Build FK-connected components using FULL DB graph,
    then return mapping only for given `tables`.
    """

    target_map = {(t.pg_schema_name, t.pg_table_name): t for t in tables}
    graph: dict[tuple[str, str], set[tuple[str, str]]] = defaultdict(set)

    result = conn.execute("""
        SELECT
            src_ns.nspname AS source_schema,
            src.relname    AS source_table,
            tgt_ns.nspname AS target_schema,
            tgt.relname    AS target_table
        FROM pg_constraint con
        JOIN pg_class src
            ON src.oid = con.conrelid
        JOIN pg_namespace src_ns
            ON src_ns.oid = src.relnamespace
        JOIN pg_class tgt
            ON tgt.oid = con.confrelid
        JOIN pg_namespace tgt_ns
            ON tgt_ns.oid = tgt.relnamespace
        WHERE con.contype = 'f'
    """)

    for src_schema, src_table, tgt_schema, tgt_table in result.fetchall():
        src = (src_schema, src_table)
        tgt = (tgt_schema, tgt_table)

        if src == tgt:
            continue

        graph[src].add(tgt)
        graph[tgt].add(src)

    visited: set[tuple[str, str]] = set()
    key_to_component: dict[tuple[str, str], set[tuple[str, str]]] = {}

    for node in graph:
        if node in visited:
            continue

        stack = [node]
        component: set[tuple[str, str]] = set()

        while stack:
            cur = stack.pop()
            if cur in visited:
                continue

            visited.add(cur)
            component.add(cur)
            stack.extend(graph[cur] - visited)

        for n in component:
            key_to_component[n] = component

    result: dict[TableIdentifier, set[TableIdentifier]] = {}

    for key, table in target_map.items():
        full_component = key_to_component.get(key, {key})
        result[table] = {target_map[k] for k in full_component if k in target_map}

    return result
