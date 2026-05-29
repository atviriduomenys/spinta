import dataclasses
from collections import defaultdict
from copy import deepcopy

import sqlalchemy as sa
from tqdm import tqdm
from multipledispatch import dispatch

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
from spinta.components import Context, Model
from spinta.exceptions import NotImplementedFeature


@dataclasses.dataclass
class ShardingPlan:
    schemas: set[str] = dataclasses.field(default_factory=set)
    references: set[TableIdentifier] = dataclasses.field(default_factory=set)
    distributed: dict[TableIdentifier, str] = dataclasses.field(default_factory=dict)
    local: set[TableIdentifier] = dataclasses.field(default_factory=set)

    _lookup: dict[TableIdentifier | str, DistributionType] = dataclasses.field(init=False, default_factory=dict)

    def empty(self) -> bool:
        return not (self.schemas or self.references or self.distributed)

    def __sub__(self, other) -> "ShardingPlan":
        return ShardingPlan(
            schemas=self.schemas - other.schemas,
            references=self.references - other.references,
            distributed=dict(self.distributed.items() - other.distributed.items()),
            local=self.local - other.local,
        )

    def __post_init__(self) -> None:
        for schema in self.schemas:
            self._lookup[schema] = DistributionType.SCHEMA

        for table_identifier in self.distributed.keys():
            self._lookup[table_identifier] = DistributionType.TABLE

        for table_identifier in self.references:
            self._lookup[table_identifier] = DistributionType.COPY

        for table_identifier in self.local:
            self._lookup[table_identifier] = DistributionType.UNDISTRIBUTED

    def distribution_type(self, key: TableIdentifier | str) -> DistributionType | None:
        return self._lookup.get(key, None)

    def discard(self, key: TableIdentifier | str) -> None:
        distribution_type = self._lookup.pop(key, None)
        if distribution_type is None:
            return

        match distribution_type:
            case DistributionType.SCHEMA:
                self.schemas.discard(key)
            case DistributionType.TABLE:
                self.distributed.pop(key)
            case DistributionType.COPY:
                self.references.discard(key)
            case DistributionType.UNDISTRIBUTED:
                self.local.discard(key)


def _generate_current_distribution_query(schemas: list[str] | None = None) -> (str, dict):
    base_query = """
    SELECT
        n.nspname AS schema_name,
        c.relname AS table_name,
        format('%I.%I', n.nspname, c.relname) AS full_table_name,
        d.description AS table_comment,
        COALESCE(ct.citus_table_type, 'local') AS distribution_type,
        ct.distribution_column
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN citus_tables ct ON ct.table_name = c.oid::regclass
    LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = 0
    WHERE c.relkind = 'r'
      AND n.nspname NOT LIKE 'pg_%'
      AND n.nspname <> 'information_schema'
    """

    params = {}
    if schemas:
        base_query += " AND n.nspname = ANY(:schemas)"
        params["schemas"] = schemas

    base_query += " ORDER BY n.nspname, c.relname"
    return base_query, params


@dispatch(Context)
def gather_current_sharding_plan(context: Context, **kwargs) -> dict[str, ShardingPlan]:
    store = context.get("store")
    backends = store.backends

    plans: dict[str, ShardingPlan] = defaultdict(ShardingPlan)
    for backend_name, backend in backends.items():
        plans[backend_name] = gather_current_sharding_plan(context, backend, **kwargs)
    return plans


@dispatch(Context, Backend)
def gather_current_sharding_plan(context: Context, backend: Backend, **kwargs) -> ShardingPlan:
    # Currently, only postgresql backend supports citus distribution, instead of erroring, return empty plan.
    return ShardingPlan()


@dispatch(Context, PostgreSQL)
def gather_current_sharding_plan(
    context: Context, backend: PostgreSQL, schemas: list[str] | None = None, **kwargs
) -> ShardingPlan:
    plan = ShardingPlan()
    with backend.begin() as conn:
        query, params = _generate_current_distribution_query(schemas)
        rows = conn.execute(sa.text(query), params).fetchall()

        for row in rows:
            if not row["table_comment"]:
                continue

            table_identifier = get_table_identifier(row["table_comment"])
            match row["distribution_type"]:
                case "schema":
                    plan.schemas.add(row["schema_name"])
                case "distributed":
                    plan.distributed[table_identifier] = row["distribution_column"]
                case "reference":
                    plan.references.add(table_identifier)
                case _:
                    plan.local.add(table_identifier)
    return plan


def create_sharding_plan(context: Context, models: list[Model], **kwargs) -> dict[str, ShardingPlan]:
    plans = defaultdict(ShardingPlan)
    for model in models:
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
                plan.distributed[table_identifier] = distribution_strategy.property
            case DistributionType.COPY:
                plan.references.add(table_identifier)
            case _:
                plan.local.add(table_identifier)

    return plans


def invalidate_default_distribution(
    context: Context, backend: PostgreSQL, plan: ShardingPlan, verbose: bool = False, **kwargs
) -> ShardingPlan:
    default_distribution = context.get("config").default_distribution_strategy
    if not default_distribution:
        return plan

    match default_distribution.distribution_type:
        case DistributionType.SCHEMA:
            return invalidate_default_schema_distributions(context, backend, plan)
        case _:
            if verbose:
                cli_message(
                    f"Skipped invalidation of default distribution for {default_distribution.distribution_type.value} type"
                )

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


def _build_fk_graph(conn: sa.engine.Connection) -> dict[tuple[str, str], set[tuple[str, str]]]:
    graph: dict[tuple[str, str], set[tuple[str, str]]] = defaultdict(set)

    rows = conn.execute("""
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

    for src_schema, src_table, tgt_schema, tgt_table in rows.fetchall():
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

    return key_to_component


def build_fk_components(
    conn: sa.engine.Connection, tables: set[TableIdentifier]
) -> dict[TableIdentifier, set[TableIdentifier]]:
    """
    Build FK-connected components using FULL DB graph,
    then return mapping only for given `tables`.
    """

    target_map = {(t.pg_schema_name, t.pg_table_name): t for t in tables}
    graph = _build_fk_graph(conn)

    result = {}
    for key, table in target_map.items():
        full_component = graph.get(key, {key})
        result[table] = {target_map[k] for k in full_component if k in target_map}

    return result


def undistribute_all(
    context: Context,
    backend: PostgreSQL,
    plan: ShardingPlan,
    handler: MigrationHandler,
    progress_bar: tqdm | None = None,
    **kwargs,
) -> None:
    # Adding sorting for test reproducibility, since the order for mass undistribution does not matter
    for schema in sorted(plan.schemas):
        handler.add_action(UndistributeSchema(schema_name=schema))
        if progress_bar is not None:
            progress_bar.update(1)

    if not (plan.references or plan.distributed):
        return

    processed = set()
    undistributed_tables = plan.references | set(table for table in plan.distributed.keys())
    with backend.begin() as conn:
        component_map = build_fk_components(conn, undistributed_tables)

    for table in sorted(plan.distributed.keys()):
        if table in processed:
            continue

        handler.add_action(UndistributeTable(table_identifier=table))
        if progress_bar is not None:
            progress_bar.update(1)
        component = component_map[table]
        processed.update(component)

    for table in sorted(plan.references):
        if table in processed:
            continue

        handler.add_action(UndistributeTable(table_identifier=table))
        if progress_bar is not None:
            progress_bar.update(1)
        component = component_map[table]
        processed.update(component)


def distribute_all(
    context: Context,
    backend: PostgreSQL,
    plan: ShardingPlan,
    handler: MigrationHandler,
    progress_bar: tqdm | None = None,
    **kwargs,
) -> None:
    # Adding sorting for test reproducibility, since the order for mass distribution does not matter
    for table in sorted(plan.references):
        handler.add_action(DistributeReference(table_identifier=table))
        if progress_bar is not None:
            progress_bar.update(1)

    for table, column in sorted(plan.distributed.items()):
        handler.add_action(DistributeTable(table_identifier=table, column=column))
        if progress_bar is not None:
            progress_bar.update(1)

    for schema in sorted(plan.schemas):
        handler.add_action(DistributeSchema(schema_name=schema))
        if progress_bar is not None:
            progress_bar.update(1)
