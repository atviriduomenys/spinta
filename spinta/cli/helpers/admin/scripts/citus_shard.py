from multipledispatch import dispatch
from tqdm import tqdm

from spinta import commands
from spinta.backends import Backend
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.migrate.actions import (
    MigrationHandler,
)
from spinta.backends.postgresql.helpers.migrate.citus import (
    ShardingPlan,
    create_sharding_plan,
    distribute_all,
    gather_current_sharding_plan,
    invalidate_default_distribution,
    undistribute_all,
)
from spinta.cli.helpers.message import cli_message
from spinta.cli.helpers.script.helpers import ensure_store_is_loaded
from spinta.cli.helpers.upgrade.scripts.backends.postgresql.comments import migrate_comments
from spinta.components import Context
from spinta.exceptions import NotImplementedFeature


def _gather_current_sharding_plan(context: Context, verbose: bool = True, **kwargs) -> ShardingPlan:
    if verbose:
        cli_message("Extracting already distributed schemas")

    return gather_current_sharding_plan(context, **kwargs)


def _create_sharding_plan(context: Context, verbose: bool = True, **kwargs) -> dict[str, ShardingPlan]:
    if verbose:
        cli_message("Creating distribution plan from manifest")

    store = context.get("store")
    models = commands.get_models(context, store.manifest).values()

    return create_sharding_plan(context, models, **kwargs)


def _invalidate_default_distribution(
    context: Context, backend: PostgreSQL, plan: ShardingPlan, verbose: bool = True, **kwargs
) -> ShardingPlan:
    if verbose:
        cli_message("Invalidating default distribution")

    return invalidate_default_distribution(context, backend, plan, verbose=verbose, **kwargs)


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

    with tqdm(desc="Generating citus migrations") as progress_bar:
        undistribute_all(context, backend, undistributed_plan, handler, progress_bar)
        distribute_all(context, backend, diff_plan, handler, progress_bar)


def migrate_citus_distributions(context: Context, destructive: bool, **kwargs) -> None:
    from alembic.operations import Operations
    from alembic.runtime.migration import MigrationContext

    store = ensure_store_is_loaded(context)
    required_plan = _create_sharding_plan(context, verbose=True)

    sharded_plan = _gather_current_sharding_plan(context, verbose=True)
    backends = store.backends
    for backend_name, plan in required_plan.items():
        backend = backends[backend_name]
        if not isinstance(backend, PostgreSQL):
            cli_message(f"Skipping '{backend_name}' backend, it's not PostgreSQL backend")
            continue

        validated_plan = _invalidate_default_distribution(context, backend, plan, verbose=True)
        handler = MigrationHandler()
        generate_citus_migrations(context, backend, sharded_plan[backend_name], validated_plan, handler)
        with tqdm(desc=f"Updating distributions for '{backend_name}' backend", total=handler.count()) as progress_bar:
            with backend.begin() as conn:
                ctx = MigrationContext.configure(conn)
                operations = Operations(ctx)
                for migration in handler.gather_migrations():
                    migration.execute(operations)
                    progress_bar.update(1)
    cli_message("Reapplying removed comments")
    migrate_comments(context, verbose=False)


def cli_requires_citus_distribution(context: Context, **kwargs) -> bool:
    store = ensure_store_is_loaded(context)
    required_plans = _create_sharding_plan(context, verbose=False)

    existing_plans = _gather_current_sharding_plan(context, verbose=False)
    backends = store.backends
    for backend_name, required_plan in required_plans.items():
        backend = backends[backend_name]
        existing_plan = existing_plans[backend_name]
        validated_plan = _invalidate_default_distribution(context, backend, required_plan, verbose=False)
        diff_plan = existing_plan - validated_plan
        if diff_plan.distributed or diff_plan.references or diff_plan.schemas:
            return True

        req_plan = validated_plan - existing_plan
        if req_plan.distributed or req_plan.references or req_plan.schemas:
            return True
    return False
