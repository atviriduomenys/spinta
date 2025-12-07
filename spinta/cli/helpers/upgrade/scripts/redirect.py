from multipledispatch import dispatch

from typer import echo
from collections.abc import Iterator
import tqdm
import sqlalchemy as sa

from spinta import commands
from spinta.backends import Backend
from spinta.backends.constants import TableType
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.name import get_pg_table_name
from spinta.cli.helpers.script.helpers import ensure_store_is_loaded
from spinta.components import Context, Model


@dispatch(Model)
def _migrate_redirect(model: Model):
    _migrate_redirect(model.backend, model)


@dispatch(Backend, Model)
def _migrate_redirect(backend: Backend, model: Model):
    raise NotImplementedError()


@dispatch(PostgreSQL, Model)
def _migrate_redirect(backend: PostgreSQL, model: Model):
    with backend.begin() as conn:
        table = backend.get_table(model, TableType.REDIRECT)
        backend.schema.create_all(conn, tables=[table])


@dispatch(Model)
def _validate_redirect_implementation(model: Model):
    return _validate_redirect_implementation(model.backend, model)


@dispatch(Backend, Model)
def _validate_redirect_implementation(backend: PostgreSQL, model: Model):
    # Skips validation
    return True


@dispatch(PostgreSQL, Model)
def _validate_redirect_implementation(backend: PostgreSQL, model: Model):
    table_name = get_pg_table_name(model, TableType.REDIRECT)
    insp = sa.inspect(backend.engine)
    return insp.has_table(table_name)


def models_missing_redirect(context: Context, **kwargs) -> Iterator[Model]:
    store = context.get("store")
    manifest = store.manifest
    for model in commands.get_models(context, manifest).values():
        if model.model_type().startswith("_"):
            continue

        valid = _validate_redirect_implementation(model)
        if not valid:
            yield model


def cli_requires_redirect_migration(context: Context, **kwargs) -> bool:
    ensure_store_is_loaded(context)
    missing_models = list(models_missing_redirect(context, **kwargs))
    model_count = len(missing_models)
    return model_count > 0


def migrate_redirect(context: Context, **kwargs):
    ensure_store_is_loaded(context)
    missing_models = list(models_missing_redirect(context, **kwargs))
    model_count = len(missing_models)

    if model_count == 0:
        return

    echo(f"Found {model_count} models that does not support REDIRECT feature.")
    counter = tqdm.tqdm(desc="MIGRATING REDIRECT MODELS", ascii=True, total=model_count)
    try:
        for model in missing_models:
            counter.write(f"\t{model.model_type()}")
            _migrate_redirect(model)
            counter.update(1)
    finally:
        counter.close()
