import logging
from collections.abc import Callable
from typing import Any

import sqlalchemy as sa

from spinta import commands
from spinta.components import Context, Model, Property
from spinta.datasets.backends.sql.components import Sql
from spinta.datasets.backends.sql.ufuncs.query.helpers import prepare_temporal_value
from spinta.datasets.components import Attribute
from spinta.datasets.utils import iterparams
from spinta.dimensions.enum.components import EnumItem

log = logging.getLogger(__name__)

# Counts values in a single table, returns `None` if it can not be counted.
CountTable = Callable[[sa.Table], int | None]


@commands.count_rows.register(Context, Sql, Model)
def count_rows(context: Context, backend: Sql, model: Model) -> int | None:
    return _count_tables(
        context,
        backend,
        model,
        lambda table: _scalar(backend, sa.select(sa.func.count()).select_from(table)),
    )


@commands.count_values.register(Context, Sql, Model, Property)
def count_values(context: Context, backend: Sql, model: Model, prop: Property) -> int | None:
    def count(table: sa.Table) -> int | None:
        column = _get_column(table, prop)
        if column is None:
            return None
        return _scalar(backend, sa.select(sa.func.count(column)).select_from(table))

    return _count_tables(context, backend, model, count)


@commands.count_enum_value.register(Context, Sql, Model, Property, EnumItem)
def count_enum_value(context: Context, backend: Sql, model: Model, prop: Property, item: EnumItem) -> int | None:
    def count(table: sa.Table) -> int | None:
        column = _get_column(table, prop)
        if column is None:
            return None
        # Enum `source` is the value stored in the external column, see `prepare_enum_value`.
        value = prepare_temporal_value(column, item.source)
        qry = sa.select(sa.func.count()).select_from(table).where(column == value)
        return _scalar(backend, qry)

    return _count_tables(context, backend, model, count)


def _get_column(table: sa.Table, prop: Property) -> sa.Column | None:
    if isinstance(prop.external, Attribute) and prop.external.name:
        return table.c.get(prop.external.name)
    return None


def _count_tables(context: Context, backend: Sql, model: Model, count: CountTable) -> int | None:
    if not model.external or not model.external.name:
        return None

    total = 0
    try:
        for params in iterparams(context, model, model.manifest):
            table = backend.get_table(model, model.external.name.format(**params))
            value = count(table)
            if value is None:
                return None
            total += value
    except sa.exc.SQLAlchemyError as e:
        log.warning("Can not count %s: %s", model.name, e)
        return None
    return total


def _scalar(backend: Sql, qry: Any) -> int:
    # Use a separate connection for each query, so that a failed query does not
    # abort other counts.
    with backend.engine.connect() as conn:
        return conn.execute(qry).scalar_one()
