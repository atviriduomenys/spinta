from typing import Union, List, Tuple

import itertools

from spinta import commands
from spinta.utils.schema import NA
from spinta.utils.data import take
from spinta.components import Context, Action, Property
from spinta.types.datatype import DataType, Object, Array
from spinta.components import Context, Action, Model, DataSubItem
from spinta.backends.constants import TableType
from spinta.backends.postgresql.components import PostgreSQL


@commands.before_write.register(Context, Array, PostgreSQL)
def before_write(
    context: Context,
    dtype: Array,
    backend: PostgreSQL,
    *,
    data: DataSubItem,
):
    if data.saved and data.patch is not NA:
        prop = dtype.prop
        table = backend.get_table(prop, TableType.LIST)
        transaction = context.get('transaction')
        connection = transaction.connection
        connection.execute(
            table.delete().
            where(table.c._rid == data.root.saved['_id'])
        )

    if dtype.prop.list:
        return {}
    else:
        return take({dtype.prop.place: data.patch})


@commands.after_write.register(Context, Array, PostgreSQL)
def after_write(
    context: Context,
    dtype: Array,
    backend: PostgreSQL,
    *,
    data: DataSubItem,
):
    if data.patch:
        prop = dtype.prop
        table = backend.get_table(prop, TableType.LIST)
        transaction = context.get('transaction')
        connection = transaction.connection
        rid = take('_id', data.root.patch, data.root.saved)
        rows = [
            {
                _get_list_column_name(prop.place, k): v
                for k, v in commands.before_write(
                    context,
                    dtype.items.dtype,
                    backend,
                    data=d,
                ).items()
            }
            for d in data.iter(patch=True)
        ]
        qry = table.insert().values({
            '_txn': transaction.id,
            '_rid': rid,
        })
        connection.execute(qry, rows)

        for d in data.iter(patch=True):
            commands.after_write(context, dtype.items.dtype, backend, data=d)


def _update_lists_table(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    action: Action,
    pk: str,
    patch: dict,
) -> None:
    transaction = context.get('transaction')
    connection = transaction.connection
    rows = _get_lists_data(model, patch)
    sort_key = lambda x: x[0].place  # noqa
    rows = sorted(rows, key=sort_key)
    for place, rows in itertools.groupby(rows, key=sort_key):
        prop = model.flatprops[place]
        table = backend.get_table(prop, TableType.LIST)
        if action != Action.INSERT:
            connection.execute(table.delete().where(table.c._rid == pk))
        rows = [
            {
                '_txn': transaction.id,
                '_rid': pk,
                **{
                    _get_list_column_name(place, k): v
                    for k, v in row.items()
                }
            }
            for prop, row in rows
        ]
        connection.execute(table.insert(), rows)


def _get_list_column_name(place, name):
    if place == name:
        return place.split('.')[-1]
    else:
        return name[len(place) + 1:]


def _get_lists_data(
    dtype: Union[Model, DataType],
    value: object,
) -> List[dict]:
    data, lists = _separate_lists_and_data(dtype, value)
    if isinstance(dtype, DataType) and data is not NA:
        yield dtype.prop, data
    for prop, vals in lists:
        for v in vals:
            yield from _get_lists_data(prop.dtype, v)


def _separate_lists_and_data(
    dtype: Union[Model, DataType],
    value: object,
) -> Tuple[dict, List[Tuple[Property, list]]]:
    if isinstance(dtype, (Model, Object)):
        data = {}
        lists = []
        for k, v in (value or {}).items():
            prop = dtype.properties[k]
            v, more = _separate_lists_and_data(prop.dtype, v)
            if v is not NA:
                data.update(v)
            if more:
                lists += more
        return data or NA, lists
    elif isinstance(dtype, Array):
        if value:
            return NA, [(dtype.items, value)]
        else:
            return NA, []
    else:
        return {dtype.prop.place: value}, []
