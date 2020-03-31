from typing import AsyncIterator


from spinta import commands
from spinta.utils.data import take
from spinta.components import Context, Model, DataItem, DataSubItem
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.sqlalchemy import utcnow


@commands.insert.register(Context, Model, PostgreSQL)
async def insert(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    dstream: AsyncIterator[DataItem],
    stop_on_error: bool = True,
):
    transaction = context.get('transaction')
    connection = transaction.connection
    table = backend.get_table(model)
    async for data in dstream:
        patch = commands.before_write(context, model, backend, data=data)

        # TODO: Refactor this to insert batches with single query.
        qry = table.insert().values(
            _id=patch['_id'],
            _revision=patch['_revision'],
            _txn=transaction.id,
            _created=utcnow(),
        )
        connection.execute(qry, patch)

        commands.after_write(context, model, backend, data=data)

        yield data


@commands.update.register(Context, Model, PostgreSQL)
async def update(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    dstream: dict,
    stop_on_error: bool = True,
):
    transaction = context.get('transaction')
    connection = transaction.connection
    table = backend.get_table(model)

    async for data in dstream:
        if not data.patch:
            yield data
            continue

        pk = data.saved['_id']
        patch = commands.before_write(context, model, backend, data=data)
        result = connection.execute(
            table.update().
            where(table.c._id == pk).
            where(table.c._revision == data.saved['_revision']).
            values(patch)
        )

        if result.rowcount == 0:
            raise Exception(f"Update failed, {model} with {pk} not found.")
        elif result.rowcount > 1:
            raise Exception(
                f"Update failed, {model} with {pk} has found and update "
                f"{result.rowcount} rows."
            )

        commands.after_write(context, model, backend, data=data)

        yield data


@commands.delete.register(Context, Model, PostgreSQL)
async def delete(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    dstream: AsyncIterator[DataItem],
    stop_on_error: bool = True,
):
    transaction = context.get('transaction')
    connection = transaction.connection
    table = backend.get_table(model)
    async for data in dstream:
        commands.before_write(context, model, backend, data=data)
        connection.execute(
            table.delete().
            where(table.c._id == data.saved['_id'])
        )
        commands.after_write(context, model, backend, data=data)
        yield data


@commands.before_write.register(Context, Model, PostgreSQL)
def before_write(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    data: DataSubItem,
) -> dict:
    patch = take(['_id'], data.patch)
    patch['_revision'] = take('_revision', data.patch, data.saved)
    patch['_txn'] = context.get('transaction').id
    patch['_created'] = utcnow()
    for prop in take(model.properties).values():
        value = commands.before_write(
            context,
            prop.dtype,
            backend,
            data=data[prop.name],
        )
        patch.update(value)
    return patch


@commands.after_write.register(Context, Model, PostgreSQL)
def after_write(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    data: DataSubItem,
) -> dict:
    for key in take(data.patch or {}):
        prop = model.properties[key]
        commands.after_write(context, prop.dtype, backend, data=data[key])
