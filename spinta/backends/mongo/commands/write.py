import datetime


from spinta import commands
from spinta.types.datatype import File
from spinta.utils.schema import NA
from spinta.utils.data import take
from spinta.components import Context, Action, Model, DataStream, DataItem
from spinta.exceptions import ItemDoesNotExist
from spinta.backends.mongo.components import Mongo


@commands.insert.register()
async def insert(
    context: Context,
    model: Model,
    backend: Mongo,
    *,
    dstream: DataStream,
    stop_on_error: bool = True,
):
    table = backend.db[model.model_type()]
    async for data in dstream:
        patch = commands.before_write(context, model, backend, data=data)
        # TODO: Insert batches in a single query, using `insert_many`.
        table.insert_one(patch)
        commands.after_write(context, model, backend, data=data)
        yield data


@commands.update.register()
async def update(
    context: Context,
    model: Model,
    backend: Mongo,
    *,
    dstream: dict,
    stop_on_error: bool = True,
):
    table = backend.db[model.model_type()]
    async for data in dstream:
        patch = commands.before_write(context, model, backend, data=data)
        result = table.update_one(
            {
                '__id': data.saved['_id'],
                '_revision': data.saved['_revision'],
            },
            {'$set': patch},
        )
        if result.matched_count == 0:
            raise ItemDoesNotExist(
                model,
                id=data.saved['_id'],
                revision=data.saved['_revision'],
            )
        assert result.matched_count == 1 and result.modified_count == 1, (
            f"matched: {result.matched_count}, modified: {result.modified_count}"
        )
        commands.after_write(context, model, backend, data=data)
        yield data


@commands.delete.register()
async def delete(
    context: Context,
    model: Model,
    backend: Mongo,
    *,
    dstream: dict,
    stop_on_error: bool = True,
):
    table = backend.db[model.model_type()]
    async for data in dstream:
        commands.before_write(context, model, backend, data=data)
        result = table.delete_one({
            '__id': data.saved['_id'],
            '_revision': data.saved['_revision'],
        })
        if result.deleted_count == 0:
            # FIXME: Respect stop_on_error flag.
            raise ItemDoesNotExist(
                model,
                id=data.saved['_id'],
                revision=data.saved['_revision'],
            )
        commands.after_write(context, model, backend, data=data)
        yield data


@commands.before_write.register(Context, Model, Mongo)
def before_write(
    context: Context,
    model: Model,
    backend: Mongo,
    *,
    data: DataItem,
) -> dict:
    patch = {}
    patch['__id'] = take('_id', data.patch)
    patch['_revision'] = take('_revision', data.patch, data.saved)
    patch['_txn'] = context.get('transaction').id
    patch['_created'] = datetime.datetime.now()
    if data.action == Action.INSERT or (data.action == Action.UPSERT and data.saved is NA):
        for k, v in take(data.patch).items():
            prop = take(k, model.properties)
            if isinstance(prop.dtype, File):
                v = commands.before_write(
                    context,
                    prop.dtype,
                    backend,
                    data=data[prop.name],
                )
            patch[k] = v
    else:
        for prop in take(model.properties).values():
            value = commands.before_write(
                context,
                prop.dtype,
                backend,
                data=data[prop.name],
            )
            patch.update(value)
    return take(all, patch)


@commands.after_write.register()
def after_write(
    context: Context,
    model: Model,
    backend: Mongo,
    *,
    data: DataItem,
) -> dict:
    pass
