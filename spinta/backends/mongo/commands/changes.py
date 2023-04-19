import datetime
import types

from spinta import commands
from spinta.utils.json import fix_data_for_json
from spinta.components import Context, Model, Property
from spinta.backends.mongo.components import Mongo


@commands.create_changelog_entry.register(Context, (Model, Property), Mongo)
async def create_changelog_entry(
    context: Context,
    model: (Model, Property),
    backend: Mongo,
    *,
    dstream: types.AsyncGeneratorType,
) -> None:
    transaction = context.get('transaction')
    if isinstance(model, Model):
        table = backend.db[model.model_type() + '__changelog']
    else:
        table = backend.db[model.model.model_type() + '__changelog']
    async for data in dstream:

        if not data.patch:
            yield data
            continue

        table.insert_one({
            '__id': data.saved['_id'] if data.saved else data.patch['_id'],
            '_revision': data.patch['_revision'] if data.patch else data.saved['_revision'],
            '_op': data.action.value,
            '_transaction': transaction.id,
            '_created': datetime.datetime.now(datetime.timezone.utc),
            **fix_data_for_json({
                k: v for k, v in data.patch.items() if not k.startswith('_')
            }),
        })
        yield data
