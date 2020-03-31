from spinta import commands
from spinta.components import Context, Model
from spinta.backends.mongo.components import Mongo


@commands.wipe.register()
def wipe(context: Context, model: Model, backend: Mongo):
    table_main = backend.db[model.model_type()]
    table_changelog = backend.db[model.model_type() + '__changelog']
    table_main.delete_many({})
    table_changelog.delete_many({})
