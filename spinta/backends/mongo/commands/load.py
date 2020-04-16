import pymongo

from spinta import commands
from spinta.core.config import RawConfig
from spinta.components import Context
from spinta.backends.mongo.components import Mongo


@commands.load.register(Context, Mongo, RawConfig)
def load(context: Context, backend: Mongo, rc: RawConfig):
    # Load Mongo client using configuration.
    backend.dsn = rc.get('backends', backend.name, 'dsn', required=True)
    backend.db_name = rc.get('backends', backend.name, 'db', required=True)

    backend.client = pymongo.MongoClient(backend.dsn)
    backend.db = backend.client[backend.db_name]
