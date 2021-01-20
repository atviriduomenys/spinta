from typing import Dict, Any

import pymongo

from spinta import commands
from spinta.components import Context
from spinta.backends.mongo.components import Mongo


@commands.load.register(Context, Mongo, dict)
def load(context: Context, backend: Mongo, config: Dict[str, Any]):
    # Load Mongo client using configuration.
    backend.dsn = config['dsn']
    backend.db_name = config['db']
    backend.client = pymongo.MongoClient(backend.dsn)
    backend.db = backend.client[backend.db_name]
