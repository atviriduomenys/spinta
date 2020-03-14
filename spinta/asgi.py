import logging

from spinta.api import init
from spinta.components import Config, Store
from spinta.config import create_context
from spinta import commands


logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s',
)

context = create_context()

rc = context.get('config.raw')

commands.load(context, config, rc)
commands.check(context, config)
commands.load(context, store, rc)
commands.check(context, store)
commands.wait(context, store, rc)
commands.prepare(context, store.internal)
commands.prepare(context, store)

app = init(context)
