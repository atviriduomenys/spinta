import logging

from spinta.api import init
from spinta.core.context import create_context
from spinta import commands


logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s',
)

context = create_context('asgi')

rc = context.get('rc')
config = context.get('config')
commands.load(context, config, rc)
commands.check(context, config)

store = context.get('store')
commands.load(context, store, config)
commands.check(context, store)

commands.wait(context, store, rc)

commands.prepare(context, store)

app = init(context)
