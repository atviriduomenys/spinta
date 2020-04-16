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
commands.load(context, config)
commands.check(context, config)

store = context.get('store')
commands.load(context, store)

commands.load(context, store.manifest)
commands.link(context, store.manifest)
commands.check(context, store.manifest)

commands.wait(context, store)

commands.prepare(context, store.manifest)

app = init(context)
