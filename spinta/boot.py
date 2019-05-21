import logging

from spinta.api import set_context
from spinta.commands import load, wait, prepare, check
from spinta.components import Context, Store
from spinta.utils.commands import load_commands
from spinta import components
from spinta.config import RawConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s',
)


c = RawConfig()
c.read()

load_commands(c.get('commands', 'modules', cast=list))

context = Context()
config = context.set('config', components.Config())
store = context.set('store', Store())

load(context, config, c)
load(context, store, c)
check(context, store)

wait(context, store, c)

prepare(context, store.internal)
prepare(context, store)

set_context(context)
