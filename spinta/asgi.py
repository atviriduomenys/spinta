from spinta.api import app, set_context
from spinta.commands import load, prepare, check
from spinta.components import Context, Config, Store
from spinta.config import get_config
from spinta.utils.commands import load_commands


CONFIG = get_config()

load_commands(CONFIG['commands']['modules'])

context = Context()
config = context.set('config', Config())
store = context.set('store', Store())

load(context, config, CONFIG)
load(context, store, config)
check(context, store)

prepare(context, store.internal)
prepare(context, store)

set_context(context)

app.debug = store.config.debug
