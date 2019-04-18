import os

from spinta.api import app, set_context
from spinta.commands import load, prepare, check
from spinta.components import Context, Store
from spinta.utils.commands import load_commands
from spinta import components
from spinta.config import Config


c = Config()
c.set_env(os.environ)
c.add_env_file('.env')

load_commands(c.get('commands', 'modules', cast=list))

context = Context()
config = context.set('config', components.Config())
store = context.set('store', Store())

load(context, config, c)
load(context, store, c)
check(context, store)

prepare(context, store.internal)
prepare(context, store)

set_context(context)

app.debug = config.debug
