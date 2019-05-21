import logging

from spinta.api import app, set_context
from spinta.commands import load, wait, prepare, check
from spinta.components import Store
from spinta.utils.commands import load_commands
from spinta import components
from spinta.config import RawConfig
from spinta.auth import AuthorizationServer, ResourceProtector, BearerTokenValidator
from spinta.utils.imports import importstr

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s',
)


c = RawConfig()
c.read()

load_commands(c.get('commands', 'modules', cast=list))

Context = c.get('components', 'core', 'context', cast=importstr)
context = Context()
config = context.set('config', components.Config())
store = context.set('store', Store())

load(context, config, c)
check(context, config)
load(context, store, c)
check(context, store)

wait(context, store, c)

prepare(context, store.internal)
prepare(context, store)

context.set('auth.server', AuthorizationServer(context))
context.set('auth.resource_protector', ResourceProtector(context, BearerTokenValidator))

set_context(context)

app.debug = config.debug
