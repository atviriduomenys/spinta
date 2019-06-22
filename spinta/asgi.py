import logging

from spinta.api import app, set_context
from spinta.commands import load, wait, prepare, check
from spinta.components import Store
from spinta import components
from spinta.auth import AuthorizationServer, ResourceProtector, BearerTokenValidator
from spinta.config import create_context

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s',
)


context = create_context()
rc = context.get('config.raw')
config = context.set('config', components.Config())
store = context.set('store', Store())

load(context, config, rc)
check(context, config)
load(context, store, rc)
check(context, store)

wait(context, store, rc)

prepare(context, store.internal)
prepare(context, store)

context.set('auth.server', AuthorizationServer(context))
context.set('auth.resource_protector', ResourceProtector(context, BearerTokenValidator))

set_context(context)

app.debug = config.debug
