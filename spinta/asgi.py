from spinta.api import init
from spinta.core.context import create_context
from spinta import commands
from spinta.logging_config import setup_logging

context = create_context("asgi")

rc = context.get("rc")
config = context.get("config")
commands.load(context, config)

setup_logging(config)

commands.check(context, config)

store = context.get("store")
commands.load(context, store)

commands.load(context, store.manifest)
commands.link(context, store.manifest)
commands.check(context, store.manifest)

commands.wait(context, store)

commands.prepare(context, store.manifest)

app = init(context)
