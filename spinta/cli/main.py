from __future__ import annotations

import logging
import pathlib
from typing import List
from typing import Optional

from typer import Context as TyperContext
from typer import Option
from typer import Typer
from typer import echo

import spinta
from spinta.cli import auth
from spinta.cli import config
from spinta.cli import data
from spinta.cli import inspect
from spinta.cli import manifest
from spinta.cli import migrate
from spinta.cli import pii
from spinta.cli import pull
from spinta.cli import push
from spinta.cli import server
from spinta.cli.init import init
from spinta.cli.show import show
from spinta.cli.helpers.typer import add
from spinta.core.context import create_context

log = logging.getLogger(__name__)

app = Typer()

add(app, 'config', config.config, help="Show current configuration values")
add(app, 'check', config.check, help="Check configuration and manifests")

add(app, 'genkeys', auth.genkeys, help="Generate client token validation keys")
add(app, 'token', auth.token, help="Tools for encoding/decode auth tokens")
add(app, 'client', auth.client, help="Manage auth clients")

add(app, 'init', init, help="Initialize new manifest table")
add(app, 'inspect', inspect.inspect, help=(
    "Update manifest schema from an external data source"
))
add(app, 'pii', pii.app, help="Manage Person Identifying Information")

add(app, 'copy', manifest.copy, help="Copy only specified metadata from a manifest")
add(app, 'show', show, help="Show manifest as ascii table")

add(app, 'bootstrap', migrate.bootstrap, help="Initialize backends")
add(app, 'sync', migrate.sync, help="Sync source manifests into main manifest")
add(app, 'migrate', migrate.migrate, help="Migrate schema changes to backends")
add(app, 'freeze', migrate.freeze, help=(
    "Detect schema changes and create new schema version"
))

add(app, 'import', data.import_, help="Import data from a file")
add(app, 'pull', pull.pull, help="Pull data from an external data source")
add(app, 'push', push.push, help="Push data into an external data store")

add(app, 'run', server.run, help="Run development server")
add(app, 'wait', server.wait, help="Wait while all backends are up")


@app.callback(invoke_without_command=True)
def main(
    ctx: TyperContext,
    option: Optional[List[str]] = Option(None, '-o', '--option', help=(
        "Set configuration option, example: `-o option.name=value`."
    )),
    env_file: Optional[pathlib.Path] = Option(None, '--env-file', help=(
        "Load configuration from a given .env file."
    )),
    version: bool = Option(False, help="Show version number.")
):
    ctx.obj = ctx.obj or create_context('cli', args=option, envfile=env_file)
    if version:
        echo(spinta.__version__)


