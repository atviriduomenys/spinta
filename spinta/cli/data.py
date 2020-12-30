import asyncio
import itertools
import json

import click

from spinta.cli import main
from spinta.cli.helpers.auth import require_auth
from spinta.cli.helpers.store import prepare_manifest
from spinta.cli.pull import _process_stream
from spinta.commands.write import write


@main.command('import', help='Import data from a file.')
@click.argument('source')
@click.option('--auth', '-a', help="Authorize as client.")
@click.option('--limit', type=int, help="Limit number of rows read from source.")
@click.pass_context
def import_(ctx, source, auth, limit):
    context = ctx.obj
    store = prepare_manifest(context)
    manifest = store.manifest
    root = manifest.objects['ns']['']

    with context:
        require_auth(context)
        context.attach('transaction', manifest.backend.transaction, write=True)
        with open(source) as f:
            stream = (json.loads(line.strip()) for line in f)
            stream = itertools.islice(stream, limit) if limit else stream
            stream = write(context, root, stream, changed=True)
            coro = _process_stream(source, stream)
            asyncio.get_event_loop().run_until_complete(coro)
