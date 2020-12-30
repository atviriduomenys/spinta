import pathlib

import click

from spinta import commands
from spinta.auth import KeyFileExists
from spinta.auth import gen_auth_server_keys
from spinta.cli import log
from spinta.cli import main
from spinta.cli.helpers.auth import require_auth
from spinta.cli.helpers.store import prepare_store
from spinta.cli.helpers.store import load_store


@main.command()
@click.option('--path', '-p', type=click.Path(exists=True, file_okay=False, writable=True))
@click.pass_context
def genkeys(ctx, path):
    context = ctx.obj
    with context:
        require_auth(context)

        if path is None:
            context = ctx.obj
            config = context.get('config')
            commands.load(context, config)
            path = config.config_path
        else:
            path = pathlib.Path(path)

        try:
            prv, pub = gen_auth_server_keys(path)
        except KeyFileExists as e:
            raise click.Abort(str(e))

        click.echo(f"Private key saved to {prv}.")
        click.echo(f"Public key saved to {pub}.")


@main.command()
@click.pass_context
def run(ctx):
    import uvicorn
    import spinta.api

    context = ctx.obj
    prepare_store(context)
    app = spinta.api.init(context)

    log.info("Spinta has started!")
    uvicorn.run(app, host='0.0.0.0', port=8000)


@main.command(help="Wait while all backends are up.")
@click.argument('seconds', type=int, required=False)
@click.pass_context
def wait(ctx, seconds):
    context = ctx.obj
    store = load_store(context)
    commands.wait(context, store, seconds=seconds)
    click.echo("All backends ar up.")
