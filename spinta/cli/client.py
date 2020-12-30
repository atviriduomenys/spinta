import pathlib
import uuid

import click

from spinta import commands
from spinta.auth import create_client_file
from spinta.cli.helpers.auth import require_auth
from spinta.cli import main


@main.group()
@click.pass_context
def client(ctx):
    pass


@client.command('add')
@click.option('--name', '-n', help="client name")
@click.option('--secret', '-s', help="client secret")
@click.option('--add-secret', is_flag=True, default=False, help="add client secret in plain text to file")
@click.option('--scope', help="space separated list of scopes")
@click.option('--path', '-p', type=click.Path(exists=True, file_okay=False, writable=True))
@click.pass_context
def client_add(ctx, name, secret, add_secret, scope: str, path):
    context = ctx.obj
    with context:
        require_auth(context)

        if path is None:
            context = ctx.obj

            config = context.get('config')
            commands.load(context, config)

            path = config.config_path / 'clients'
            path.mkdir(exist_ok=True)
        else:
            path = pathlib.Path(path)

        name = name or str(uuid.uuid4())

        scope = scope or click.get_text_stream('stdin').read()
        if scope:
            scope = [s.strip() for s in scope.split()]
            scope = [s for s in scope if s]
        scope = scope or None

        client_file, client_ = create_client_file(
            path,
            name,
            secret,
            scope,
            add_secret=add_secret,
        )

        client_secret = client_['client_secret']
        click.echo(
            f"New client created and saved to:\n\n"
            f"    {client_file}\n\n"
            f"Client secret:\n\n"
            f"    {client_secret}\n\n"
            f"Remember this client secret, because only a secure hash of\n"
            f"client secret will be stored in the config file."
        )
