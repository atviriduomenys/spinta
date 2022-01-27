import json
import pathlib
import sys
import uuid

import click
from authlib.jose import jwt
from typer import Argument
from typer import Context as TyperContext
from typer import Exit
from typer import Option
from typer import Typer
from typer import echo

from spinta import commands
from spinta.auth import KeyFileExists
from spinta.auth import KeyType
from spinta.auth import create_client_file
from spinta.auth import gen_auth_server_keys
from spinta.auth import load_key
from spinta.cli.helpers.auth import require_auth
from spinta.cli.helpers.store import load_config
from spinta.client import get_access_token
from spinta.client import get_client_credentials
from spinta.components import Context
from spinta.core.context import configure_context


def genkeys(
    ctx: TyperContext,
    path: pathlib.Path = Option(None, '--path', '-p', help=(
        "directory where client YAML files are stored"
    )),
):
    """Generate client token validation keys"""
    context: Context = ctx.obj
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
            echo(str(e))
            raise Exit(code=1)

        click.echo(f"Private key saved to {prv}.")
        click.echo(f"Public key saved to {pub}.")


token = Typer()


@token.command('decode', short_help="Decode auth token passed via stdin")
def token_decode(
    ctx: TyperContext,
    token_: str = Argument(None, help=(
        "Token string to decode, if not given token will be read from stdin"
    )),
):
    """Decode auth token passed via stdin

    This will decode token given by Auth server via /auth/token API endpoint.
    """
    context = configure_context(ctx.obj)
    load_config(context)
    key = load_key(context, KeyType.public)
    token_ = token_ or sys.stdin.read().strip()
    token_ = jwt.decode(token_, key)
    echo(json.dumps(token_, indent='  '))


@token.command('get', short_help="Get auth toke from an auth server")
def token_get(
    ctx: TyperContext,
    server: str = Argument(None, help=(
        "Source manifest files to copy from"
    )),
    header: bool = Option(False, '--header', help=(
        "Return full header (Authorization: Bearer {token})"
    )),
    credentials: str = Option(None, '--credentials', help=(
        "Credentials file, defaults to {config_path}/credentials.cfg"
    )),
):
    """Get auth token from a given authorization server"""
    context = configure_context(ctx.obj)
    config = load_config(context)
    if credentials:
        credsfile = pathlib.Path(credentials)
        if not credsfile.exists():
            echo(f"Credentials file {credsfile} does not exit.")
            raise Exit(code=1)
    else:
        credsfile = config.credentials_file
    # TODO: Read client credentials only if a Spinta URL is given.
    creds = get_client_credentials(credsfile, server)
    token_ = get_access_token(creds)
    if header:
        echo(f'Authorization: Bearer {token_}')
    else:
        echo(token_)


client = Typer()


@client.command('add')
def client_add(
    ctx: TyperContext,
    name: str = Option(None, '-n', '--name', help="client name"),
    secret: str = Option(None, '-s', '--secret', help="client secret"),
    add_secret: bool = Option(False, '--add-secret', help=(
        "add client secret in plain text to file"
    )),
    scope: str = Option(None, help=(
        "space separated list of scopes (if - is given, read scopes from stdin)"
    )),
    path: pathlib.Path = Option(None, '-p', '--path', help=(
        "directory where client YAML files are stored"
    )),
):
    """Add a new client"""
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

        if scope == '-':
            scope = click.get_text_stream('stdin').read()
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
