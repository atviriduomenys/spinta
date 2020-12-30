import json
import sys

import click
from authlib.jose import jwt

from spinta import commands
from spinta.auth import KeyType
from spinta.auth import load_key
from spinta.cli import main


@main.command('decode-token')
@click.pass_context
def decode_token(ctx):
    context = ctx.obj
    config = context.get('config')
    commands.load(context, config)
    key = load_key(context, KeyType.public)
    token = sys.stdin.read().strip()
    token = jwt.decode(token, key)
    click.echo(json.dumps(token, indent='  '))
