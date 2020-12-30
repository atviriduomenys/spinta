from __future__ import annotations

import logging

import click

from spinta.core.context import create_context

log = logging.getLogger(__name__)


@click.group()
@click.option('--option', '-o', multiple=True, help=(
    "Set configuration option, example: `-o option.name=value`."
))
@click.option('--env-file', help="Load configuration from a given .env file.")
@click.pass_context
def main(ctx, option, env_file):
    ctx.obj = ctx.obj or create_context('cli', args=option, envfile=env_file)


