import pathlib

from spinta.utils.imports import importstr
from spinta.commands import load, check
from spinta.components import Context
from spinta.config import Config
from spinta import components


@load.register()
def load(context: Context, config: components.Config, c: Config) -> Config:

    # Load commands.
    config.commands = {}
    for scope in c.keys('commands'):
        if scope == 'modules':
            continue
        config.commands[scope] = {}
        for name in c.keys('commands', scope):
            command = c.get('commands', scope, name, cast=importstr)
            config.commands[scope][name] = command

    # Load components.
    config.components = {}
    for group in c.keys('components'):
        config.components[group] = {}
        for name in c.keys('components', group):
            component = c.get('components', group, name, cast=importstr)
            config.components[group][name] = component

    # Load exporters.
    config.exporters = {}
    for name in c.keys('exporters'):
        exporter = c.get('exporters', name, cast=importstr)
        config.exporters[name] = exporter()

    # Load everything else.
    config.debug = c.get('debug', default=False)
    config.config_path = c.get('config_path', cast=pathlib.Path, exists=True)
    config.server_url = c.get('server_url')
    config.scope_prefix = c.get('scope_prefix')
    config.scope_max_length = c.get('scope_max_length', cast=int)
    config.default_auth_client = c.get('default_auth_client')

    return config


@check.register()
def check(context: Context, config: components.Config):
    if config.default_auth_client and not (config.config_path / 'clients' / f'{config.default_auth_client}.yml').exists():
        clients_dir = config.config_path / 'clients'
        raise Exception(f"default_auth_client {config.default_auth_client!r} does not exist in {clients_dir}.")
