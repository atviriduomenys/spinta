import pathlib

from ruamel.yaml import YAML

from spinta.utils.imports import importstr
from spinta.commands import load, check
from spinta.components import Context
from spinta.config import RawConfig
from spinta import components

yaml = YAML(typ='safe')


@load.register()
def load(context: Context, config: components.Config, raw: RawConfig) -> components.Config:
    # Save reference to raw config and give chance for other components to read
    # directly from raw config.
    config.raw = raw

    # Load commands.
    config.commands = {}
    for scope in raw.keys('commands'):
        if scope == 'modules':
            continue
        config.commands[scope] = {}
        for name in raw.keys('commands', scope):
            command = raw.get('commands', scope, name, cast=importstr)
            config.commands[scope][name] = command

    # Load components.
    config.components = {}
    for group in raw.keys('components'):
        config.components[group] = {}
        for name in raw.keys('components', group):
            component = raw.get('components', group, name, cast=importstr)
            config.components[group][name] = component

    # Load exporters.
    config.exporters = {}
    for name in raw.keys('exporters'):
        exporter = raw.get('exporters', name, cast=importstr)
        config.exporters[name] = exporter()

    # Load everything else.
    config.debug = raw.get('debug', default=False)
    config.config_path = raw.get('config_path', cast=pathlib.Path, exists=True)
    config.server_url = raw.get('server_url')
    config.scope_prefix = raw.get('scope_prefix')
    config.scope_max_length = raw.get('scope_max_length', cast=int)
    config.default_auth_client = raw.get('default_auth_client')
    config.datasets = raw.get('datasets', default={})
    config.env = raw.get('env')
    config.docs_path = raw.get('docs_path', default=None)
    config.always_show_id = raw.get('always_show_id', default=False)

    return config


@check.register()
def check(context: Context, config: components.Config):
    if config.default_auth_client and not (config.config_path / 'clients' / f'{config.default_auth_client}.yml').exists():
        clients_dir = config.config_path / 'clients'
        raise Exception(f"default_auth_client {config.default_auth_client!r} does not exist in {clients_dir}.")
