import pathlib

from ruamel.yaml import YAML

from spinta.utils.imports import importstr
from spinta.commands import load, check
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta import components
from spinta.core.ufuncs import ufunc

yaml = YAML(typ='safe')


@load.register()
def load(context: Context, config: components.Config, rc: RawConfig) -> components.Config:
    # Save reference to raw config and give chance for other components to read
    # directly from raw config.
    config.rc = rc

    # Load commands.
    config.commands = {}
    for scope in rc.keys('commands'):
        if scope == 'modules':
            continue
        config.commands[scope] = {}
        for name in rc.keys('commands', scope):
            command = rc.get('commands', scope, name, cast=importstr)
            config.commands[scope][name] = command

    # Load ufuncs.
    ufunc.resolver.collect(rc.get('ufuncs'))
    config.resolvers = ufunc.resolver.ufuncs()
    config.executors = ufunc.executor.ufuncs()

    # Load components.
    config.components = {}
    for group in rc.keys('components'):
        config.components[group] = {}
        for name in rc.keys('components', group):
            component = rc.get('components', group, name, cast=importstr)
            config.components[group][name] = component

    # Load exporters.
    config.exporters = {}
    for name in rc.keys('exporters'):
        exporter = rc.get('exporters', name, cast=importstr)
        config.exporters[name] = exporter()

    # Load everything else.
    config.debug = rc.get('debug', default=False)
    config.config_path = rc.get('config_path', cast=pathlib.Path, exists=True)
    config.server_url = rc.get('server_url')
    config.scope_prefix = rc.get('scope_prefix')
    config.scope_max_length = rc.get('scope_max_length', cast=int)
    config.default_auth_client = rc.get('default_auth_client')
    config.datasets = rc.get('datasets', default={})
    config.env = rc.get('env')
    config.docs_path = rc.get('docs_path', default=None)
    config.always_show_id = rc.get('always_show_id', default=False)

    return config


@check.register()
def check(context: Context, config: components.Config):
    if config.default_auth_client and not (config.config_path / 'clients' / f'{config.default_auth_client}.yml').exists():
        clients_dir = config.config_path / 'clients'
        raise Exception(f"default_auth_client {config.default_auth_client!r} does not exist in {clients_dir}.")
