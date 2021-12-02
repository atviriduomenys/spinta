import json
import pathlib
from typing import Type

from ruamel.yaml import YAML

from spinta.formats.components import Format
from spinta.core.config import DEFAULT_CONFIG_PATH
from spinta.core.config import DEFAULT_DATA_PATH
from spinta.utils.config import asbool
from spinta.utils.imports import importstr
from spinta.commands import load, check
from spinta.components import Context, Config
from spinta import components
from spinta.core.ufuncs import ufunc

yaml = YAML(typ='safe')


@load.register(Context, Config)
def load(context: Context, config: Config) -> Config:
    # Save reference to raw config and give chance for other components to read
    # directly from raw config.
    rc = config.rc = context.get('rc')

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
    # TODO: Replace components from dict to classes with type annotations.
    config.components = {}
    for group in rc.keys('components'):
        config.components[group] = {}
        for name in rc.keys('components', group):
            component = rc.get('components', group, name, cast=importstr)
            config.components[group][name] = component

    # Load exporters.
    config.exporters = {}
    for name in rc.keys('exporters'):
        exporter: Type[Format] = rc.get('exporters', name, cast=importstr)
        config.exporters[name] = exporter()

    # Load accesslog
    accesslog = rc.get('accesslog', 'type', required=True)
    config.AccessLog = config.components['accesslog'][accesslog]

    # Load everything else.
    config.debug = rc.get('debug', default=False)
    config.config_path = pathlib.Path(
        rc.get('config_path') or
        DEFAULT_CONFIG_PATH
    )
    config.data_path = pathlib.Path(
        rc.get('data_path') or
        DEFAULT_DATA_PATH,
    )
    config.credentials_file = pathlib.Path(
        rc.get('credentials_file') or
        DEFAULT_CONFIG_PATH / 'credentials.cfg'
    )
    config.server_url = rc.get('server_url')
    config.scope_prefix = rc.get('scope_prefix')
    config.scope_formatter = rc.get('scope_formatter', cast=importstr)
    config.scope_max_length = rc.get('scope_max_length', cast=int)
    config.default_auth_client = rc.get('default_auth_client')
    config.http_basic_auth = rc.get('http_basic_auth', default=False, cast=asbool)
    config.token_validation_key = rc.get('token_validation_key', cast=json.loads) or None
    config.datasets = rc.get('datasets', default={})
    config.env = rc.get('env')
    config.docs_path = rc.get('docs_path', default=None)
    config.always_show_id = rc.get('always_show_id', default=False)
    config.root = rc.get('root', default=None)
    if config.root is not None:
        config.root = config.root.strip().strip('/')

    # XXX: A check to make sure, that I don't add mode to the config root, which
    #      already happened several times. Remove this, when configuration
    #      ensures that there are no unknown configuration options added.
    if rc.get('mode') is not None:
        raise RuntimeError(
            "Configuration option `mode` must be added to a manifest, now it "
            "is added to the config root."
        )

    return config


@check.register(Context, components.Config)
def check(context: Context, config: components.Config):
    if config.default_auth_client and not (config.config_path / 'clients' / f'{config.default_auth_client}.yml').exists():
        clients_dir = config.config_path / 'clients'
        raise Exception(f"default_auth_client {config.default_auth_client!r} does not exist in {clients_dir}.")
