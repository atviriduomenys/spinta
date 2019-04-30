import pathlib

from spinta.utils.imports import importstr
from spinta.commands import load
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
    config.keys_dir = c.get('keys_dir', cast=pathlib.Path)

    return config
