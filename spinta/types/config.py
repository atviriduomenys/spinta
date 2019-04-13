from spinta.utils.imports import importstr
from spinta.commands import load
from spinta.components import Context, Config, BackendConfig


@load.register()
def load(context: Context, config: Config, data: dict):

    # Load commands.
    config.commands = {}
    for scope, commands in data.get('commands', {}).items():
        if scope == 'modules':
            continue
        config.commands[scope] = {}
        for name, command in commands.items():
            config.commands[scope][name] = importstr(command)

    # Load data types.
    config.types = {}
    for name, type in data['types'].items():
        config.types[name] = importstr(type)

    # Load backends.
    config.backends = {}
    for name, backend in data['backends'].items():
        bconf = config.backends[name] = BackendConfig()
        bconf.Backend = importstr(backend['backend'])
        bconf.name = name
        bconf.dsn = backend['dsn']

    config.manifests = data['manifests']
    config.ignore = data.get('ignore', [])
    config.debug = data.get('debug', False)
