import collections
import copy
import importlib
import logging
import operator
import os
import pathlib

from ruamel.yaml import YAML

from spinta.utils.imports import importstr

yaml = YAML(typ='safe')

log = logging.getLogger(__name__)


CONFIG = {
    'config': [],
    'commands': {
        'modules': [
            'spinta.backends',
            'spinta.commands.search',
            'spinta.commands.version',
            'spinta.types',
            'spinta.urlparams',
        ],
        'service': {
            'range': 'spinta.commands.helpers:range_',
        },
    },
    'components': {
        'core': {
            'context': 'spinta.components:Context',
        },
        'backends': {
            'postgresql': 'spinta.backends.postgresql:PostgreSQL',
            'mongo': 'spinta.backends.mongo:Mongo',
        },
        'nodes': {
            'model': 'spinta.components:Model',
            'project': 'spinta.types.project:Project',
            'dataset': 'spinta.types.dataset:Dataset',
            'owner': 'spinta.types.owner:Owner',
        },
        'types': {
            'integer': 'spinta.types.type:Integer',
            'any': 'spinta.types.type:Type',
            'pk': 'spinta.types.type:PrimaryKey',
            'date': 'spinta.types.type:Date',
            'datetime': 'spinta.types.type:DateTime',
            'string': 'spinta.types.type:String',
            'integer': 'spinta.types.type:Integer',
            'number': 'spinta.types.type:Number',
            'boolean': 'spinta.types.type:Boolean',
            'url': 'spinta.types.type:URL',
            'image': 'spinta.types.type:Image',
            'spatial': 'spinta.types.type:Spatial',
            'ref': 'spinta.types.type:Ref',
            'backref': 'spinta.types.type:BackRef',
            'generic': 'spinta.types.type:Generic',
            'array': 'spinta.types.type:Array',
            'object': 'spinta.types.type:Object',
            'file': 'spinta.types.type:File',
        },
        'urlparams': {
            'component': 'spinta.urlparams:UrlParams',
            # do not bother with versions for this time
            # 'versions': {
            #     '1': 'spinta.urlparams:Version',
            # },
        },
        'sources': {
            'csv': 'spinta.commands.sources.csv:Csv',
            'html': 'spinta.commands.sources:Source',  # TODO: this is a stub
            'json': 'spinta.commands.sources.json:Json',
            'url': 'spinta.commands.sources:Source',
            'xlsx': 'spinta.commands.sources.xlsx:Xlsx',
            'xml': 'spinta.commands.sources.xml:Xml',
            'sql': 'spinta.commands.sources.sql:Sql',
        },
    },
    'exporters': {
        'ascii': 'spinta.commands.formats.ascii:Ascii',
        'csv': 'spinta.commands.formats.csv:Csv',
        'json': 'spinta.commands.formats.json:Json',
        'jsonl': 'spinta.commands.formats.jsonl:JsonLines',
        'html': 'spinta.commands.formats.html:Html',
    },
    'backends': {
        'default': {
            'backend': 'spinta.backends.postgresql:PostgreSQL',
            'dsn': 'postgresql://admin:admin123@localhost:54321/spinta',
        },
    },
    'manifests': {
        'default': {
            'backend': 'default',
            'path': pathlib.Path(),
        },
    },

    # Parameters for datasets, for example credentials. Example:
    #
    #   datasets: {default: {gov/vpk: {resource: sql://user:pass@host/db}}
    #
    # There configuraiton parameters should be read directly by datasets.
    'datasets': {},

    # When scanning for manifest YAML files, ignore these files.
    'ignore': [
        '.travis.yml',
        '/prefixes.yml',
        '/schema/',
        '/env/',
    ],

    'debug': False,

    # How much time to wait in seconds for the backends to go up.
    'wait': 30,

    # Configuration path, where clients, keys and other things are stored.
    'config_path': pathlib.Path('tests/config'),

    # Path to documentation folder. If set will serve the folder at `/docs`.
    'docs_path': None,

    'server_url': 'https://example.com/',

    # This prefix will be added to autogenerated scope names.
    'scope_prefix': 'spinta_',

    # Part of the scope will be replaced with a hash to fit scope_max_length.
    'scope_max_length': 60,

    'default_auth_client': None,

    'env': 'dev',

    'environments': {
        'dev': {
            'backends': {
                'mongo': {
                    'backend': 'spinta.backends.mongo:Mongo',
                    'dsn': 'mongodb://admin:admin123@localhost:27017/',
                    'db': 'splat',
                },
                'fs': {
                    'backend': 'spinta.backends.fs:FileSystem',
                    'path': pathlib.Path() / 'var/files',
                },
            },
            'manifests': {
                'default': {
                    'backend': 'default',
                    'path': pathlib.Path() / 'tests/manifest',
                },
            },
            'default_auth_client': '3388ea36-4a4f-4821-900a-b574c8829d52',
        },
        'test': {
            'manifests': {
                'default': {
                    'backend': 'default',
                    'path': pathlib.Path() / 'tests/manifest',
                },
            },
            'config_path': pathlib.Path('tests/config'),
            'default_auth_client': 'baa448a8-205c-4faa-a048-a10e4b32a136',
        }
    },
}


class RawConfig:

    def __init__(self):
        self._dirty = False
        self._backup = None
        self._config = {
            'hardset': {},
            'cliargs': {},
            'environ': {},
            'envfile': {},
            'default': {},
        }

    def read(self, hardset=None, *, env_vars=None, env_files=None, cli_args=None, config=None):
        if self._config['default']:
            raise Exception(
                "Configuraiton has been read already, you can call read method "
                "only once for each Config instance."
            )

        # Default configuration.
        log.info("Reading config from '%s:CONFIG'.", __name__)
        self._add_config(CONFIG)

        # Add CLI args
        if cli_args:
            self._add_cli_args(cli_args)

        # Environment variables.
        if env_vars is None or env_vars is True:
            self._set_env_vars(os.environ)
        elif isinstance(env_vars, dict):
            self._set_env_vars(env_vars)

        # Environment files.
        if env_files is None or env_files is True:
            log.info("Reading config from '%s'.", pathlib.Path('.env').resolve())
            self._add_env_file('.env')
        elif isinstance(env_files, list):
            for env_file in env_files:
                log.info("Reading config from '%s'.", pathlib.Path(env_file).resolve())
                self._add_env_file(env_file)

        # Hard set configuration values.
        if hardset:
            self.hardset(hardset, dirty=False)

        # Add user supplied config.
        if config:
            self._add_config(config)

        # Override defaults from other locations.
        for _config in self.get('config', cast=list, default=[]):
            log.info("Reading config from %r.", _config)
            if _config.endswith(('.yml', '.yaml')):
                _config = pathlib.Path(_config)
                _config = yaml.load(_config.read_text())
            else:
                _config = importstr(_config)
            self._add_config(_config)

        # Update defaults from specified environment.
        environment = self.get('env', default=None)
        if environment:
            self._config['default'].update(dict(_get_from_prefix(
                self._config['default'],
                ('environments', environment),
            )))

        # Create inner keys
        self._config['default'].update(_get_inner_keys(self._config['default']))

    def get(self, *key, default=None, cast=None, env=True, required=False, exists=False, origin=False):
        envname, _ = self._get_config_value(('env',), None, True)
        value, origin_ = self._get_config_value(key, default, env, env=envname)

        if cast is not None:
            if cast is list and isinstance(value, str):
                value = value.split(',') if value else []
            elif value is not None:
                value = cast(value)

        if required and not value:
            name = '.'.join(key)
            raise Exception(f"{name!r} is a required configuration option.")

        if exists and isinstance(value, pathlib.Path) and not value.exists():
            name = '.'.join(key)
            raise Exception(f"{name} ({value}) path does not exist.")

        if origin:
            return value, origin_
        else:
            return value

    def keys(self, *key, **kwargs):
        kwargs.setdefault('default', [])
        return self.get(*key, cast=list, **kwargs)

    def getall(self, origin=False):
        for key, value in self._config['default'].items():
            if origin:
                value, origin = self.get(*key, cast=type(value), origin=origin)
                yield key, value, origin
            else:
                yield key, self.get(*key, cast=type(value), origin=origin)

    def dump(self, names=None):
        print('{:<10} {:<20} {}'.format('Origin', 'Name', 'Value'))
        print('{:-<10} {:-<20} {:-<40}'.format('', '', ''))
        for key, value, origin in sorted(self.getall(origin=True), key=operator.itemgetter(0)):
            if names:
                for name in names:
                    if '.'.join(key).startswith(name):
                        break
                else:
                    continue
            *pkey, key = key
            key = len(pkey) * '  ' + key
            if isinstance(value, list):
                value = ' | '.join(value)
            print(f'{origin:<10} {key:<20} {value}')

    def hardset(self, config, dirty=True):
        if dirty and not self._dirty:
            self._backup = self._config
            self._config = copy.deepcopy(self._config)
            self._dirty = dirty
        self._add_config(config, name='hardset')
        self._config['hardset'].update(_get_inner_keys(self._config['hardset']))

    def restore(self):
        if not self._dirty:
            return
        self._config = self._backup
        self._backup = None
        self._dirty = False

    def _add_config(self, config, name='default'):
        self._config[name].update(_traverse(config))

    def _add_cli_args(self, args):
        for arg in args:
            name, value = arg.split('=', 1)
            key = tuple(name.split('.'))
            self._config['cliargs'][key] = value
        self._config['cliargs'].update(_get_inner_keys(self._config['cliargs']))

    def _add_env_file(self, path):
        if isinstance(path, str):
            path = pathlib.Path(path)

        if not path.exists():
            return

        with path.open() as f:
            for line in f:
                if line.startswith('#'):
                    continue
                line = line.strip()
                if line == '':
                    continue
                if '=' not in line:
                    continue
                name, value = line.split('=', 1)
                self._config['envfile'][name] = value

    def _set_env_vars(self, environ):
        self._config['environ'] = environ

    def _get_config_value(self, key: tuple, default, envvar, env=None):
        assert isinstance(key, tuple)

        envvar = '_'.join(key).upper() if envvar is True else envvar

        # 1. Get hard set value with env prefix.
        if env:
            _key = ('environments', env) + key
            if _key in self._config['hardset']:
                return self._config['hardset'][_key], f'hardset:{env}'

        # 2. Get hard set value without env prefix.
        if key in self._config['hardset']:
            return self._config['hardset'][key], 'hardset'

        # 3. Get value from command line arguments.
        if key in self._config['cliargs']:
            return self._config['cliargs'][key], 'cliargs'

        # 4. Get value from environment with env prefix.
        if env:
            name = 'SPINTA_' + env.upper() + '_' + envvar if envvar else None
            if name and name in self._config['environ']:
                return self._config['environ'][name], f'environ:{env}'

        # 5. Get value from environment without env prefix.
        name = 'SPINTA_' + envvar if envvar else None
        if name and name in self._config['environ']:
            return self._config['environ'][name], 'environ'

        # 6. Get value from env file with env prefix.
        if env:
            name = 'SPINTA_' + env.upper() + '_' + envvar if envvar else None
            if name and name in self._config['envfile']:
                return self._config['envfile'][name], f'envfile:{env}'

        # 7. Get value from env file without env prefix.
        name = 'SPINTA_' + envvar if envvar else None
        if name and name in self._config['envfile']:
            return self._config['envfile'][name], 'envfile'

        # 8. Get value from default configs.
        if key in self._config['default']:
            return self._config['default'][key], 'config'

        return default, 'default'


def load_commands(modules):
    for module_path in modules:
        module = importlib.import_module(module_path)
        path = pathlib.Path(module.__file__).resolve()
        if path.name != '__init__.py':
            continue
        path = path.parent
        base = path.parents[module_path.count('.')]
        for path in path.glob('**/*.py'):
            if path.name == '__init__.py':
                module_path = path.parent.relative_to(base)
            else:
                module_path = path.relative_to(base).with_suffix('')
            module_path = '.'.join(module_path.parts)
            module = importlib.import_module(module_path)


def create_context(**kwargs):
    # Calling internal method, because internal method is patched in tests.
    return _create_context(**kwargs)


def _create_context(**kwargs):
    config = RawConfig()
    config.read(**kwargs)
    load_commands(config.get('commands', 'modules', cast=list))
    Context = config.get('components', 'core', 'context', cast=importstr)
    context = Context()
    context.set('config.raw', config)
    return context


def _traverse(value, path=()):
    if isinstance(value, dict):
        for k, v in value.items():
            yield from _traverse(v, path + (k,))
    else:
        yield path, value


def _get_inner_keys(config: dict):
    """Get inner keys for config.

    `config` is flattened dict, that looks like this:

        {
            ('a', 'b', 'c'): 1,
            ('a', 'b', 'd'): 2,
        }

    Nested version of this would look like this:

        {
            'a': {
                'b': {
                    'c': 1,
                    'd': 1,
                }
            }
        }

    Then, the purpose of this function is to add keys to all inner nesting
    levels. For this example, function result will be:

        {
            ('a'): ['b'],
            ('a', 'b'): ['c', 'd'],
        }

    This is needed for `RawConfig` class, in order to be able to do things like
    this:

        config.keys('a', 'b')
        ['c', 'd']

    And this functionality is needed, because of environment variables. For
    example, in order to add a new backend, first you need to add new keys, like
    this:

        SPINTA_A=b,x

    And then, you can add values to it:

        SPINTA_A_X=3

    And the end configuration will look like this:

        {
            'a': {
                'b': {
                    'c': 1,
                    'd': 1,
                },
                'x': '3'
            }
        }

    """
    inner = collections.defaultdict(list)
    for key in config.keys():
        for i in range(1, len(key)):
            k = tuple(key[:i])
            if key[i] not in inner[k]:
                inner[k].append(key[i])
    return inner


def _get_from_prefix(config: dict, prefix: tuple):
    for k, v in config.items():
        if k[:len(prefix)] == prefix:
            yield k[len(prefix):], v
