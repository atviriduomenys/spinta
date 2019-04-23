import collections
import pathlib


CONFIG = {
    'commands': {
        'modules': [
            'spinta.types',
            'spinta.backends',
            'spinta.urlparams',
        ],
        'source': {
            'csv': 'spinta.commands.sources.csv:read_csv',
            'html': 'spinta.commands.sources.html:read_html',
            'json': 'spinta.commands.sources.json:read_json',
            'url': 'spinta.commands.sources.url:read_url',
            'xlsx': 'spinta.commands.sources.xlsx:read_xlsx',
            'xml': 'spinta.commands.sources.xml:read_xml',
        },
        'service': {
            'range': 'spinta.commands.helpers:range_',
        },
    },
    'components': {
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
        },
        'urlparams': {
            'component': 'spinta.urlparams:UrlParams',
            # 'versions': {
            #     '1': 'spinta.urlparams:Version',
            # },
        },
    },
    'exporters': {
        'ascii': 'spinta.commands.formats.ascii:Ascii',
        'csv': 'spinta.commands.formats.csv:Csv',
        'json': 'spinta.commands.formats.json:Json',
        'jsonl': 'spinta.commands.formats.jsonl:JsonLines',
    },
    'backends': {
        'default': {
            'backend': 'spinta.backends.postgresql:PostgreSQL',
            'dsn': 'postgresql:///spinta',
        },
        'mongo': {
            'backend': 'spinta.backends.mongo:Mongo',
            'dsn': 'mongodb:///',
            'db': 'spinta',
        },
    },
    'manifests': {
        'default': {
            'backend': 'default',
            'path': pathlib.Path(),
        },
    },
    'ignore': [
        '.travis.yml',
        '/prefixes.yml',
        '/schema/',
        '/env/',
    ],
    'debug': False,
}


class Config:

    def __init__(self):
        self._config = {
            'cliargs': {},
            'environ': {},
            'envfile': {},
            'default': {},
        }
        self.add(CONFIG)

    def add(self, config):
        self._config['default'].update(_traverse(config))
        self._config['default'].update(_get_inner_keys(self._config['default']))

    def add_cli_args(self, args):
        for arg in args:
            name, value = arg.split('=', 1)
            key = tuple(name.split('.'))
            self._config['cliargs'][key] = value
        self._config['cliargs'].update(_get_inner_keys(self._config['cliargs']))

    def add_env_file(self, path):
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

    def set_env(self, environ):
        self._config['environ'] = environ

    def get(self, *key, default=None, cast=None, env=True, required=False):
        value = self._get_config_value(key, default, env)

        if cast is not None:
            if cast is list and isinstance(value, str):
                value = value.split(',')
            elif value is not None:
                value = cast(value)

        if required and not value:
            raise Exception("'%s' is a required configuration option." % '.'.join(key))

        return value

    def keys(self, *key, **kwargs):
        return self.get(*key, cast=list, **kwargs)

    def _get_config_value(self, key: tuple, default, env):

        # 1. Get value from comman line arguments.
        if key in self._config['cliargs']:
            return self._config['cliargs'][key]

        # Auto-generate environment variable name.
        if env is True:
            env = 'SPINTA_' + '_'.join(key).upper()

        # 2. Get value from environment.
        if env and env in self._config['environ']:
            return self._config['environ'][env]

        # 3. Get value from env file.
        if env and env in self._config['envfile']:
            return self._config['envfile'][env]

        # 4. Get value from default configs.
        if key in self._config['default']:
            return self._config['default'][key]

        return default


def _traverse(value, path=()):
    if isinstance(value, dict):
        for k, v in value.items():
            yield from _traverse(v, path + (k,))
    else:
        yield path, value


def _get_inner_keys(value):
    inner = collections.defaultdict(list)
    for key in value.keys():
        for i in range(1, len(key)):
            k = tuple(key[:i])
            if key[i] not in inner[k]:
                inner[k].append(key[i])
    return inner
