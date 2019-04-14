import copy
import os
import pathlib


CONFIG = {
    'commands': {
        'modules': [
            'spinta.commands',
            'spinta.config.commands',
            'spinta.manifest.commands',
            'spinta.types.commands',
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

TEMPLATES = {
    ('backends',): 'default',
    ('manifests',): 'default',
}


def get_config(options=None):
    config = copy.deepcopy(CONFIG)
    envfile = pathlib.Path('.env')
    if envfile.exists():
        config = update_config_from_env_file(config, envfile)
    config = update_config_from_env(config, os.environ)
    if options is not None:
        config = update_config_from_cli(config, options)
    return config


def update_config(config, options):
    config = copy.deepcopy(config)
    for keys, value in options:
        path = ()
        this = config
        for key in keys[:-1]:
            if isinstance(this, dict):
                if key in this:
                    this = this[key]
                elif path in TEMPLATES:
                    this[key] = get_copy_by_path(CONFIG, path + (TEMPLATES[path],))
                    this = this[key]
            else:
                name = '.'.join(keys)
                raise Exception(f"Unknown configuration option {name!r}.")

            path += (key,)

        key = keys[-1]
        if key in this:
            Type = type(this[key])
            if Type is bool:
                value = value in ('on', 'true', 'yes', '1')
            else:
                value = Type(value)
            this[key] = value
        else:
            name = '.'.join(keys)
            raise Exception(f"Unknown configuration option {name!r}.")
    return config


def update_config_from_cli(config, options):
    options = (o.split('=', 1) for o in options)
    options = ((k.split('.'), v) for k, v in options)
    return update_config(config, options)


def update_config_from_env(config, env):
    options = get_options_from_env(CONFIG, env)
    for path, template in TEMPLATES.items():
        name = '_'.join(['SPINTA'] + [p.upper() for p in path])
        if name in env:
            for key in env[name].split(','):
                _config = get_by_path(CONFIG, path + (template,))
                options += get_options_from_env(_config, env, path + (key,))
    return update_config(config, options)


def update_config_from_env_file(config, path: str):
    env = {}
    with pathlib.Path(path).open() as f:
        for line in f:
            if line.startswith('#'):
                continue
            line = line.strip()
            if line == '':
                continue
            if '=' not in line:
                continue
            name, value = line.split('=', 1)
            env[name] = value
    return update_config_from_env(config, env)


def get_options_from_env(config, env, base=()):
    options = []
    for path, v in traverse(config):
        name = '_'.join(['SPINTA'] + [p.upper() for p in base + path])
        if name in env:
            options.append((base + path, env[name]))
    return options


def get_by_path(config, path):
    this = config
    for key in path:
        if isinstance(this, dict) and key in this:
            this = this[key]
        else:
            path = '.'.join(path)
            raise Exception(f"Unknow configuration parameter {path!r}.")
    return this


def get_copy_by_path(config, path):
    return copy.deepcopy(get_by_path(config, path))


def traverse(value, path=()):
    if isinstance(value, dict):
        for k, v in value.items():
            yield from traverse(v, path + (k,))
    else:
        yield path, value
