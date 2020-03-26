import pathlib


CONFIG = {
    'config': [],
    'commands': {
        'modules': [
            'spinta.backends',
            'spinta.commands.auth',
            'spinta.commands.read',
            'spinta.commands.write',
            'spinta.commands.search',
            'spinta.commands.version',
            'spinta.types',
            'spinta.urlparams',
        ],
        'service': {
            'range': 'spinta.commands.helpers:range_',
        },
    },
    'ufuncs': [
        'spinta.ufuncs',
    ],
    'components': {
        'core': {
            'context': 'spinta.components:Context',
            'config': 'spinta.components:Config',
            'store': 'spinta.components:Store',
            'source': 'spinta.components:Source',
        },
        'accesslog': {
            'file': 'spinta.accesslog.file:FileAccessLog',
            'python': 'spinta.accesslog.python:PythonAccessLog',
        },
        'manifests': {
            'spinta': 'spinta.manifests.spinta:SpintaManifest',
            'yaml': 'spinta.manifests.yaml:YamlManifest',
            'inline': 'spinta.manifests.inline:InlineManifest',
        },
        'backends': {
            # internal
            'postgresql': 'spinta.backends.internal.postgresql:PostgreSQL',
            'mongo': 'spinta.backends.internal.mongo:Mongo',


            # external
            'fs': 'spinta.backends.fs:FileSystem',
            'csv': 'spinta.external.csv:Csv',
            #'json': 'spinta.external.json:Json',
            #'xlsx': 'spinta.external.xlsx:Xlsx',
            #'xml': 'spinta.external.xml:Xml',
            #'sql': 'spinta.external.sql:Sql',
        },
        'services': {
            'http': 'spinta.service.http:Http',
        },
        'formats': {
            'string': 'spinta.formats.string:StringFormat',
            'ascii': 'spinta.formats.ascii:Ascii',
            'csv': 'spinta.formats.csv:Csv',
            'json': 'spinta.formats.json:Json',
            'jsonl': 'spinta.formats.jsonl:JsonLines',
            'html': 'spinta.formats.html:Html',
        },
        'migrations': {
            'alembic': 'spinta.migrations.schema.alembic:Alembic',
        },
        'nodes': {
            'ns': 'spinta.components:Namespace',
            'model': 'spinta.components:Model',
            'property': 'spinta.components:Property',

        },
        'datasets': {
            'owner': 'spinta.components.datasets:Owner',
            'impact': 'spinta.components.datasets:Impact',
            'project': 'spinta.components.datasets:Project',
            'dataset': 'spinta.components.datasets:Dataset',
            'resource': 'spinta.components.datasets.Resource',
            'entity': 'spinta.components.datasets.Entity',
            'attribute': 'spinta.components.datasets.Attribute',
        },
        'types': {
            'integer': 'spinta.types.datatype:Integer',
            'any': 'spinta.types.datatype:DataType',
            'pk': 'spinta.types.datatype:PrimaryKey',
            'date': 'spinta.types.datatype:Date',
            'datetime': 'spinta.types.datatype:DateTime',
            'string': 'spinta.types.datatype:String',
            'binary': 'spinta.types.datatype:Binary',
            'integer': 'spinta.types.datatype:Integer',
            'number': 'spinta.types.datatype:Number',
            'boolean': 'spinta.types.datatype:Boolean',
            'url': 'spinta.types.datatype:URL',
            'image': 'spinta.types.datatype:Image',
            'spatial': 'spinta.types.datatype:Spatial',
            'ref': 'spinta.types.datatype:Ref',
            'backref': 'spinta.types.datatype:BackRef',
            'generic': 'spinta.types.datatype:Generic',
            'array': 'spinta.types.datatype:Array',
            'object': 'spinta.types.datatype:Object',
            'file': 'spinta.types.datatype:File',
            'rql': 'spinta.types.datatype:RQL',
            'json': 'spinta.types.datatype:JSON',
        },
    },
    'accesslog': {
        'type': 'file',
        'file': 'stdout',
        'buffer_size': 300,
    },
    'services': {
        'http': {
            'type': 'http',
            'cache': pathlib.Path() / 'var/cache/http',
        },
    },
    'backends': {
        'default': {
            'type': 'postgresql',
            'dsn': 'postgresql://admin:admin123@localhost:54321/spinta',
            'migrate': 'alembic',
        },
    },
    'manifests': {
        'default': {
            'type': 'spinta',
            'backend': 'default',
            'sync': 'yaml',
        },
        'yaml': {
            'type': 'yaml',
            'backend': 'default',
            'path': pathlib.Path(),
        },
    },

    'manifest': 'default',

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

    # If True, then 'id' property is always included into Action.SEARCH result,
    # even if not explicitly askend.
    'always_show_id': False,

    'default_auth_client': None,

    'env': 'dev',

    'environments': {
        'dev': {
            'backends': {
                'default': {
                    'type': 'postgresql',
                    'dsn': 'postgresql://admin:admin123@localhost:54321/spinta',
                    'migrate': 'alembic',
                },
                'mongo': {
                    'type': 'mongo',
                    'dsn': 'mongodb://admin:admin123@localhost:27017/',
                    'db': 'splat',
                },
                'fs': {
                    'type': 'fs',
                    'path': pathlib.Path() / 'var/files',
                },
            },
            'manifests': {
                # TODO: remove `default` manifest
                'default': {
                    'type': 'yaml',
                    'path': pathlib.Path() / 'tests/manifest',
                    'sync': None,
                },
                'yaml': {
                    'path': pathlib.Path() / 'tests/manifest',
                },
            },
            'default_auth_client': '3388ea36-4a4f-4821-900a-b574c8829d52',
        },
        'test': {
            'accesslog': {
                'type': 'python',
            },
            'backends': {
                'default': {
                    'type': 'postgresql',
                    'dsn': 'postgresql://admin:admin123@localhost:54321/spinta_tests',
                },
                'mongo': {
                    'type': 'mongo',
                    'dsn': 'mongodb://admin:admin123@localhost:27017/',
                    'db': 'spinta_tests',
                },
                'fs': {
                    'type': 'fs',
                    'path': pathlib.Path() / 'var/files',
                },
            },
            'manifests': {
                # TODO: remove `default` manifest
                'default': {
                    'type': 'yaml',
                    'path': pathlib.Path() / 'tests/manifest',
                    'sync': None,
                },
            },
            'config_path': pathlib.Path('tests/config'),
            'default_auth_client': 'baa448a8-205c-4faa-a048-a10e4b32a136',
        }
    },
}
