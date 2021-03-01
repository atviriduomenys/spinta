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
            'spinta.manifests',
            'spinta.datasets',
            'spinta.dimensions',
            'spinta.naming',
            'spinta.ufuncs',
        ],
        'service': {
            'range': 'spinta.commands.helpers:range_',
        },
    },
    'ufuncs': [
        'spinta.ufuncs',
        'spinta.datasets.backends.sql.commands.query',
    ],
    'components': {
        'core': {
            'context': 'spinta.components:Context',
            'config': 'spinta.components:Config',
            'store': 'spinta.components:Store',
        },
        'accesslog': {
            'file': 'spinta.accesslog.file:FileAccessLog',
            'python': 'spinta.accesslog.python:PythonAccessLog',
        },
        'manifests': {
            'backend': 'spinta.manifests.backend.components:BackendManifest',
            'yaml': 'spinta.manifests.yaml.components:YamlManifest',
            'tabular': 'spinta.manifests.tabular.components:TabularManifest',
        },
        'backends': {
            # In memory backends mostly usable in tests
            'memory': 'spinta.backends.memory.components:Memory',

            # Internal backends
            'postgresql': 'spinta.backends.postgresql.components:PostgreSQL',
            'mongo': 'spinta.backends.mongo.components:Mongo',
            'fs': 'spinta.backends.fs.components:FileSystem',
            's3': 'spinta.backends.s3:S3',

            # External backends
            'sql': 'spinta.datasets.backends.sql.components:Sql',
            'sqldump': 'spinta.datasets.backends.sqldump.components:SqlDump',
            'csv': 'spinta.datasets.backends.frictionless.components:FrictionlessBackend',
            'xml': 'spinta.datasets.backends.frictionless.components:FrictionlessBackend',
            'xlsx': 'spinta.datasets.backends.frictionless.components:FrictionlessBackend',
            'json': 'spinta.datasets.backends.frictionless.components:FrictionlessBackend',
        },
        'migrations': {
            'alembic': 'spinta.migrations.schema.alembic:Alembic',
        },
        'nodes': {
            'ns': 'spinta.components:Namespace',
            'model': 'spinta.components:Model',
            'owner': 'spinta.types.owner:Owner',
            'project': 'spinta.types.project:Project',
            'dataset': 'spinta.datasets.components:Dataset',
        },
        'datasets': {
            'resource': 'spinta.datasets.components:Resource',
            'entity': 'spinta.datasets.components:Entity',
            'attribute': 'spinta.datasets.components:Attribute',
        },
        'keymaps': {
            'sqlalchemy': 'spinta.datasets.keymaps.sqlalchemy:SqlAlchemyKeyMap',
        },
        'types': {
            'any': 'spinta.types.datatype:DataType',
            'pk': 'spinta.types.datatype:PrimaryKey',
            'date': 'spinta.types.datatype:Date',
            'datetime': 'spinta.types.datatype:DateTime',
            'temporal': 'spinta.types.datatype:DateTime',
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
        'urlparams': {
            'component': 'spinta.urlparams:UrlParams',
        },
        'dimensions': {
            'prefix': 'spinta.dimensions.prefix.components:UriPrefix',
        }
    },
    'exporters': {
        'ascii': 'spinta.commands.formats.ascii:Ascii',
        'csv': 'spinta.commands.formats.csv:Csv',
        'json': 'spinta.commands.formats.json:Json',
        'jsonl': 'spinta.commands.formats.jsonl:JsonLines',
        'html': 'spinta.commands.formats.html:Html',
    },
    'accesslog': {
        'type': 'file',
        'file': 'stdout',
        'buffer_size': 300,
    },
    'manifests': {
        'default': {
            'type': 'memory',
            'backend': '',
            'mode': 'internal',
            'keymap': '',
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
    'config_path': '',

    # Path to documentation folder. If set will serve the folder at `/docs`.
    'docs_path': None,

    'server_url': 'https://example.com/',

    # This prefix will be added to autogenerated scope names.
    'scope_prefix': 'spinta_',

    # Scope name formatter.
    'scope_formatter': 'spinta.auth:get_scope_name',

    # Part of the scope will be replaced with a hash to fit scope_max_length.
    'scope_max_length': 60,

    # If True, then 'id' property is always included into Action.SEARCH result,
    # even if not explicitly asked.
    'always_show_id': False,

    'default_auth_client': None,

    # Public JWK key for validating auth bearer tokens.
    'token_validation_key': None,

    # Limit access to specified namespace root.
    'root': None,

    'env': 'prod',

    'environments': {
        'dev': {
            'keymaps.default': {
                'type': 'sqlalchemy',
                'dsn': 'sqlite:///var/keymaps.db',
            },
            'backends': {
                'default': {
                    'type': 'postgresql',
                    'dsn': 'postgresql://admin:admin123@localhost:54321/spinta',
                    'migrate': 'alembic',
                },
                'mongo': {
                    'type': 'mongo',
                    'dsn': 'mongodb://admin:admin123@localhost:27017/',
                    'db': 'spinta',
                },
                'fs': {
                    'type': 'fs',
                    'path': pathlib.Path() / 'var/files',
                },
                # Locally we don't have access to s3.
                's3': {
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
                    'keymap': 'default',
                    'mode': 'internal',
                    'backend': 'default',
                },
                'yaml': {
                    'type': 'yaml',
                    'backend': 'default',
                    'path': pathlib.Path() / 'tests/manifest',
                },
            },
            'default_auth_client': '3388ea36-4a4f-4821-900a-b574c8829d52',
            'config_path': pathlib.Path('tests/config'),
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
                's3': {
                    'type': 's3',
                    'bucket': 'splat-test',
                    'region': 'eu-north-1',
                    'access_key_id': 'test_access_key',
                    'secret_access_key': 'test_secret_access_key',
                },
            },
            'manifests': {
                # TODO: remove `default` manifest
                'default': {
                    'type': 'yaml',
                    'path': pathlib.Path() / 'tests/manifest',
                    'sync': None,
                    'backend': 'default',
                    'keymap': 'default',
                    'mode': 'internal',
                },
                'yaml': {
                    'type': 'yaml',
                    'backend': 'default',
                    'path': pathlib.Path() / 'tests/manifest',
                },
            },
            'config_path': pathlib.Path('tests/config'),
            'default_auth_client': 'baa448a8-205c-4faa-a048-a10e4b32a136',
        }
    },
}
