import pathlib


CONFIG = {
    'config': [],
    'commands': {
        'modules': [
            'spinta.cli',
            'spinta.backends',
            'spinta.formats',
            'spinta.commands',
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
        'spinta.backends.postgresql.ufuncs',
        'spinta.backends.mongo.ufuncs',
        'spinta.datasets.backends.sql.ufuncs',
        'spinta.datasets.backends.dataframe.ufuncs',
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
            'inline': 'spinta.manifests.yaml.components:InlineManifest',
            'tabular': 'spinta.manifests.tabular.components:CsvManifest',
            'csv': 'spinta.manifests.tabular.components:CsvManifest',
            'ascii': 'spinta.manifests.tabular.components:AsciiManifest',
            'xlsx': 'spinta.manifests.tabular.components:XlsxManifest',
            'rdf': 'spinta.manifests.rdf.components:RdfManifest',
            'gsheets': 'spinta.manifests.tabular.components:GsheetsManifest',
            'sql': 'spinta.manifests.sql.components:SqlManifest',
            'memory': 'spinta.manifests.memory.components:MemoryManifest',
            'json': 'spinta.manifests.dict.components:JsonManifest',
            'xml': 'spinta.manifests.dict.components:XmlManifest',
            'internal': 'spinta.manifests.internal_sql.components:InternalSQLManifest',
            'xsd': 'spinta.manifests.xsd.components:XsdManifest',
        },
        'backends': {
            # In memory backends mostly usable in tests
            'memory': 'spinta.backends.memory.components:Memory',

            # Internal backends
            'postgresql': 'spinta.backends.postgresql.components:PostgreSQL',
            'mongo': 'spinta.backends.mongo.components:Mongo',
            'fs': 'spinta.backends.fs.components:FileSystem',

            # External backends
            # XXX: Probably these should be moved to components.resources?
            'sql': 'spinta.datasets.backends.sql.components:Sql',
            'sqldump': 'spinta.datasets.backends.sqldump.components:SqlDump',
            'csv': 'spinta.datasets.backends.dataframe.components:Csv',
            'xml': 'spinta.datasets.backends.dataframe.components:Xml',
            'json': 'spinta.datasets.backends.dataframe.components:Json',
            'xlsx': 'spinta.datasets.backends.notimpl.components:BackendNotImplemented',
            'geojson': 'spinta.datasets.backends.notimpl.components:BackendNotImplemented',
            'html': 'spinta.datasets.backends.notimpl.components:BackendNotImplemented',
        },
        'migrations': {
            'alembic': 'spinta.migrations.schema.alembic:Alembic',
        },
        'nodes': {
            'ns': 'spinta.components:Namespace',
            'model': 'spinta.components:Model',
            'dataset': 'spinta.datasets.components:Dataset',
            'base': 'spinta.components:Base',
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
            'time': 'spinta.types.datatype:Time',
            'datetime': 'spinta.types.datatype:DateTime',
            'temporal': 'spinta.types.datatype:DateTime',
            'string': 'spinta.types.datatype:String',
            'text': 'spinta.types.text.components:Text',
            'binary': 'spinta.types.datatype:Binary',
            'integer': 'spinta.types.datatype:Integer',
            'number': 'spinta.types.datatype:Number',
            'boolean': 'spinta.types.datatype:Boolean',
            'url': 'spinta.types.datatype:URL',
            'uri': 'spinta.types.datatype:URI',
            'image': 'spinta.types.datatype:Image',
            'geometry': 'spinta.types.geometry.components:Geometry',
            'spatial': 'spinta.types.geometry.components:Spatial',
            'ref': 'spinta.types.datatype:Ref',
            'backref': 'spinta.types.datatype:BackRef',
            'array_backref': 'spinta.types.datatype:ArrayBackRef',
            'generic': 'spinta.types.datatype:Generic',
            'array': 'spinta.types.datatype:Array',
            'object': 'spinta.types.datatype:Object',
            'file': 'spinta.types.datatype:File',
            'rql': 'spinta.types.datatype:RQL',
            'json': 'spinta.types.datatype:JSON',
            'denorm': 'spinta.types.datatype:Denorm',
            '_external_ref': 'spinta.types.datatype:ExternalRef',
            'inherit': 'spinta.types.datatype:Inherit',
            'page': 'spinta.types.datatype:PageType',
            'partial': 'spinta.types.datatype:Partial',
            'partial_array': 'spinta.types.datatype:PartialArray',
            'uuid': 'spinta.types.datatype:UUID',
        },
        'urlparams': {
            'component': 'spinta.urlparams:UrlParams',
        },
        'dimensions': {
            'prefix': 'spinta.dimensions.prefix.components:UriPrefix',
        },
    },
    'exporters': {
        'ascii': 'spinta.formats.ascii.components:Ascii',
        'csv': 'spinta.formats.csv.components:Csv',
        'json': 'spinta.formats.json.components:Json',
        'jsonl': 'spinta.formats.jsonlines.components:JsonLines',
        'html': 'spinta.formats.html.components:Html',
        'rdf': 'spinta.formats.rdf.components:Rdf',
        'xlsx': 'spinta.formats.xlsx.components:Xlsx',
    },
    'accesslog': {
        'type': 'file',
        'file': '/dev/null',
    },
    'manifests': {
        'default': {
            'type': 'memory',
            'backend': 'default',
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

    # Configuration path, where clients, keys and other configuration files are
    # stored. Defaults to ~/.config/spinta.
    'config_path': None,

    # Data path, where all data files are stored, like push state, keymap, etc.
    # Defaults to ~/.local/share/spinta.
    'data_path': None,

    # Path to documentation folder. If set will serve the folder at `/docs`.
    'docs_path': None,

    # Credentials file for remote Spinta instances.
    'credentials_file': None,

    'server_url': 'https://example.com/',

    # This prefix will be added to autogenerated scope names.
    'scope_prefix': 'spinta_',

    # Scope name formatter.
    'scope_formatter': 'spinta.auth:get_scope_name',

    # Part of the scope will be replaced with a hash to fit scope_max_length.
    'scope_max_length': 60,

    # If True, then scopes are added to access logs.
    'scope_log': True,

    # If True, then 'id' property is always included into Action.SEARCH result,
    # even if not explicitly asked.
    'always_show_id': False,

    'default_auth_client': 'default',

    # Public JWK key for validating auth bearer tokens.
    'token_validation_key': None,

    # Limit access to specified namespace root.
    'root': None,

    'env': 'prod',

    # Default limit of objects in a page
    # If None is given default is 100000
    'default_page_size': 100000,

    # Enable pagination by default for all requests (push/get)
    # If None is given default is True
    'enable_pagination': True,

    # Limit of objects in a page
    # If None is given default is 100000
    'sync_page_size': 100000,

    # Default languages
    # Top most popular EU languages + lt, gathered from https://en.wikipedia.org/wiki/List_of_languages_by_number_of_speakers_in_Europe
    # Last updated: 2023-11-08
    'languages': ['lt', 'ru', 'de', 'en', 'fr', 'it', 'es', 'pl', 'uk', 'ro', 'nl'],

    # Used to determine max file size (MB) that can be uploaded using API (to prevent DoS attacks)
    # If nothing is set, default is 100 MB
    'max_file_size': 100,

    # Used to determine max amount of errors can be thrown while writing, before canceling writing stream
    'max_error_count_on_insert': 100,

    'environments': {
        'dev': {
            'keymaps.default': {
                'type': 'sqlalchemy',
                'dsn': 'sqlite:///{data_dir}/keymap.db',
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
