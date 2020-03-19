import pathlib


CONFIG = {
    'environments': {
        'test': {
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
        },
    },
}
