import pathlib

from spinta.auth import gen_auth_server_keys


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


def create_config_path(path: pathlib.Path) -> pathlib.Path:
    path.mkdir(exist_ok=True)
    (path / 'clients').mkdir(exist_ok=True)
    gen_auth_server_keys(path, exist_ok=True)
    return path
