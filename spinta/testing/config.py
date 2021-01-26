import pathlib
from typing import Optional

from spinta.auth import gen_auth_server_keys
from spinta.core.config import RawConfig
from spinta.testing.datasets import Sqlite
from spinta.testing.tabular import create_tabular_manifest
from spinta.manifests.tabular.helpers import striptable

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


def configure(
    rc: RawConfig,
    db: Optional[Sqlite],   # TODO: move backend configuration to manifest
    path: pathlib.Path,     # manifest file path
    manifest: str,          # manifest as ascii table string
) -> RawConfig:
    create_tabular_manifest(path, striptable(manifest))
    if db:
        return rc.fork({
            'manifests': {
                'default': {
                    'type': 'tabular',
                    'path': str(path),
                    'backend': 'sql',
                    'keymap': 'default',
                    'mode': 'external',
                },
            },
            'backends': {
                'sql': {
                    'type': 'sql',
                    'dsn': db.dsn,
                },
            },
            # tests/config/clients/3388ea36-4a4f-4821-900a-b574c8829d52.yml
            'default_auth_client': '3388ea36-4a4f-4821-900a-b574c8829d52',
        })
    else:
        return rc.fork({
            'manifests': {
                'default': {
                    'type': 'tabular',
                    'path': str(path),
                    'backend': 'default',
                    'keymap': 'default',
                    'mode': 'external',
                },
            },
            'backends': {
                'default': {
                    'type': 'memory',
                },
            },
            # tests/config/clients/3388ea36-4a4f-4821-900a-b574c8829d52.yml
            'default_auth_client': '3388ea36-4a4f-4821-900a-b574c8829d52',
        })
