import pathlib

import pytest

from spinta.config import Config
from spinta.testing import CONFIG

pytest_plugins = [
    'spinta.testing.pytest',
]


@pytest.fixture(scope='session')
def config(postgresql, mongo):
    config = Config()

    # Add default configuration for tests.
    config.add(CONFIG)

    # Add test configuration for this project.
    config.add({
        'backends': {
            'default': {
                'backend': 'spinta.backends.postgresql:PostgreSQL',
                'dsn': postgresql,
            },
            'mongo': {
                'backend': 'spinta.backends.mongo:Mongo',
                'dsn': mongo,
                'dbName': 'spinta_test',
            },
        },
        'manifests': {
            'default': {
                'path': pathlib.Path(__file__).parent / 'manifest',
            },
        },
    })

    return config
