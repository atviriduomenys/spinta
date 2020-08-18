from pathlib import Path

from spinta.core.config import RawConfig
from spinta.testing.client import TestClient
from spinta.testing.client import create_test_client
from spinta.testing.context import create_test_context
from spinta.testing.utils import create_manifest_files


def _create_host_client(
    rc: RawConfig,
    path: Path,
    host: str,
    schema: dict,
) -> TestClient:
    create_manifest_files(path / host, schema)
    context = create_test_context(rc)
    return create_test_client(context, host=f'{host}.example.com')


def test_multihosts(rc: RawConfig, tmpdir: Path):
    rc = rc.fork({
        'manifest': 'default',
        'manifests.default': {
            'type': 'yaml',
            'path': str(tmpdir / 'host1'),
        },
        'manifests.host2': {
            'type': 'yaml',
            'path': str(tmpdir / 'host2'),
            'host': 'host2.example.com',
        },
    })

    _create_host_client(rc, tmpdir, 'host1', {
        'country.yml': {
            'type': 'model',
            'name': 'country',
            'properties': {
                'name': {'type': 'string'},
            },
        },
    })

    _create_host_client(rc, tmpdir, 'host2', {
        'city.yml': {
            'type': 'model',
            'name': 'country',
            'properties': {
                'name': {'type': 'string'},
            },
        },
    })
