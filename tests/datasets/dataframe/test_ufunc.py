import pytest
from fsspec import AbstractFileSystem
from fsspec.implementations.memory import MemoryFileSystem

from spinta.components import Mode
from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import prepare_manifest


@pytest.fixture
def fs():
    fs = MemoryFileSystem()
    yield fs
    MemoryFileSystem.store.clear()


def test_csv_tabular_seperator_default_error(rc: RawConfig, fs: AbstractFileSystem):
    fs.pipe('cities.csv', (
        'šalis;miestas\n'
        'lt;Vilnius\n'
        'lv;Ryga\n'
        'ee;Talin'
    ).encode('utf-8'))

    context, manifest = prepare_manifest(rc, '''
    d | r | b | m | property | type    | ref  | source              | prepare | access
    example                  |         |      |                     |         |
      | csv                  | csv     |      | memory://cities.csv |         |
      |   |   | City         |         | name |                     |         |
      |   |   |   | name     | string  |      | miestas             |         | open
      |   |   |   | country  | string  |      | šalis               |         | open
    ''', mode=Mode.external)
    context.loaded = True
    app = create_test_client(context)
    app.authmodel('example/City', ['getall'])

    resp = app.get('/example/City')
    assert resp.status_code == 404


def test_csv_tabular_seperator(rc: RawConfig, fs: AbstractFileSystem):
    fs.pipe('cities.csv', (
        'šalis;miestas\n'
        'lt;Vilnius\n'
        'lv;Ryga\n'
        'ee;Talin'
    ).encode('utf-8'))

    context, manifest = prepare_manifest(rc, '''
    d | r | b | m | property | type    | ref  | source              | prepare          | access
    example                  |         |      |                     |                  |
      | csv                  | csv     |      | memory://cities.csv | tabular(sep:";") |
      |   |   | City         |         | name |                     |                  |
      |   |   |   | name     | string  |      | miestas             |                  | open
      |   |   |   | country  | string  |      | šalis               |                  | open
    ''', mode=Mode.external)
    context.loaded = True
    app = create_test_client(context)
    app.authmodel('example/City', ['getall'])

    resp = app.get('/example/City')
    assert listdata(resp, sort=False) == [
        ('lt', 'Vilnius'),
        ('lv', 'Ryga'),
        ('ee', 'Talin'),
    ]
