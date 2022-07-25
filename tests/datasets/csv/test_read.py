import pytest
from fsspec import AbstractFileSystem
from fsspec.implementations.memory import MemoryFileSystem

from spinta.components import Mode
from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import prepare_manifest
from spinta.testing.utils import error


@pytest.fixture
def fs():
    fs = MemoryFileSystem()
    yield fs
    MemoryFileSystem.store.clear()


def test_read(rc: RawConfig, fs: AbstractFileSystem):
    fs.pipe('cities.csv', (
        'šalis,miestas\n'
        'lt,Vilnius\n'
        'lv,Ryga\n'
        'ee,Talin'
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
    assert listdata(resp, sort=False) == [
        ('lt', 'Vilnius'),
        ('lv', 'Ryga'),
        ('ee', 'Talin'),
    ]


def test_read_unknown_column(rc: RawConfig, fs: AbstractFileSystem):
    fs.pipe('cities.csv', (
        'šalis,miestas\n'
        'lt,Vilnius\n'
        'lv,Ryga\n'
        'ee,Talin'
    ).encode('utf-8'))

    context, manifest = prepare_manifest(rc, '''
    d | r | b | m | property | type    | source              | prepare | access
    example                  |         |                     |         |
      | csv                  | csv     | memory://cities.csv |         |
      |   |   | City         |         |                     |         |
      |   |   |   | name     | string  | miestas             |         | open
      |   |   |   | country  | string  | salis               |         | open
    ''', mode=Mode.external)
    context.loaded = True
    app = create_test_client(context)
    app.authmodel('example/City', ['getall'])

    resp = app.get('/example/City')
    assert error(resp) == 'PropertyNotFound'


def test_read_refs(rc: RawConfig, fs: AbstractFileSystem):
    fs.pipe('countries.csv', (
        'kodas,pavadinimas\n'
        'lt,Lietuva\n'
        'lv,Latvija\n'
        'ee,Estija'
    ).encode('utf-8'))

    fs.pipe('cities.csv', (
        'šalis,miestas\n'
        'lt,Vilnius\n'
        'lv,Ryga\n'
        'ee,Talin'
    ).encode('utf-8'))

    context, manifest = prepare_manifest(rc, '''
    d | r | b | m | property | type   | ref                        | source                 | prepare | access
    example/countries        |        |                            |                        |         |
      | csv                  | csv    |                            | memory://countries.csv |         |
      |   |   | Country      |        | code                       |                        |         |
      |   |   |   | code     | string |                            | kodas                  |         | open
      |   |   |   | name     | string |                            | pavadinimas            |         | open
    example/cities           |        |                            |                        |         |
      | csv                  | csv    |                            | memory://cities.csv    |         |
      |   |   | City         |        | name                       |                        |         |
      |   |   |   | name     | string |                            | miestas                |         | open
      |   |   |   | country  | ref    | /example/countries/Country | šalis                  |         | open
    ''', mode=Mode.external)
    context.loaded = True
    app = create_test_client(context)
    app.authmodel('example', ['getall'])

    resp = app.get('/example/countries/Country')
    countries = {
        c['code']: c['_id']
        for c in listdata(resp, '_id', 'code', full=True)
    }
    assert sorted(countries) == ['ee', 'lt', 'lv']

    resp = app.get('/example/cities/City')
    assert listdata(resp, sort=False) == [
        (countries['lt'], 'Vilnius'),
        (countries['lv'], 'Ryga'),
        (countries['ee'], 'Talin'),
    ]
