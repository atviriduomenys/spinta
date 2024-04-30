from pathlib import Path

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


def test_xsd_base64(rc: RawConfig, tmp_path: Path):
    xml = """<CITY>
    <NAME>Kaunas</NAME>
    <COUNTRY>Lietuva</COUNTRY>
    <FILE>VGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIDEzIGxhenkgZG9ncy4=</FILE>
    </CITY>"""

    path = tmp_path / 'cities.xml'
    path.write_text(xml)

    context, manifest = prepare_manifest(rc, f'''
    d | r | b | m | property | type    | ref  | source              | prepare          | access
    example                  |         |      |                     |                  |
      | res1                 | xml     |      | {tmp_path}/cities.xml |                  |
      |   |   | City         |         |      | /CITY               |                  |
      |   |   |   | name     | string  |      | NAME                |                  | open
      |   |   |   | country  | string  |      | COUNTRY             |                  | open
      |   |   |   | file     | binary  |      | FILE                | base64()         | open
    ''', mode=Mode.external)
    #
    #       /example/City?select(name, country, base64(file))
    context.loaded = True
    app = create_test_client(context)
    app.authmodel('example/City', ['getall'])

    resp = app.get('/example/City')
    assert listdata(resp, sort=False) == [
        ('Lietuva', 'Kaunas', "01010100 01101000 01100101 00100000 01110001 01110101 01101001 01100011 01101011 00100000 01100010 01110010 01101111 01110111 01101110 00100000 01100110 01101111 01111000 00100000 01101010 01110101 01101101 01110000 01110011 00100000 01101111 01110110 01100101 01110010 00100000 00110001 00110011 00100000 01101100 01100001 01111010 01111001 00100000 01100100 01101111 01100111 01110011 00101110")
    ]
