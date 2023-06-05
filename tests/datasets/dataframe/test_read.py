import json
from pathlib import Path

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


def test_csv_read(rc: RawConfig, fs: AbstractFileSystem):
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


def test_csv_read_unknown_column(rc: RawConfig, fs: AbstractFileSystem):
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


def test_csv_read_refs(rc: RawConfig, fs: AbstractFileSystem):
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


def test_csv_read_multiple_models(rc: RawConfig, fs: AbstractFileSystem):
    fs.pipe('countries.csv', (
        'kodas,pavadinimas,id\n'
        'lt,Lietuva,1\n'
        'lv,Latvija,2\n'
        'ee,Estija,3'
    ).encode('utf-8'))

    fs.pipe('cities.csv', (
        'šalis,miestas\n'
        'lt,Vilnius\n'
        'lv,Ryga\n'
        'ee,Talin'
    ).encode('utf-8'))

    context, manifest = prepare_manifest(rc, '''
    d | r | b | m | property | type    | ref     | source                 | prepare | access
    example/countries        |         |         |                        |         |
      | csv0                 | csv     |         | memory://countries.csv |         |
      |   |   | Country      |         | code    |                        |         |
      |   |   |   | code     | string  |         | kodas                  |         | open
      |   |   |   | name     | string  |         | pavadinimas            |         | open
      |   |   |   | id       | integer |         | id                     |         | open
                             |         |         |                        |         |
      | csv1                 | csv     |         | memory://cities.csv    |         |
      |   |   | City         |         | miestas |                        |         |
      |   |   |   | miestas  | string  |         | miestas                |         | open
      |   |   |   | kodas    | string  |         | šalis                  |         | open
    ''', mode=Mode.external)
    context.loaded = True
    app = create_test_client(context)
    app.authmodel('example', ['getall'])

    resp = app.get('/example/countries/Country')
    assert listdata(resp, sort=False) == [
        ('lt', 1, 'Lietuva'),
        ('lv', 2, 'Latvija'),
        ('ee', 3, 'Estija'),
    ]

    resp = app.get('example/countries/City')
    assert listdata(resp, sort=False) == [
        ('lt', 'Vilnius'),
        ('lv', 'Ryga'),
        ('ee', 'Talin'),
    ]


def test_xml_read(rc: RawConfig, tmp_path: Path):
    xml = '''
        <cities>
            <city>
                <code>lt</code>
                <name>Vilnius</name>
            </city>
            <city>
                <code>lv</code>
                <name>Ryga</name>
            </city>
            <city>
                <code>ee</code>
                <name>Talin</name>
            </city>
        </cities>
    '''
    path = tmp_path / 'cities.xml'
    path.write_text(xml)

    context, manifest = prepare_manifest(rc, f'''
    d | r | b | m | property | type    | ref  | source              | prepare | access
    example                  |         |      |                     |         |
      | xml                  | xml     |      | {path}              |         |
      |   |   | City         |         | name | /cities/city        |         |
      |   |   |   | name     | string  |      | name                |         | open
      |   |   |   | country  | string  |      | code                |         | open
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


def test_xml_read_with_attributes(rc: RawConfig, tmp_path: Path):
    xml = '''
        <cities>
            <city code="lt" name="Vilnius"/>
            <city code="lv" name="Ryga"/>
            <city code="ee" name="Talin"/>
        </cities>
    '''
    path = tmp_path / 'cities.xml'
    path.write_text(xml)

    context, manifest = prepare_manifest(rc, f'''
    d | r | b | m | property | type    | ref  | source              | prepare | access
    example                  |         |      |                     |         |
      | xml                  | xml     |      | {path}              |         |
      |   |   | City         |         | name | /cities/city        |         |
      |   |   |   | name     | string  |      | @name               |         | open
      |   |   |   | country  | string  |      | @code               |         | open
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


def test_xml_read_refs_level_3(rc: RawConfig, tmp_path: Path):
    xml = '''
        <countries>
            <country>
                <code>lt</code>
                <name>Lietuva</name>
                <cities>
                    <city name="Vilnius"/>
                </cities>
            </country>
            <country>
                <code>lv</code>
                <name>Latvija</name>
                <cities>
                    <city name="Ryga"/>
                </cities>
            </country>
            <country>
                <code>ee</code>
                <name>Estija</name>
                <cities>
                    <city name="Talin"/>
                </cities>
            </country>
        </countries>
    '''
    path = tmp_path / 'cities.xml'
    path.write_text(xml)

    context, manifest = prepare_manifest(rc, f'''
       d | r | b | m | property | type    | ref     | source                               | prepare | access | level
       example                  |         |         |                                      |         |        |
         | xml                  | xml     |         | {path}                               |         |        |
         |   |   | City         |         | name    | /countries/country/cities/city       |         |        |
         |   |   |   | name     | string  |         | @name                                |         | open   |
         |   |   |   | code     | ref     | Country | ../../code                           |         | open   | 3
                     | 
         |   |   | Country      |         | country | /countries/country                   |         |        |
         |   |   |   | name     | string  |         | name                                 |         | open   |
         |   |   |   | country  | string  |         | code                                 |         | open   |
        ''', mode=Mode.external)
    context.loaded = True
    app = create_test_client(context)
    app.authmodel('example', ['getall'])

    resp = app.get('/example/City')
    assert listdata(resp, sort=False) == [
        ('lt', 'Vilnius'),
        ('lv', 'Ryga'),
        ('ee', 'Talin'),
    ]


def test_xml_read_refs_level_4(rc: RawConfig, tmp_path: Path):
    xml = '''
        <countries>
            <country>
                <code>lt</code>
                <name>Lietuva</name>
                <cities>
                    <city name="Vilnius"/>
                </cities>
            </country>
            <country>
                <code>lv</code>
                <name>Latvija</name>
                <cities>
                    <city name="Ryga"/>
                </cities>
            </country>
            <country>
                <code>ee</code>
                <name>Estija</name>
                <cities>
                    <city name="Talin"/>
                </cities>
            </country>
        </countries>
    '''
    path = tmp_path / 'cities.xml'
    path.write_text(xml)

    context, manifest = prepare_manifest(rc, f'''
       d | r | b | m | property | type    | ref     | source                               | prepare | access | level
       example                  |         |         |                                      |         |        |
         | xml                  | xml     |         | {path}                               |         |        |
         |   |   | City         |         | name    | /countries/country/cities/city       |         |        |
         |   |   |   | name     | string  |         | @name                                |         | open   |
         |   |   |   | code     | ref     | Country | ../..                                |         | open   | 4
                     | 
         |   |   | Country      |         | country | /countries/country                   |         |        |
         |   |   |   | name     | string  |         | name                                 |         | open   |
         |   |   |   | country  | string  |         | code                                 |         | open   |
        ''', mode=Mode.external)
    context.loaded = True
    app = create_test_client(context)
    app.authmodel('example', ['getall'])

    resp = app.get('/example/Country')
    countries = {
        c['country']: c['_id']
        for c in listdata(resp, '_id', 'country', full=True)
    }
    assert sorted(countries) == ['ee', 'lt', 'lv']

    resp = app.get('/example/City')
    assert listdata(resp, sort=False) == [
        (countries['lt'], 'Vilnius'),
        (countries['lv'], 'Ryga'),
        (countries['ee'], 'Talin'),
    ]


def test_xml_read_multiple_sources(rc: RawConfig, tmp_path: Path):
    xml0 = '''
        <cities>
            <city name="Vilnius" code="lt"/>
            <city name="Ryga" code="lv"/>
            <city name="Talin" code="ee"/>
        </cities>
    '''
    path0 = tmp_path / 'cities.xml'
    path0.write_text(xml0)

    xml1 = '''
        <countries>
            <country code="lt" name="Lietuva"/>
            <country code="lv" name="Latvija"/>
            <country code="ee" name="Estija"/>
        </countries>
    '''
    path1 = tmp_path / 'countries.xml'
    path1.write_text(xml1)

    context, manifest = prepare_manifest(rc, f'''
       d | r | b | m | property | type    | ref     | source             | prepare | access | level
       example                  |         |         |                    |         |        |
         | xml_city             | xml     |         | {path0}            |         |        |
         |   |   | City         |         | name    | /cities/city       |         |        |
         |   |   |   | name     | string  |         | @name              |         | open   |
         |   |   |   | code     | string  |         | @code              |         | open   | 3
                     | 
         | xml_country          | xml     |         | {path1}            |         |        |
         |   |   | Country      |         | code    | /countries/country |         |        |
         |   |   |   | name     | string  |         | @name              |         | open   |
         |   |   |   | code     | string  |         | @code              |         | open   |
        ''', mode=Mode.external)
    context.loaded = True
    app = create_test_client(context)
    app.authmodel('example', ['getall'])

    resp = app.get('/example/City')
    assert listdata(resp, sort=False) == [
        ('lt', 'Vilnius'),
        ('lv', 'Ryga'),
        ('ee', 'Talin'),
    ]

    resp = app.get('/example/Country')
    assert listdata(resp, sort=False) == [
        ('lt', 'Lietuva'),
        ('lv', 'Latvija'),
        ('ee', 'Estija'),
    ]


def test_json_read(rc: RawConfig, tmp_path: Path):
    json_manifest = {
        "countries": [
            {
                "code": "lt",
                "name": "Lietuva",
            },
            {
                "code": "lv",
                "name": "Latvija",
            },
            {
                "code": "ee",
                "name": "Estija",
            },
        ]
    }
    path = tmp_path / 'countries.json'
    path.write_text(json.dumps(json_manifest))

    context, manifest = prepare_manifest(rc, f'''
    d | r | m              | property | type   | ref     | source                | level        
           example                    |        |         |                       |
             | resource               | json   |         | {path}                |
                                      |        |         |                       |
             |   | Country |          |        | code    | countries             |
             |   |         | name     | string |         | name                  |
             |   |         | code     | string |         | code                  |
    ''', mode=Mode.external)
    context.loaded = True
    app = create_test_client(context)
    app.authmodel('example/Country', ['getall'])

    resp = app.get('/example/Country')
    assert listdata(resp, sort=False) == [
        ('lt', 'Lietuva'),
        ('lv', 'Latvija'),
        ('ee', 'Estija'),
    ]


def test_json_read_nested(rc: RawConfig, tmp_path: Path):
    json_manifest = {
        "countries": [
            {
                "code": "lt",
                "name": "Lietuva",
                "location": {
                    "lat": 0,
                    "lon": 1
                }
            },
            {
                "code": "lv",
                "name": "Latvija",
                "location": {
                    "lat": 2,
                    "lon": 3
                }
            },
            {
                "code": "ee",
                "name": "Estija",
                "location": {
                    "lat": 4,
                    "lon": 5
                }
            },
        ]
    }
    path = tmp_path / 'countries.json'
    path.write_text(json.dumps(json_manifest))

    context, manifest = prepare_manifest(rc, f'''
    d | r | m              | property  | type    | ref     | source                | level        
           example                     |         |         |                       |
             | resource                | json    |         | {path}                |
                                       |         |         |                       |
             |   | Country |           |         | code    | countries             |
             |   |         | name      | string  |         | name                  |
             |   |         | code      | string  |         | code                  |
             |   |         | latitude  | integer |         | location.lat          |
             |   |         | longitude | integer |         | location.lon          |
    ''', mode=Mode.external)
    context.loaded = True
    app = create_test_client(context)
    app.authmodel('example/Country', ['getall'])

    resp = app.get('/example/Country')
    assert listdata(resp, sort=False) == [
        ('lt', 0, 1, 'Lietuva'),
        ('lv', 2, 3, 'Latvija'),
        ('ee', 4, 5, 'Estija'),
    ]


def test_json_read_multi_nested(rc: RawConfig, tmp_path: Path):
    json_manifest = {
        "galaxy": {
            "name": "Milky",
            "solar_system": {
                "name": "Solar",
                "planet": {
                    "name": "Earth",
                    "countries": [
                        {
                            "code": "lt",
                            "name": "Lietuva",
                            "location": {
                                "lat": 0,
                                "lon": 1
                            }
                        },
                        {
                            "code": "lv",
                            "name": "Latvija",
                            "location": {
                                "lat": 2,
                                "lon": 3
                            }
                        },
                        {
                            "code": "ee",
                            "name": "Estija",
                            "location": {
                                "lat": 4,
                                "lon": 5
                            }
                        },
                    ]
                }
            }
        }

    }
    path = tmp_path / 'countries.json'
    path.write_text(json.dumps(json_manifest))

    context, manifest = prepare_manifest(rc, f'''
    d | r | m              | property  | type    | ref     | source
           example                     |         |         |
             | resource                | json    |         | {path}
                                       |         |         |
             |   | Country |           |         | code    | galaxy.solar_system.planet.countries
             |   |         | name      | string  |         | name
             |   |         | code      | string  |         | code
             |   |         | latitude  | integer |         | location.lat
             |   |         | longitude | integer |         | location.lon
             |   |         | galaxy    | string  |         | ....name
             |   |         | system    | string  |         | ...name
             |   |         | planet    | string  |         | ..name
    ''', mode=Mode.external)
    context.loaded = True
    app = create_test_client(context)
    app.authmodel('example/Country', ['getall'])

    resp = app.get('/example/Country')
    assert listdata(resp, sort=False, full=True) == [
        {
            'name': 'Lietuva',
            'code': 'lt',
            'latitude': 0,
            'longitude': 1,
            'galaxy': "Milky",
            "system": "Solar",
            "planet": "Earth"
        },
        {
            'name': 'Latvija',
            'code': 'lv',
            'latitude': 2,
            'longitude': 3,
            'galaxy': "Milky",
            "system": "Solar",
            "planet": "Earth"
        },
        {
            'name': 'Estija',
            'code': 'ee',
            'latitude': 4,
            'longitude': 5,
            'galaxy': "Milky",
            "system": "Solar",
            "planet": "Earth"
        }
    ]


def test_json_read_blank_node_list(rc: RawConfig, tmp_path: Path):
    json_manifest = [
        {
            "code": "lt",
            "name": "Lietuva",
        },
        {
            "code": "lv",
            "name": "Latvija",
        },
        {
            "code": "ee",
            "name": "Estija",
        },
    ]

    path = tmp_path / 'countries.json'
    path.write_text(json.dumps(json_manifest))

    context, manifest = prepare_manifest(rc, f'''
    d | r | m              | property | type   | ref     | source                | level        
           example                    |        |         |                       |
             | resource               | json   |         | {path}                |
                                      |        |         |                       |
             |   | Country |          |        | code    | .                     |
             |   |         | name     | string |         | name                  |
             |   |         | code     | string |         | code                  |
    ''', mode=Mode.external)
    context.loaded = True
    app = create_test_client(context)
    app.authmodel('example/Country', ['getall'])

    resp = app.get('/example/Country')
    assert listdata(resp, sort=False) == [
        ('lt', 'Lietuva'),
        ('lv', 'Latvija'),
        ('ee', 'Estija'),
    ]


def test_json_read_blank_node_single_level(rc: RawConfig, tmp_path: Path):
    json_manifest = {
        "id": 0,
        "countries": [
            {
                "code": "lt",
                "name": "Lietuva",
            },
            {
                "code": "lv",
                "name": "Latvija",
            },
            {
                "code": "ee",
                "name": "Estija",
            },
        ]
    }

    path = tmp_path / 'countries.json'
    path.write_text(json.dumps(json_manifest))

    context, manifest = prepare_manifest(rc, f'''
    d | r | m              | property | type    | ref     | source                | level        
           example                    |         |         |                       |
             | resource               | json    |         | {path}                |
                                      |         |         |                       |
             |   | Country |          |         | code    | countries             |
             |   |         | name     | string  |         | name                  |
             |   |         | code     | string  |         | code                  |
             |   |         | id       | integer |         | ..id                  |       
    ''', mode=Mode.external)
    context.loaded = True
    app = create_test_client(context)
    app.authmodel('example/Country', ['getall'])

    resp = app.get('/example/Country')
    assert listdata(resp, sort=False) == [
        ('lt', 0, 'Lietuva'),
        ('lv', 0, 'Latvija'),
        ('ee', 0, 'Estija'),
    ]


def test_json_read_blank_node_multi_level(rc: RawConfig, tmp_path: Path):
    json_manifest = {
        "id": 0,
        "galaxy": {
            "name": "Milky",
            "solar_system": {
                "name": "Solar",
                "planet": {
                    "name": "Earth",
                    "countries": [
                        {
                            "code": "lt",
                            "name": "Lietuva",
                            "location": {
                                "lat": 0,
                                "lon": 1
                            }
                        },
                        {
                            "code": "lv",
                            "name": "Latvija",
                            "location": {
                                "lat": 2,
                                "lon": 3
                            }
                        },
                        {
                            "code": "ee",
                            "name": "Estija",
                            "location": {
                                "lat": 4,
                                "lon": 5
                            }
                        },
                    ]
                }
            }
        }
    }

    path = tmp_path / 'countries.json'
    path.write_text(json.dumps(json_manifest))

    context, manifest = prepare_manifest(rc, f'''
    d | r | m              | property  | type    | ref     | source   
           example                     |         |         |
             | resource                | json    |         | {path}
                                       |         |         |
             |   | Country |           |         | code    | galaxy.solar_system.planet.countries
             |   |         | name      | string  |         | name
             |   |         | code      | string  |         | code
             |   |         | latitude  | integer |         | location.lat
             |   |         | longitude | integer |         | location.lon
             |   |         | id        | integer |         | .....id  
             |   |         | galaxy    | string  |         | ....name
             |   |         | system    | string  |         | ...name
             |   |         | planet    | string  |         | ..name     
    ''', mode=Mode.external)
    context.loaded = True
    app = create_test_client(context)
    app.authmodel('example/Country', ['getall'])

    resp = app.get('/example/Country')
    assert listdata(resp, sort=False, full=True) == [
        {
            'name': 'Lietuva',
            'code': 'lt',
            'latitude': 0,
            'longitude': 1,
            'galaxy': "Milky",
            "system": "Solar",
            "planet": "Earth",
            "id": 0
        },
        {
            'name': 'Latvija',
            'code': 'lv',
            'latitude': 2,
            'longitude': 3,
            'galaxy': "Milky",
            "system": "Solar",
            "planet": "Earth",
            "id": 0
        },
        {
            'name': 'Estija',
            'code': 'ee',
            'latitude': 4,
            'longitude': 5,
            'galaxy': "Milky",
            "system": "Solar",
            "planet": "Earth",
            "id": 0
        }
    ]


def test_json_read_ref_level_3(rc: RawConfig, tmp_path: Path):
    json_manifest = {
        "countries": [
            {
                "code": "lt",
                "name": "Lietuva",
                "cities": [
                    {
                        "name": "Vilnius"
                    }
                ]
            },
            {
                "code": "lv",
                "name": "Latvija",
                "cities": [
                    {
                        "name": "Ryga"
                    }
                ]
            },
            {
                "code": "ee",
                "name": "Estija",
                "cities": [
                    {
                        "name": "Talin"
                    }
                ]
            },
        ]
    }
    path = tmp_path / 'countries.json'
    path.write_text(json.dumps(json_manifest))

    context, manifest = prepare_manifest(rc, f'''
    d | r | m              | property | type   | ref     | source                | level        
           example                    |        |         |                       |
             | resource               | json   |         | {path}                |
                                      |        |         |                       |
             |   | Country |          |        | code    | countries             |
             |   |         | name     | string |         | name                  |
             |   |         | code     | string |         | code                  |
                                      |        |         |                       |
             |   | City    |          |        |         | countries[].cities    |
             |   |         | name     | string |         | name                  |
             |   |         | country  | ref    | Country | ..                    | 3
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


def test_json_read_ref_level_4(rc: RawConfig, tmp_path: Path):
    json_manifest = {
        "countries": [
            {
                "code": "lt",
                "name": "Lietuva",
                "cities": [
                    {
                        "name": "Vilnius"
                    }
                ]
            },
            {
                "code": "lv",
                "name": "Latvija",
                "cities": [
                    {
                        "name": "Ryga"
                    }
                ]
            },
            {
                "code": "ee",
                "name": "Estija",
                "cities": [
                    {
                        "name": "Talin"
                    }
                ]
            },
        ]
    }
    path = tmp_path / 'countries.json'
    path.write_text(json.dumps(json_manifest))

    context, manifest = prepare_manifest(rc, f'''
    d | r | m              | property | type   | ref     | source                | level        
           example                    |        |         |                       |
             | resource               | json   |         | {path}                |
                                      |        |         |                       |
             |   | Country |          |        | code    | countries             |
             |   |         | name     | string |         | name                  |
             |   |         | code     | string |         | code                  |
                                      |        |         |                       |
             |   | City    |          |        |         | countries[].cities    |
             |   |         | name     | string |         | name                  |
             |   |         | country  | ref    | Country | ..                    | 4
    ''', mode=Mode.external)
    context.loaded = True
    app = create_test_client(context)
    app.authmodel('example', ['getall'])

    resp = app.get('/example/Country')
    countries = {
        c['code']: c['_id']
        for c in listdata(resp, '_id', 'code', full=True)
    }
    assert sorted(countries) == ['ee', 'lt', 'lv']

    resp = app.get('/example/City')
    assert listdata(resp, sort=False) == [
        (countries['lt'], 'Vilnius'),
        (countries['lv'], 'Ryga'),
        (countries['ee'], 'Talin'),
    ]


def test_json_read_multiple_sources(rc: RawConfig, tmp_path: Path):
    json0 = {
        "countries": [
            {
                "code": "lt",
                "name": "Lietuva",
            },
            {
                "code": "lv",
                "name": "Latvija",
            },
            {
                "code": "ee",
                "name": "Estija",
            },
        ]
    }
    path0 = tmp_path / 'cities.json'
    path0.write_text(json.dumps(json0))

    json1 = {
        "cities": [
            {
                "code": "lt",
                "name": "Vilnius"
            },
            {
                "code": "lv",
                "name": "Ryga"
            },
            {
                "code": "ee",
                "name": "Talin"
            }
        ]
    }
    path1 = tmp_path / 'countries.json'
    path1.write_text(json.dumps(json1))

    context, manifest = prepare_manifest(rc, f'''
       d | r | b | m | property | type    | ref     | source    | prepare | access | level
       example                  |         |         |           |         |        |
         | json_city            | json    |         | {path1}   |         |        |
         |   |   | City         |         | name    | cities    |         |        |
         |   |   |   | name     | string  |         | name      |         | open   |
         |   |   |   | code     | string  |         | code      |         | open   |
                     |
         | json_country         | json    |         | {path0}   |         |        |
         |   |   | Country      |         | code    | countries |         |        |
         |   |   |   | name     | string  |         | name      |         | open   |
         |   |   |   | code     | string  |         | code      |         | open   |
        ''', mode=Mode.external)
    context.loaded = True
    app = create_test_client(context)
    app.authmodel('example', ['getall'])

    resp = app.get('/example/City')
    assert listdata(resp, sort=False) == [
        ('lt', 'Vilnius'),
        ('lv', 'Ryga'),
        ('ee', 'Talin'),
    ]

    resp = app.get('/example/Country')
    assert listdata(resp, sort=False) == [
        ('lt', 'Lietuva'),
        ('lv', 'Latvija'),
        ('ee', 'Estija'),
    ]


def test_json_read_with_empty(rc: RawConfig, tmp_path: Path):
    json_manifest = {
        "countries": [
            {
                "code": "lt",
            },
            {
                "code": "lv",
                "name": "Latvija",
            },
            {
                "name": "Estija",
            },
        ]
    }
    path = tmp_path / 'countries.json'
    path.write_text(json.dumps(json_manifest))

    context, manifest = prepare_manifest(rc, f'''
    d | r | m              | property | type   | ref     | source                | level        
           example                    |        |         |                       |
             | resource               | json   |         | {path}                |
                                      |        |         |                       |
             |   | Country |          |        | code    | countries             |
             |   |         | name     | string |         | name                  |
             |   |         | code     | string |         | code                  |
    ''', mode=Mode.external)
    context.loaded = True
    app = create_test_client(context)
    app.authmodel('example/Country', ['getall'])

    resp = app.get('/example/Country')
    assert listdata(resp, sort=False) == [
        ('lt', None),
        ('lv', 'Latvija'),
        (None, 'Estija'),
    ]


def test_json_read_with_empty_nested(rc: RawConfig, tmp_path: Path):
    json_manifest = {
        "countries": [
            {
                "code": "lt",
                "name": "Lietuva",
            },
            {
                "name": "Latvija",
                "location": {
                    "lon": 3
                }
            },
            {
                "code": "ee",
                "location": {
                    "lat": 4,
                    "lon": 5
                }
            },
        ]
    }
    path = tmp_path / 'countries.json'
    path.write_text(json.dumps(json_manifest))

    context, manifest = prepare_manifest(rc, f'''
    d | r | m              | property  | type    | ref     | source                | level        
           example                     |         |         |                       |
             | resource                | json    |         | {path}                |
                                       |         |         |                       |
             |   | Country |           |         | code    | countries             |
             |   |         | name      | string  |         | name                  |
             |   |         | code      | string  |         | code                  |
             |   |         | latitude  | integer |         | location.lat          |
             |   |         | longitude | integer |         | location.lon          |
    ''', mode=Mode.external)
    context.loaded = True
    app = create_test_client(context)
    app.authmodel('example/Country', ['getall'])

    resp = app.get('/example/Country')
    assert listdata(resp, sort=False) == [
        ('lt', None, None, 'Lietuva'),
        (None, None, 3, 'Latvija'),
        ('ee', 4, 5, None),
    ]
