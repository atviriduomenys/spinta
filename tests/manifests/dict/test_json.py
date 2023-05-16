import json

from spinta.core.config import RawConfig

from pathlib import Path

from spinta.testing.manifest import load_manifest, compare_manifest


def test_json_normal(rc: RawConfig, tmp_path: Path):
    json_manifest = {
        "country": [
            {
                "name": "Lithuania",
                "code": "LT",
                "cities": [
                    {
                        "name": "Vilnius"
                    },
                    {
                        "name": "Kaunas"
                    }
                ]
            },
            {
                "name": "Latvia",
                "code": "LV",
                "cities": [
                    {
                        "name": "Riga"
                    }
                ]
            }
        ]
    }
    path = tmp_path / 'manifest.json'
    path.write_text(json.dumps(json_manifest))

    manifest = load_manifest(rc, path)
    manifest.datasets["dataset"].resources["resource"].external = "manifest.json"
    a, b = compare_manifest(manifest, f'''
id | d | r | b | m | property | type                   | ref     | source        | prepare | level | access | uri | title | description
   | dataset                  |                        |         |               |         |       |        |     |       |
   |   | resource             | json                   |         | manifest.json |         |       |        |     |       |
   |                          |                        |         |               |         |       |        |     |       |
   |   |   |   | Country      |                        |         | country       |         |       |        |     |       |
   |   |   |   |   | name     | string unique required |         | name          |         |       |        |     |       |
   |   |   |   |   | code     | string unique required |         | code          |         |       |        |     |       |
   |                          |                        |         |               |         |       |        |     |       |
   |   |   |   | Cities       |                        |         | cities        |         |       |        |     |       |
   |   |   |   |   | name     | string unique required |         | name          |         |       |        |     |       |
   |   |   |   |   | country  | ref                    | Country | country       |         |       |        |     |       |
''')
    assert a == b


def test_json_blank_node(rc: RawConfig, tmp_path: Path):
    json_manifest = [
        {
            "name": "Lithuania",
            "code": "LT",
            "cities": [
                {
                    "name": "Vilnius"
                },
                {
                    "name": "Kaunas"
                }
            ]
        },
        {
            "name": "Latvia",
            "code": "LV",
            "cities": [
                {
                    "name": "Riga"
                }
            ]
        }
    ]
    path = tmp_path / 'manifest.json'
    path.write_text(json.dumps(json_manifest))

    manifest = load_manifest(rc, path)
    manifest.datasets["dataset"].resources["resource"].external = "manifest.json"
    a, b = compare_manifest(manifest, f'''
id | d | r | b | m | property | type                   | ref     | source        | prepare | level | access | uri | title | description
   | dataset                  |                        |         |               |         |       |        |     |       |
   |   | resource             | json                   |         | manifest.json |         |       |        |     |       |
   |                          |                        |         |               |         |       |        |     |       |
   |   |   |   | Model1       |                        |         | .             |         |       |        |     |       |
   |   |   |   |   | name     | string unique required |         | name          |         |       |        |     |       |
   |   |   |   |   | code     | string unique required |         | code          |         |       |        |     |       |
   |                          |                        |         |               |         |       |        |     |       |
   |   |   |   | Cities       |                        |         | cities        |         |       |        |     |       |
   |   |   |   |   | name     | string unique required |         | name          |         |       |        |     |       |
   |   |   |   |   | parent   | ref                    | Model1  | ..            |         |       |        |     |       |
''')
    assert a == b


def test_json_blank_node_inherit(rc: RawConfig, tmp_path: Path):
    json_manifest = [
        {
            "name": "Lithuania",
            "code": "LT",
            "location": {
                "latitude": 54.5,
                "longitude": 12.6
            },
            "cities": [
                {
                    "name": "Vilnius",
                    "weather": {
                        "temperature": 24.7,
                        "wind_speed": 12.4
                    }
                },
                {
                    "name": "Kaunas",
                    "weather": {
                        "temperature": 29.7,
                        "wind_speed": 11.4
                    }
                }
            ]
        },
        {
            "name": "Latvia",
            "code": "LV",
            "cities": [
                {
                    "name": "Riga"
                }
            ]
        }
    ]
    path = tmp_path / 'manifest.json'
    path.write_text(json.dumps(json_manifest))

    manifest = load_manifest(rc, path)
    manifest.datasets["dataset"].resources["resource"].external = "manifest.json"
    a, b = compare_manifest(manifest, f'''
id | d | r | b | m | property            | type                   | ref     | source              | prepare | level | access | uri | title | description
   | dataset                             |                        |         |                     |         |       |        |     |       |
   |   | resource                        | json                   |         | manifest.json       |         |       |        |     |       |
   |                                     |                        |         |                     |         |       |        |     |       |
   |   |   |   | Model1                  |                        |         | .                   |         |       |        |     |       |
   |   |   |   |   | name                | string unique required |         | name                |         |       |        |     |       |
   |   |   |   |   | code                | string unique required |         | code                |         |       |        |     |       |
   |   |   |   |   | location_latitude   | number unique          |         | location.latitude   |         |       |        |     |       |
   |   |   |   |   | location_longitude  | number unique          |         | location.longitude  |         |       |        |     |       |
   |                                     |                        |         |                     |         |       |        |     |       |
   |   |   |   | Cities                  |                        |         | cities              |         |       |        |     |       |
   |   |   |   |   | name                | string unique required |         | name                |         |       |        |     |       |
   |   |   |   |   | weather_temperature | number unique          |         | weather.temperature |         |       |        |     |       |
   |   |   |   |   | weather_wind_speed  | number unique          |         | weather.wind_speed  |         |       |        |     |       |
   |   |   |   |   | parent              | ref                    | Model1  | ..                  |         |       |        |     |       |
''')
    assert a == b
