import json

from spinta import commands
from spinta.core.config import RawConfig

from pathlib import Path

from spinta.testing.manifest import compare_manifest, load_manifest_and_context


def test_json_normal(rc: RawConfig, tmp_path: Path):
    json_manifest = {
        "country": [
            {"name": "Lithuania", "code": "LT", "cities": [{"name": "Vilnius"}, {"name": "Kaunas"}]},
            {"name": "Latvia", "code": "LV", "cities": [{"name": "Riga"}]},
        ]
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(json_manifest))

    context, manifest = load_manifest_and_context(rc, path)
    commands.get_dataset(context, manifest, "dataset").resources["resource"].external = "manifest.json"
    a, b = compare_manifest(
        manifest,
        """
id | d | r | b | m | property | type                   | ref     | source           | prepare | level | access | uri | title | description
   | dataset                  |                        |         |                  |         |       |        |     |       |
   |   | resource             | dask/json              |         | manifest.json    |         |       |        |     |       |
   |                          |                        |         |                  |         |       |        |     |       |
   |   |   |   | Country      |                        |         | country          |         |       |        |     |       |
   |   |   |   |   | name     | string required unique |         | name             |         |       |        |     |       |
   |   |   |   |   | code     | string required unique |         | code             |         |       |        |     |       |
   |                          |                        |         |                  |         |       |        |     |       |
   |   |   |   | Cities       |                        |         | country[].cities |         |       |        |     |       |
   |   |   |   |   | name     | string required unique |         | name             |         |       |        |     |       |
   |   |   |   |   | country  | ref                    | Country | ..               |         |       |        |     |       |
""",
        context,
    )
    assert a == b


def test_json_blank_node(rc: RawConfig, tmp_path: Path):
    json_manifest = [
        {"name": "Lithuania", "code": "LT", "cities": [{"name": "Vilnius"}, {"name": "Kaunas"}]},
        {"name": "Latvia", "code": "LV", "cities": [{"name": "Riga"}]},
    ]
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(json_manifest))

    context, manifest = load_manifest_and_context(rc, path)
    commands.get_dataset(context, manifest, "dataset").resources["resource"].external = "manifest.json"
    a, b = compare_manifest(
        manifest,
        """
id | d | r | b | m | property | type                   | ref     | source        | prepare | level | access | uri | title | description
   | dataset                  |                        |         |               |         |       |        |     |       |
   |   | resource             | dask/json              |         | manifest.json |         |       |        |     |       |
   |                          |                        |         |               |         |       |        |     |       |
   |   |   |   | Model1       |                        |         | .             |         |       |        |     |       |
   |   |   |   |   | name     | string required unique |         | name          |         |       |        |     |       |
   |   |   |   |   | code     | string required unique |         | code          |         |       |        |     |       |
   |                          |                        |         |               |         |       |        |     |       |
   |   |   |   | Cities       |                        |         | cities        |         |       |        |     |       |
   |   |   |   |   | name     | string required unique |         | name          |         |       |        |     |       |
   |   |   |   |   | parent   | ref                    | Model1  | ..            |         |       |        |     |       |
""",
        context,
    )
    assert a == b


def test_json_blank_node_inherit(rc: RawConfig, tmp_path: Path):
    json_manifest = [
        {
            "name": "Lithuania",
            "code": "LT",
            "location": {"latitude": 54.5, "longitude": 12.6},
            "cities": [
                {"name": "Vilnius", "weather": {"temperature": 24.7, "wind_speed": 12.4}},
                {"name": "Kaunas", "weather": {"temperature": 29.7, "wind_speed": 11.4}},
            ],
        },
        {"name": "Latvia", "code": "LV", "cities": [{"name": "Riga"}]},
    ]
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(json_manifest))

    context, manifest = load_manifest_and_context(rc, path)
    commands.get_dataset(context, manifest, "dataset").resources["resource"].external = "manifest.json"
    a, b = compare_manifest(
        manifest,
        """
id | d | r | b | m | property            | type                   | ref     | source              | prepare | level | access | uri | title | description
   | dataset                             |                        |         |                     |         |       |        |     |       |
   |   | resource                        | dask/json              |         | manifest.json       |         |       |        |     |       |
   |                                     |                        |         |                     |         |       |        |     |       |
   |   |   |   | Model1                  |                        |         | .                   |         |       |        |     |       |
   |   |   |   |   | name                | string required unique |         | name                |         |       |        |     |       |
   |   |   |   |   | code                | string required unique |         | code                |         |       |        |     |       |
   |   |   |   |   | location_latitude   | number unique          |         | location.latitude   |         |       |        |     |       |
   |   |   |   |   | location_longitude  | number unique          |         | location.longitude  |         |       |        |     |       |
   |                                     |                        |         |                     |         |       |        |     |       |
   |   |   |   | Cities                  |                        |         | cities              |         |       |        |     |       |
   |   |   |   |   | name                | string required unique |         | name                |         |       |        |     |       |
   |   |   |   |   | weather_temperature | number unique          |         | weather.temperature |         |       |        |     |       |
   |   |   |   |   | weather_wind_speed  | number unique          |         | weather.wind_speed  |         |       |        |     |       |
   |   |   |   |   | parent              | ref                    | Model1  | ..                  |         |       |        |     |       |
""",
        context,
    )
    assert a == b


def test_json_inherit_nested(rc: RawConfig, tmp_path: Path):
    json_manifest = {
        "country": [
            {
                "name": "Lithuania",
                "code": "LT",
                "location": {"coords": [54.5, 58.6], "test": "nope", "geo": [{"geo_test": "test"}]},
                "cities": [
                    {"name": "Vilnius", "location": {"coords": [54.5, 55.1], "geo": [{"geo_test": 5}]}},
                    {"name": "Kaunas"},
                ],
            },
            {"name": "Latvia", "code": "LV", "cities": [{"name": "Riga"}]},
        ]
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(json_manifest))

    context, manifest = load_manifest_and_context(rc, path)
    commands.get_dataset(context, manifest, "dataset").resources["resource"].external = "manifest.json"
    a, b = compare_manifest(
        manifest,
        """
id | d | r | b | m | property            | type                    | ref     | source                          | prepare | level | access | uri | title | description
   | dataset                             |                         |         |                                 |         |       |        |     |       |
   |   | resource                        | dask/json               |         | manifest.json                   |         |       |        |     |       |
   |                                     |                         |         |                                 |         |       |        |     |       |
   |   |   |   | Country                 |                         |         | country                         |         |       |        |     |       |
   |   |   |   |   | name                | string required unique  |         | name                            |         |       |        |     |       |
   |   |   |   |   | code                | string required unique  |         | code                            |         |       |        |     |       |
   |   |   |   |   | location_coords[]   | number                  |         | location.coords                 |         |       |        |     |       |
   |   |   |   |   | location_test       | string unique           |         | location.test                   |         |       |        |     |       |
   |                                     |                         |         |                                 |         |       |        |     |       |
   |   |   |   | Geo                     |                         |         | country[].location.geo          |         |       |        |     |       |
   |   |   |   |   | geo_test            | string required unique  |         | geo_test                        |         |       |        |     |       |
   |   |   |   |   | country             | ref                     | Country | ...                             |         |       |        |     |       |
   |                                     |                         |         |                                 |         |       |        |     |       |
   |   |   |   | Geo1                    |                         |         | country[].cities[].location.geo |         |       |        |     |       |
   |   |   |   |   | geo_test            | integer required unique |         | geo_test                        |         |       |        |     |       |
   |   |   |   |   | cities              | ref                     | Cities  | ...                             |         |       |        |     |       |
   |                                     |                         |         |                                 |         |       |        |     |       |
   |   |   |   | Cities                  |                         |         | country[].cities                |         |       |        |     |       |
   |   |   |   |   | name                | string required unique  |         | name                            |         |       |        |     |       |
   |   |   |   |   | location_coords[]   | number                  |         | location.coords                 |         |       |        |     |       |
   |   |   |   |   | country             | ref                     | Country | ..                              |         |       |        |     |       |
""",
        context,
    )
    assert a == b
