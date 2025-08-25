import json
from pathlib import Path

from spinta.core.config import RawConfig
from spinta.testing.manifest import load_manifest
from spinta.manifests.open_api.helpers import model_deduplicator


def test_open_api_manifest(rc: RawConfig, tmp_path: Path):
    # TODO: Update everytime a new part is supported
    api_countries_get_id = "097b4270882b4facbd9d9ce453f8c196"
    data = json.dumps(
        {
            "openapi": "3.0.0",
            "info": {
                "title": "Example API",
                "version": "1.0.0",
                "summary": "Example of an API",
                "description": "Intricate description",
            },
            "tags": [{"name": "List of Countries", "description": "A list of world countries"}],
            "paths": {
                "/api/countries/{countryId}": {
                    "get": {
                        "tags": ["List of Countries"],
                        "summary": "Countries",
                        "description": "Lists known countries",
                        "operationId": api_countries_get_id,
                        "parameters": [
                            {
                                "name": "countryId",
                                "in": "path",
                            },
                            {"name": "Title", "in": "query", "description": ""},
                            {"name": "page", "in": "query", "description": "Page number for paginated results"},
                        ],
                    }
                },
            },
        }
    )

    table = """
    id | d | r | b | m | property               | type      | ref        | source                        | prepare                           | level | access | uri | title             | description
       | services/example_api                   | ns        |            |                               |                                   |       |        |     | Example of an API | Intricate description
       |                                        |           |            |                               |                                   |       |        |     |                   |
       | services/example_api/list_of_countries |           |            |                               |                                   |       |        |     | List of Countries | A list of world countries
    09 |   | api_countries_country_id_get       | dask/json |            | /api/countries/{country_id}   | http(method: 'GET', body: 'form') |       |        |     | Countries         | Lists known countries
       |                                        | param     | country_id | countryId                     | path()                            |       |        |     |                   |
       |                                        | param     | title      | Title                         | query()                           |       |        |     |                   |
       |                                        | param     | page       | page                          | query()                           |       |        |     |                   | Page number for paginated results
    """

    path = tmp_path / "manifest.json"
    path_openapi = f"openapi+file://{path}"
    with open(path, "w") as json_file:
        json_file.write(data)

    manifest = load_manifest(rc, path_openapi)

    assert manifest == table


def test_open_api_manifest_no_paths(rc: RawConfig, tmp_path: Path):
    data = json.dumps(
        {
            "openapi": "3.0.0",
            "info": {
                "title": "Example API",
                "version": "1.0.0",
                "summary": "Example of an API",
                "description": "Intricate description",
            },
        }
    )

    table = """
    id | d | r | b | m | property               | type | ref | source | prepare | level | access | uri | title             | description
       | services/example_api                   | ns   |     |        |         |       |        |     | Example of an API | Intricate description
       |                                        |      |     |        |         |       |        |     |                   |
       | services/example_api/default           |      |     |        |         |       |        |     |                   |
    """

    path = tmp_path / "manifest.json"
    path_openapi = f"openapi+file://{path}"
    with open(path, "w") as json_file:
        json_file.write(data)

    manifest = load_manifest(rc, path_openapi)

    assert manifest == table


def test_open_api_manifest_title_with_slashes(rc: RawConfig, tmp_path: Path):
    data = json.dumps(
        {
            "openapi": "3.0.0",
            "info": {
                "title": "Example API/Example for example",
                "version": "1.0.0",
                "summary": "Example of an API",
                "description": "Intricate description",
            },
        }
    )

    table = """
        id | d | r | b | m | property             | type | ref | source | prepare | level | access | uri | title             | description
           | services/example_for_example         | ns   |     |        |         |       |        |     | Example of an API | Intricate description
           |                                      |      |     |        |         |       |        |     |                   |
           | services/example_for_example/default |      |     |        |         |       |        |     |                   |
        """

    path = tmp_path / "manifest.json"
    path_openapi = f"openapi+file://{path}"
    with open(path, "w") as json_file:
        json_file.write(data)

    manifest = load_manifest(rc, path_openapi)

    assert manifest == table


def test_open_api_manifest_root_model(rc: RawConfig, tmp_path: Path):
    data = json.dumps(
        {
            "openapi": "3.0.0",
            "info": {
                "title": "Example API",
                "version": "1.0.0",
                "summary": "Example of an API",
                "description": "Intricate description",
            },
            "paths": {
                "/api/countries": {
                    "get": {
                        "responses": {
                            "200": {
                                "description": "Country",
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "properties": {
                                                "id": {
                                                    "type": "integer",
                                                },
                                                "name": {"type": "string"},
                                            },
                                            "type": "object",
                                        }
                                    }
                                },
                            }
                        }
                    }
                }
            },
        }
    )

    table = """
    id | d | r | b | m | property                | type              | ref | source                    | source.type | prepare                           | origin | count | level | status  | visibility | access | uri | eli | title       | description
       | services/example_api                    | ns                |     |                           |             |                                   |        |       |       |         |            |        |     |     | Example of an API | Intricate description
       |                                         |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       | services/example_api/default            |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       |   | api_countries_get                   | dask/json         |     | /api/countries            |             | http(method: 'GET', body: 'form') |        |       |       |         |            |        |     |     |             |
       |                                         |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       |   |   |   | Country                     |                   |     | .                         |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | id                      | integer required  |     | id                        |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | name                    | string required   |     | name                      |             |                                   |        |       |       | develop | private    |        |     |     |             |
   """

    path = tmp_path / "manifest.json"
    path_openapi = f"openapi+file://{path}"
    with open(path, "w") as json_file:
        json_file.write(data)

    manifest = load_manifest(rc, path_openapi)

    assert manifest == table


def test_open_api_manifest_nested_models(rc: RawConfig, tmp_path: Path):
    data = json.dumps(
        {
            "openapi": "3.0.0",
            "info": {
                "title": "Example API",
                "version": "1.0.0",
                "summary": "Example of an API",
                "description": "Intricate description",
            },
            "paths": {
                "/api/countries": {
                    "get": {
                        "responses": {
                            "200": {
                                "description": "List of Countries",
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "type": "array",
                                            "items": {
                                                "properties": {
                                                    "name": {"type": "string"},
                                                    "cities": {
                                                        "type": "array",
                                                        "items": {
                                                            "properties": {
                                                                "name": {"type": "string"},
                                                                "counties": {
                                                                    "type": "array",
                                                                    "items": {
                                                                        "properties": {"name": {"type": "string"}},
                                                                        "type": "object",
                                                                    },
                                                                },
                                                            },
                                                            "type": "object",
                                                        },
                                                    },
                                                },
                                                "type": "object",
                                            },
                                        }
                                    }
                                },
                            }
                        }
                    }
                }
            },
        }
    )

    table = """
    id | d | r | b | m | property                | type              | ref | source                    | source.type | prepare                           | origin | count | level | status  | visibility | access | uri | eli | title       | description
       | services/example_api                    | ns                |     |                           |             |                                   |        |       |       |         |            |        |     |     | Example of an API | Intricate description
       |                                         |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       | services/example_api/default            |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       |   | api_countries_get                   | dask/json         |     | /api/countries            |             | http(method: 'GET', body: 'form') |        |       |       |         |            |        |     |     |             |
       |                                         |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       |   |   |   | ListOfCountries             |                   |     | .[]                       |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | name                    | string required   |     | name                      |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | cities                  | array required    |     | cities                    |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | cities[]                | backref required  | Cities |                           |             | expand()                          |        |       |       | develop | private    |        |     |     |             |
       |                                         |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       |   |   |   | Cities                      |                   |     |                           |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | name                    | string required   |     | name                      |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | counties                | array required    |     | counties                  |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | counties[]              | backref required  | Counties |                           |             | expand()                          |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | list_of_countries       | ref required      | ListOfCountries |                           |             | expand()                          |        |       |       | develop | private    |        |     |     |             |
       |                                         |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       |   |   |   | Counties                    |                   |     |                           |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | name                    | string required   |     | name                      |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | cities                  | ref required      | Cities |                           |             | expand()                          |        |       |       | develop | private    |        |     |     |             |
       """

    path = tmp_path / "manifest.json"
    path_openapi = f"openapi+file://{path}"
    with open(path, "w") as json_file:
        json_file.write(data)

    manifest = load_manifest(rc, path_openapi)

    assert manifest == table


def test_open_api_manifest_enum(rc: RawConfig, tmp_path: Path):
    model_deduplicator._names.clear()
    data = json.dumps(
        {
            "openapi": "3.0.0",
            "info": {
                "title": "Example API",
                "version": "1.0.0",
                "summary": "Example of an API",
                "description": "Intricate description",
            },
            "paths": {
                "/api/countries": {
                    "get": {
                        "responses": {
                            "200": {
                                "description": "Country",
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "properties": {
                                                "name": {"type": "string"},
                                                "region": {
                                                    "type": "string",
                                                    "enum": ["Europe", "Asia", "Africa", "Americas", "Oceania"],
                                                },
                                            },
                                            "type": "object",
                                        }
                                    }
                                },
                            }
                        }
                    }
                }
            },
        }
    )

    table = """
    id | d | r | b | m | property                | type              | ref | source                    | source.type | prepare                           | origin | count | level | status  | visibility | access | uri | eli | title       | description
       | services/example_api                    | ns                |     |                           |             |                                   |        |       |       |         |            |        |     |     | Example of an API | Intricate description
       |                                         |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       | services/example_api/default            |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       |   | api_countries_get                   | dask/json         |     | /api/countries            |             | http(method: 'GET', body: 'form') |        |       |       |         |            |        |     |     |             |
       |                                         |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       |   |   |   | Country                     |                   |     | .                         |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | name                    | string required   |     | name                      |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | region                  | string required   |     | region                    |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |                                         | enum              |     | Europe                    |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |                                         |                   |     | Asia                      |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |                                         |                   |     | Africa                    |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |                                         |                   |     | Americas                  |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |                                         |                   |     | Oceania                   |             |                                   |        |       |       | develop | private    |        |     |     |             |
       """

    path = tmp_path / "manifest.json"
    path_openapi = f"openapi+file://{path}"
    with open(path, "w") as json_file:
        json_file.write(data)

    manifest = load_manifest(rc, path_openapi)

    assert manifest == table


def test_open_api_manifest_datatypes(rc: RawConfig, tmp_path: Path):
    model_deduplicator._names.clear()
    data = json.dumps(
        {
            "openapi": "3.0.0",
            "info": {
                "title": "Example API",
                "version": "1.0.0",
                "summary": "Example of an API",
                "description": "Intricate description",
            },
            "paths": {
                "/api/countries": {
                    "get": {
                        "responses": {
                            "200": {
                                "description": "Country",
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "type": "object",
                                            "properties": {
                                                "name": {"type": "string"},
                                                "independence_date": {"type": "string", "format": "date"},
                                                "timezone_offset": {"type": "string", "format": "time"},
                                                "last_updated": {"type": "string", "format": "date-time"},
                                                "is_member_of_un": {"type": "boolean"},
                                                "population": {"type": "integer"},
                                                "gdp": {"type": "number"},
                                                "flag_base64": {
                                                    "type": "string",
                                                    "contentEncoding": "base64",
                                                },
                                            },
                                        }
                                    }
                                },
                            }
                        }
                    }
                }
            },
        }
    )

    table = """
    id | d | r | b | m | property                | type              | ref | source                    | source.type | prepare                           | origin | count | level | status  | visibility | access | uri | eli | title       | description
       | services/example_api                    | ns                |     |                           |             |                                   |        |       |       |         |            |        |     |     | Example of an API | Intricate description
       |                                         |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       | services/example_api/default            |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       |   | api_countries_get                   | dask/json         |     | /api/countries            |             | http(method: 'GET', body: 'form') |        |       |       |         |            |        |     |     |             |
       |                                         |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       |   |   |   | Country                     |                   |     | .                         |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | name                    | string required   |     | name                      |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | independence_date       | date required     |     | independence_date         |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | timezone_offset         | time required     |     | timezone_offset           |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | last_updated            | datetime required |     | last_updated              |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | is_member_of_un         | boolean required  |     | is_member_of_un           |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | population              | integer required  |     | population                |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | gdp                     | number required   |     | gdp                       |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | flag_base64             | binary required   |     | flag_base64               |             |                                   |        |       |       | develop | private    |        |     |     |             |
       """

    path = tmp_path / "manifest.json"
    path_openapi = f"openapi+file://{path}"
    with open(path, "w") as json_file:
        json_file.write(data)

    manifest = load_manifest(rc, path_openapi)

    assert manifest == table


def test_open_api_manifest_array_of_primitives(rc: RawConfig, tmp_path: Path):
    model_deduplicator._names.clear()
    data = json.dumps(
        {
            "openapi": "3.0.0",
            "info": {
                "title": "Example API",
                "version": "1.0.0",
                "summary": "Example of an API",
                "description": "Intricate description",
            },
            "paths": {
                "/api/countries": {
                    "get": {
                        "responses": {
                            "200": {
                                "description": "Country",
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "type": "object",
                                            "properties": {
                                                "name": {"type": "string"},
                                                "city_names": {"type": "array", "items": {"type": "string"}},
                                            },
                                        }
                                    }
                                },
                            }
                        }
                    }
                }
            },
        }
    )

    table = """
    id | d | r | b | m | property                | type              | ref | source                    | source.type | prepare                           | origin | count | level | status  | visibility | access | uri | eli | title       | description
       | services/example_api                    | ns                |     |                           |             |                                   |        |       |       |         |            |        |     |     | Example of an API | Intricate description
       |                                         |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       | services/example_api/default            |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       |   | api_countries_get                   | dask/json         |     | /api/countries            |             | http(method: 'GET', body: 'form') |        |       |       |         |            |        |     |     |             |
       |                                         |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       |   |   |   | Country                     |                   |     | .                         |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | name                    | string required   |     | name                      |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | city_names              | array required    |     | city_names                |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | city_names[]            | string required   |     |                           |             |                                   |        |       |       | develop | private    |        |     |     |             |
       """

    path = tmp_path / "manifest.json"
    path_openapi = f"openapi+file://{path}"
    with open(path, "w") as json_file:
        json_file.write(data)

    manifest = load_manifest(rc, path_openapi)

    assert manifest == table
