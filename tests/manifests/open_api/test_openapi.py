import json
from pathlib import Path

from spinta.core.config import RawConfig
from spinta.testing.manifest import load_manifest


def test_open_api_manifest(rc: RawConfig, tmp_path: Path):
    # TODO: Update everytime a new part is supported
    api_countries_get_id = '097b4270882b4facbd9d9ce453f8c196'
    data = json.dumps({
        'openapi': '3.0.0',
        'info': {
            'title': 'Example API',
            'version': '1.0.0',
            'summary': 'Example of an API',
            'description': 'Intricate description'
        },
        'tags': [
            {
                'name': 'List of Countries',
                'description': 'A list of world countries'
            }
        ],
        'paths': {
            '/api/countries/{countryId}': {
                'get': {
                    'tags': ['List of Countries'],
                    'summary': 'Countries',
                    'description': 'Lists known countries',
                    'operationId': api_countries_get_id,
                    'parameters': [
                        {
                            'name': 'countryId',
                            'in': 'path',
                        },
                        {
                            'name': 'Title',
                            'in': 'query',
                            'description': ''
                        },
                        {
                            'name': 'page',
                            'in': 'query',
                            'description': 'Page number for paginated results'
                        }
                    ]
                }
            },
        }
    })

    table = '''
    id | d | r | b | m | property               | type      | ref        | source                        | prepare                           | level | access | uri | title             | description
       | services/example_api                   | ns        |            |                               |                                   |       |        |     | Example of an API | Intricate description
       |                                        |           |            |                               |                                   |       |        |     |                   |
       | services/example_api/list_of_countries |           |            |                               |                                   |       |        |     | List of Countries | A list of world countries
    09 |   | api_countries_country_id_get       | dask/json |            | /api/countries/{country_id}   | http(method: 'GET', body: 'form') |       |        |     | Countries         | Lists known countries
       |                                        | param     | country_id | countryId                     | path()                            |       |        |     |                   |
       |                                        | param     | title      | Title                         | query()                           |       |        |     |                   |
       |                                        | param     | page       | page                          | query()                           |       |        |     |                   | Page number for paginated results
    '''

    path = tmp_path / 'manifest.json'
    path_openapi = f'openapi+file://{path}'
    with open(path, 'w') as json_file:
        json_file.write(data)

    manifest = load_manifest(rc, path_openapi)

    assert manifest == table


def test_open_api_manifest_no_paths(rc: RawConfig, tmp_path: Path):
    data = json.dumps({
        'openapi': '3.0.0',
        'info': {
            'title': 'Example API',
            'version': '1.0.0',
            'summary': 'Example of an API',
            'description': 'Intricate description'
        }
    })

    table = '''
    id | d | r | b | m | property               | type | ref | source | prepare | level | access | uri | title             | description
       | services/example_api                   | ns   |     |        |         |       |        |     | Example of an API | Intricate description
       |                                        |      |     |        |         |       |        |     |                   |
       | services/example_api/default           |      |     |        |         |       |        |     |                   |
    '''

    path = tmp_path / 'manifest.json'
    path_openapi = f'openapi+file://{path}'
    with open(path, 'w') as json_file:
        json_file.write(data)

    manifest = load_manifest(rc, path_openapi)

    assert manifest == table


def test_open_api_manifest_title_with_slashes(rc: RawConfig, tmp_path: Path):
    data = json.dumps({
        'openapi': '3.0.0',
        'info': {
            'title': 'Example API/Example for example',
            'version': '1.0.0',
            'summary': 'Example of an API',
            'description': 'Intricate description'
        }
    })

    table = '''
        id | d | r | b | m | property             | type | ref | source | prepare | level | access | uri | title             | description
           | services/example_for_example         | ns   |     |        |         |       |        |     | Example of an API | Intricate description
           |                                      |      |     |        |         |       |        |     |                   |
           | services/example_for_example/default |      |     |        |         |       |        |     |                   |
        '''

    path = tmp_path / 'manifest.json'
    path_openapi = f'openapi+file://{path}'
    with open(path, 'w') as json_file:
        json_file.write(data)

    manifest = load_manifest(rc, path_openapi)

    assert manifest == table

def test_open_api_manifest_root_model(rc: RawConfig, tmp_path: Path):
    data = json.dumps({
        "openapi": "3.0.0",
        "info": {"title": "Vilnius API", "version": "1.0.0"},
        "paths": {
            "/api/spis/relations/child": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "Relationship",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "properties": {
                                            "id": {
                                                "type": "integer",
                                            },
                                            "created_at": {
                                                "type": "string",
                                                "format": "date-time",
                                                "example": "2025-02-19T07:00:11.000000Z",
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
    })

    table = """
    id | d | r | b | m | property                | type              | ref | source                    | source.type | prepare                           | origin | count | level | status  | visibility | access | uri | eli | title       | description
       | services/vilnius_api                    | ns                |     |                           |             |                                   |        |       |       |         |            |        |     |     | Vilnius API |
       |                                         |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       | services/vilnius_api/default            |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       |   | api_spis_relations_child_get        | dask/json         |     | /api/spis/relations/child |             | http(method: 'GET', body: 'form') |        |       |       |         |            |        |     |     |             |
       |                                         |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       |   |   |   | Relationship                |                   |     | .                         |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | id                      | integer required  |     | id                        |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | created_at              | datetime required |     | created_at                |             |                                   |        |       |       | develop | private    |        |     |     |             |
       """


    path = tmp_path / 'manifest.json'
    path_openapi = f'openapi+file://{path}'
    with open(path, 'w') as json_file:
        json_file.write(data)

    manifest = load_manifest(rc, path_openapi)

    assert manifest == table

def test_open_api_manifest_nested_models(rc: RawConfig, tmp_path: Path):
    data = json.dumps({
        "openapi": "3.0.0",
        "info": {"title": "Vilnius API", "version": "1.0.0"},
        "paths": {
            "/api/spis/relations/child": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "Relation with child and parents",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": {
                                            "properties": {
                                                "data": {
                                                    "properties": {
                                                        "Child_Person": {
                                                            "properties": {
                                                                "siblings": {
                                                                    "type": "array",
                                                                    "items": {
                                                                        "type": "object",
                                                                        "properties": {},
                                                                    },
                                                                }
                                                            },
                                                            "type": "object",
                                                        },
                                                        "parents_legal": {
                                                            "type": "array",
                                                            "items": {"type": "object"},
                                                        },
                                                    },
                                                    "type": "object",
                                                }
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
    })

    table = """
    id | d | r | b | m | property                | type              | ref | source                    | source.type | prepare                           | origin | count | level | status  | visibility | access | uri | eli | title       | description
       | services/vilnius_api                    | ns                |     |                           |             |                                   |        |       |       |         |            |        |     |     | Vilnius API |
       |                                         |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       | services/vilnius_api/default            |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       |   | api_spis_relations_child_get        | dask/json         |     | /api/spis/relations/child |             | http(method: 'GET', body: 'form') |        |       |       |         |            |        |     |     |             |
       |                                         |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       |   |   |   | RelationWithChildAndParents |                   |     | .[]                       |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | data                    | ref required      | Data | data                      |             | expand()                          |        |       |       | develop | private    |        |     |     |             |
       |                                         |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       |   |   |   | Data                        |                   |     |                           |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | child_person            | ref required      | ChildPerson | Child_Person              |             | expand()                          |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | parents_legal           | array required    |     | parents_legal             |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | parents_legal[]         | backref required  | ParentsLegal |                           |             | expand()                          |        |       |       | develop | private    |        |     |     |             |
       |                                         |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       |   |   |   | ChildPerson                 |                   |     |                           |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | siblings                | array required    |     | siblings                  |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | siblings[]              | backref required  | Siblings |                           |             | expand()                          |        |       |       | develop | private    |        |     |     |             |
       |                                         |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       |   |   |   | Siblings                    |                   |     |                           |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | child_person            | ref required      | ChildPerson |                           |             | expand()                          |        |       |       | develop | private    |        |     |     |             |
       |                                         |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       |   |   |   | ParentsLegal                |                   |     |                           |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | data                    | ref required      | Data |                           |             | expand()                          |        |       |       | develop | private    |        |     |     |             |
   """

 
    path = tmp_path / 'manifest.json'
    path_openapi = f'openapi+file://{path}'
    with open(path, 'w') as json_file:
        json_file.write(data)

    manifest = load_manifest(rc, path_openapi)

    assert manifest == table

def test_open_api_manifest_enum(rc: RawConfig, tmp_path: Path):
    data = json.dumps({
        "openapi": "3.0.0",
        "info": {"title": "Example", "version": "1.0.0"},
        "paths": {
            "/api/spis/relations/child": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "Relation",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "properties": {
                                            "relation_type": {
                                                "type": "string",
                                                "enum": [
                                                    "biological",
                                                    "adoptive",
                                                    "foster",
                                                ]
                                            }
                                        },
                                        "type": "object",
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    )

    table = """
    id | d | r | b | m | property                | type              | ref | source                    | source.type | prepare                           | origin | count | level | status  | visibility | access | uri | eli | title       | description
       | services/example                        | ns                |     |                           |             |                                   |        |       |       |         |            |        |     |     | Example     |
       |                                         |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       | services/example/default                |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       |   | api_spis_relations_child_get        | dask/json         |     | /api/spis/relations/child |             | http(method: 'GET', body: 'form') |        |       |       |         |            |        |     |     |             |
       |                                         |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       |   |   |   | Relation                    |                   |     | .                         |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | relation_type           | string required   |     | relation_type             |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |                                         | enum              |     | biological                |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |                                         |                   |     | adoptive                  |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |                                         |                   |     | foster                    |             |                                   |        |       |       | develop | private    |        |     |     |             |
       """
 
    path = tmp_path / 'manifest.json'
    path_openapi = f'openapi+file://{path}'
    with open(path, 'w') as json_file:
        json_file.write(data)

    manifest = load_manifest(rc, path_openapi)

    assert manifest == table

def test_open_api_manifest_datatypes(rc: RawConfig, tmp_path: Path):
    data = json.dumps({
        "openapi": "3.0.0",
        "info": {"title": "Example", "version": "1.0.0"},
        "paths": {
            "/api/datatypes": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "Example of datatypes",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "id": {"type": "integer", "example": 1},
                                            "active_from": {
                                                "type": "string",
                                                "format": "date",
                                                "example": "2019-12-31",
                                            },
                                            "active_to": {
                                                "nullable": True,
                                                "type": "string",
                                            },
                                            "created_at": {
                                                "type": "string",
                                                "format": "date-time",
                                                "example": "2025-02-19T07:00:11.000000Z",
                                            },
                                            "Subscribed": {"type": "boolean"},
                                            "weight": {"type": "number"},
                                            "attachment": {
                                                "type": "string",
                                                "contentEncoding": "base64",
                                            },
                                            "meeting_time": {
                                                "type": "string",
                                                "format": "time",
                                                "example": "14:30:00",
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    })

    table = """
    id | d | r | b | m | property                | type              | ref | source                    | source.type | prepare                           | origin | count | level | status  | visibility | access | uri | eli | title       | description
       | services/example                        | ns                |     |                           |             |                                   |        |       |       |         |            |        |     |     | Example     |
       |                                         |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       | services/example/default                |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       |   | api_datatypes_get                   | dask/json         |     | /api/datatypes            |             | http(method: 'GET', body: 'form') |        |       |       |         |            |        |     |     |             |
       |                                         |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       |   |   |   | ExampleOfDatatypes          |                   |     | .                         |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | id                      | integer required  |     | id                        |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | active_from             | date required     |     | active_from               |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | active_to               | string            |     | active_to                 |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | created_at              | datetime required |     | created_at                |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | subscribed              | boolean required  |     | Subscribed                |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | weight                  | number required   |     | weight                    |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | attachment              | binary required   |     | attachment                |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | meeting_time            | time required     |     | meeting_time              |             |                                   |        |       |       | develop | private    |        |     |     |             |
       """
 
    path = tmp_path / 'manifest.json'
    path_openapi = f'openapi+file://{path}'
    with open(path, 'w') as json_file:
        json_file.write(data)

    manifest = load_manifest(rc, path_openapi)

    assert manifest == table

def test_open_api_manifest_array_of_primitives(rc: RawConfig, tmp_path: Path):
    data = json.dumps({
        "openapi": "3.0.0",
        "info": {"title": "Test API", "version": "1.0.0"},
        "paths": {
            "/api/array": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "Masyvas",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "notes": {
                                                "type": "array",
                                                "items": {"type": "string"}
                                            },
                                            "numbers": {
                                                "type": "array",
                                                "items": {"type": "integer"}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    })

    table = """
    id | d | r | b | m | property                | type              | ref | source                    | source.type | prepare                           | origin | count | level | status  | visibility | access | uri | eli | title       | description
       | services/test_api                       | ns                |     |                           |             |                                   |        |       |       |         |            |        |     |     | Test API    |
       |                                         |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       | services/test_api/default               |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       |   | api_array_get                       | dask/json         |     | /api/array                |             | http(method: 'GET', body: 'form') |        |       |       |         |            |        |     |     |             |
       |                                         |                   |     |                           |             |                                   |        |       |       |         |            |        |     |     |             |
       |   |   |   | Masyvas                     |                   |     | .                         |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | notes                   | array required    |     | notes                     |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | notes[]                 | string required   |     |                           |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | numbers                 | array required    |     | numbers                   |             |                                   |        |       |       | develop | private    |        |     |     |             |
       |   |   |   |   | numbers[]               | integer required  |     |                           |             |                                   |        |       |       | develop | private    |        |     |     |             |
    """
 
    path = tmp_path / 'manifest.json'
    path_openapi = f'openapi+file://{path}'
    with open(path, 'w') as json_file:
        json_file.write(data)

    manifest = load_manifest(rc, path_openapi)

    assert manifest == table
