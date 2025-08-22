import json
import uuid
from pathlib import Path

import pytest

from spinta.core.ufuncs import Expr
from spinta.manifests.open_api.helpers import (
    read_file_data_and_transform_to_json,
    get_dataset_schemas,
    get_namespace_schema,
    get_resource_parameters,
    replace_url_parameters,
    get_schema_from_response,
    get_model_schemas,
    Model,
    Property,
)
from spinta.utils.naming import to_model_name, to_property_name


@pytest.mark.parametrize(
    argnames=["url", "expected_result_url"],
    argvalues=[
        ["/api/countries/{countryId}", "/api/countries/{country_id}"],
        ["/api/countries/{countryId}/cities/{cityId}", "/api/countries/{country_id}/cities/{city_id}"],
    ],
)
def test_replace_url_parameters(url: str, expected_result_url: str):
    adjusted_url = replace_url_parameters(url)

    assert adjusted_url == expected_result_url


def test_read_file_data_and_transform_to_json(tmp_path: Path):
    file_data = {"openapi": "3.0.0", "info": {"title": "OpenAPI"}}
    path = tmp_path / "manifest.json"
    with open(path, "w") as json_file:
        json_file.write(json.dumps(file_data))

    result = read_file_data_and_transform_to_json(path)

    assert result == file_data


def test_get_namespace_schemas():
    title = "Geography API"
    dataset_prefix = "services/geography_api"
    data = {
        "info": {
            "title": title,
            "version": "1.0.0",
            "summary": "API for geographic objects",
            "description": "Intricate description",
        }
    }

    namespace_schemas = [schema for _, schema in get_namespace_schema(data["info"], title, dataset_prefix)]

    assert namespace_schemas == [
        {
            "type": "ns",
            "name": dataset_prefix,
            "title": "API for geographic objects",
            "description": "Intricate description",
        }
    ]


def test_get_resource_parameters():
    data = [
        {
            "name": "title",
            "in": "query",
            "schema": {"example": "EXAMPLE", "type": "string"},
            "description": "Item search by name",
        },
        {
            "name": "page",
            "in": "path",
            "schema": {"example": "EXAMPLE_2", "type": "string"},
            "description": "Page number for paginated results",
        },
    ]

    resource_parameters = get_resource_parameters(data)

    assert resource_parameters == {
        "parameter_0": {
            "description": "Item search by name",
            "name": "title",
            "prepare": [Expr("query")],
            "source": ["title"],
            "type": "param",
        },
        "parameter_1": {
            "description": "Page number for paginated results",
            "name": "page",
            "prepare": [Expr("path")],
            "source": ["page"],
            "type": "param",
        },
    }


def test_get_dataset_schemas():
    resource_countries_id = str(uuid.uuid4().hex)
    resource_cities_id = str(uuid.uuid4().hex)
    data = {
        "info": {
            "title": "Geography API",
            "version": "1.0.0",
            "summary": "API for geographic objects",
            "description": "Intricate description",
        },
        "tags": [
            {"name": "List of Countries", "description": "List known countries"},
            {"name": "Cities", "description": "Known cities"},
        ],
        "paths": {
            "/api/countries/{countryId}": {
                "get": {
                    "tags": ["List of Countries"],
                    "summary": "List of countries API",
                    "description": "List of known countries in the world",
                    "operationId": resource_countries_id,
                    "parameters": [
                        {"name": "countryId", "in": "path"},
                        {"name": "title", "in": "query", "description": ""},
                        {"name": "page", "in": "query", "description": "Page number for paginated results"},
                    ],
                }
            },
            "/api/cities": {
                "get": {
                    "tags": ["List", "Cities"],
                    "summary": "List of cities API",
                    "description": "List of known cities in the world",
                    "operationId": resource_cities_id,
                    "parameters": [{"name": "limit", "in": "query", "description": "Limit to X results"}],
                }
            },
        },
    }

    dataset_schemas = [schema for _, schema in get_dataset_schemas(data, "services/geography_api")]

    assert dataset_schemas == [
        {
            "type": "dataset",
            "name": "services/geography_api/list_of_countries",
            "title": "List of Countries",
            "description": "List known countries",
            "resources": {
                "api_countries_country_id_get": {
                    "id": resource_countries_id,
                    "params": {
                        "parameter_0": {
                            "description": "",
                            "name": "country_id",
                            "prepare": [Expr("path")],
                            "source": ["countryId"],
                            "type": "param",
                        },
                        "parameter_1": {
                            "description": "",
                            "name": "title",
                            "prepare": [Expr("query")],
                            "source": ["title"],
                            "type": "param",
                        },
                        "parameter_2": {
                            "description": "Page number for paginated results",
                            "name": "page",
                            "prepare": [Expr("query")],
                            "source": ["page"],
                            "type": "param",
                        },
                    },
                    "external": "/api/countries/{country_id}",
                    "type": "dask/json",
                    "prepare": Expr("http", method="GET", body="form"),
                    "title": "List of countries API",
                    "description": "List of known countries in the world",
                }
            },
        },
        {
            "type": "dataset",
            "name": "services/geography_api/list_cities",
            "title": "List, Cities",
            "description": "Known cities",
            "resources": {
                "api_cities_get": {
                    "id": resource_cities_id,
                    "params": {
                        "parameter_0": {
                            "description": "Limit to X results",
                            "name": "limit",
                            "prepare": [Expr("query")],
                            "source": ["limit"],
                            "type": "param",
                        }
                    },
                    "external": "/api/cities",
                    "type": "dask/json",
                    "prepare": Expr("http", method="GET", body="form"),
                    "title": "List of cities API",
                    "description": "List of known cities in the world",
                }
            },
        },
    ]


def test_get_dataset_schemas_no_parameters():
    resource_countries_id = str(uuid.uuid4().hex)
    resource_cities_id = str(uuid.uuid4().hex)
    data = {
        "info": {
            "title": "Geography API",
            "version": "1.0.0",
            "summary": "API for geographic objects",
            "description": "Intricate description",
        },
        "tags": [
            {"name": "List of Countries", "description": "List known countries"},
            {"name": "Cities", "description": "Known cities"},
        ],
        "paths": {
            "/api/countries": {
                "get": {
                    "tags": ["List of Countries"],
                    "summary": "List of countries API",
                    "description": "List of known countries in the world",
                    "operationId": resource_countries_id,
                }
            },
            "/api/cities": {
                "get": {
                    "tags": ["List", "Cities"],
                    "summary": "List of cities API",
                    "description": "List of known cities in the world",
                    "operationId": resource_cities_id,
                }
            },
        },
    }

    dataset_schemas = [schema for _, schema in get_dataset_schemas(data, "services/geography_api")]

    assert dataset_schemas == [
        {
            "type": "dataset",
            "name": "services/geography_api/list_of_countries",
            "title": "List of Countries",
            "description": "List known countries",
            "resources": {
                "api_countries_get": {
                    "id": resource_countries_id,
                    "params": {},
                    "external": "/api/countries",
                    "type": "dask/json",
                    "prepare": Expr("http", method="GET", body="form"),
                    "title": "List of countries API",
                    "description": "List of known countries in the world",
                }
            },
        },
        {
            "type": "dataset",
            "name": "services/geography_api/list_cities",
            "title": "List, Cities",
            "description": "Known cities",
            "resources": {
                "api_cities_get": {
                    "id": resource_cities_id,
                    "params": {},
                    "external": "/api/cities",
                    "type": "dask/json",
                    "prepare": Expr("http", method="GET", body="form"),
                    "title": "List of cities API",
                    "description": "List of known cities in the world",
                }
            },
        },
    ]


def test_get_dataset_schemas_no_tags_default_dataset():
    resource_countries_id = str(uuid.uuid4().hex)
    resource_cities_id = str(uuid.uuid4().hex)
    data = {
        "info": {
            "title": "Geography API",
            "version": "1.0.0",
            "summary": "API for geographic objects",
            "description": "Intricate description",
        },
        "paths": {
            "/api/countries": {
                "get": {
                    "summary": "List of countries API",
                    "description": "List of known countries in the world",
                    "operationId": resource_countries_id,
                }
            },
            "/api/cities": {
                "get": {
                    "summary": "List of cities API",
                    "description": "List of known cities in the world",
                    "operationId": resource_cities_id,
                }
            },
        },
    }

    dataset_schemas = [schema for _, schema in get_dataset_schemas(data, "services/geography_api")]

    assert dataset_schemas == [
        {
            "type": "dataset",
            "name": "services/geography_api/default",
            "title": "",
            "description": "",
            "resources": {
                "api_countries_get": {
                    "id": resource_countries_id,
                    "params": {},
                    "external": "/api/countries",
                    "type": "dask/json",
                    "prepare": Expr("http", method="GET", body="form"),
                    "title": "List of countries API",
                    "description": "List of known countries in the world",
                },
                "api_cities_get": {
                    "id": resource_cities_id,
                    "params": {},
                    "external": "/api/cities",
                    "type": "dask/json",
                    "prepare": Expr("http", method="GET", body="form"),
                    "title": "List of cities API",
                    "description": "List of known cities in the world",
                },
            },
        }
    ]


JSON_SCHEMA = {
    "properties": {
        "data": {
            "properties": {
                "id": {
                    "type": "integer",
                    "example": 1,
                },
                "active_from": {"type": "string", "format": "date", "example": "2019-12-31"},
            },
            "type": "object",
        },
        "created_at": {
            "type": "string",
            "format": "date-time",
            "example": "2025-02-19T07:00:11.000000Z",
        },
    },
    "type": "object",
}

OPENAPI_RESPONSE = {
    "description": "Relation with child and parents",
    "content": {"application/json": {"schema": JSON_SCHEMA}},
}
OPENAPI_SCHEMA = {
    "openapi": "3.0.0",
    "info": {"title": "Vilnius API", "version": "1.0.0"},
    "paths": {
        "/api/spis/relations/child/{personal_code}": {
            "get": {
                "tags": ["SPIS"],
                "summary": "Check all SPIS relations for child personal code",
                "operationId": "e0e4ad148932d12d438e6a52ebbba641",
                "parameters": [
                    {
                        "name": "personal_code",
                        "in": "path",
                        "description": "Personal code to check",
                        "required": True,
                        "schema": {"type": "integer", "example": "30101010018"},
                    }
                ],
                "responses": {"200": OPENAPI_RESPONSE},
            }
        }
    },
}


@pytest.mark.parametrize(
    "response,schema",
    [
        # Swagger 2.0 specification
        (
            {"description": "User found", "schema": JSON_SCHEMA},
            {
                "swagger": "2.0",
                "info": {"title": "User API", "version": "1.0.0"},
                "paths": {
                    "/users/{id}": {
                        "get": {
                            "summary": "Get user by ID",
                            "responses": {"200": {"description": "User found", "schema": JSON_SCHEMA}},
                        }
                    }
                },
            },
        ),
        # OpenAPI 3.0 specification
        (OPENAPI_RESPONSE, OPENAPI_SCHEMA),
        # Unknown specification
        (
            OPENAPI_RESPONSE,
            {
                "info": {"title": "User API", "version": "1.0.0"},
                "paths": {
                    "/users/{id}": {"get": {"summary": "Get user by ID", "responses": {"200": {"schema": JSON_SCHEMA}}}}
                },
            },
        ),
    ],
)
def test_get_schema_from_response(response, schema):
    assert get_schema_from_response(response, schema) == JSON_SCHEMA


def test_get_model_schemas():
    dataset = "dataset_name"
    resource = "resource_name"
    result = get_model_schemas(dataset, resource, OPENAPI_RESPONSE, OPENAPI_SCHEMA)

    expected = [
        {
            "type": "model",
            "name": f"{dataset}/RelationWithChildAndParents",
            "title": "",
            "description": "",
            "external": {
                "dataset": dataset,
                "name": ".",
                "resource": resource,
            },
            "properties": {
                "created_at": {
                    "type": "datetime",
                    "title": "",
                    "description": "",
                    "external": {"name": "created_at"},
                    "required": True,
                },
                "data": {
                    "type": "ref",
                    "title": "",
                    "description": "",
                    "external": {"name": "data", "prepare": Expr("expand")},
                    "model": f"{dataset}/Data",
                    "required": True,
                },
            },
        },
        {
            "type": "model",
            "name": f"{dataset}/Data",
            "title": "",
            "description": "",
            "external": {
                "dataset": dataset,
                "name": "",
                "resource": resource,
            },
            "properties": {
                "id": {
                    "type": "integer",
                    "title": "",
                    "description": "",
                    "external": {"name": "id"},
                    "required": True,
                },
                "active_from": {
                    "type": "date",
                    "title": "",
                    "description": "",
                    "external": {"name": "active_from"},
                    "required": True,
                },
            },
        },
    ]

    assert result == expected


def test_basic_model():
    dataset = "dataset_name"
    resource = "resource_name"
    basename = "MyModel"
    model = Model(dataset, resource, basename, {})

    assert model.dataset == dataset
    assert model.resource == resource
    assert model.basename == basename
    assert model.json_schema == {}
    assert model.source == basename
    assert model.title == ""
    assert model.description == ""
    assert model.name == f"{dataset}/{to_model_name(basename)}"
    assert model.parent is None

    assert len(model.children) == 0
    assert len(model.properties) == 0


def test_model_add_property():
    model = Model("dataset", "resource", "MyModel", {})

    assert model.properties == []

    prop = model.add_property("MyProperty", {})

    assert model.properties == [prop]


def test_model_add_child():
    model = Model("dataset", "resource", "MyModel", {})
    child = model.add_child("Child", {})
    assert model.children == [child]
    assert child.parent == model


def test_model_with_ref_property():
    schema = {"properties": {"child": {"type": "object", "properties": {}}}}
    model = Model("dataset", "resource", "MyModel", schema)

    prop = model.properties[0]

    child = model.children[0]

    assert len(model.properties) == 1
    assert len(model.children) == 1
    assert prop.ref == child
    assert prop.datatype == "ref"


def test_model_with_backref_property():
    schema = {"properties": {"child": {"type": "array", "items": {"type": "object"}}}}
    model = Model("dataset", "resource", "MyModel", schema)

    assert len(model.properties) == 2

    prop_array = [prop for prop in model.properties if prop.datatype == "array"][0]
    prop_backref = [prop for prop in model.properties if prop.datatype == "backref"][0]

    assert len(model.children) == 1

    child = model.children[0]

    assert prop_array.ref is None
    assert prop_backref.ref == child
    assert prop_backref.datatype == "backref"

    assert f"{prop_array.name}[]" == prop_backref.name

    assert len(child.properties) == 1

    child_prop = child.properties[0]

    assert child_prop.datatype == "ref"
    assert child_prop.ref == model


def test_basic_property():
    schema = {"type": "string", "title": "Title", "description": "Desc"}
    basename = "prop_name"
    prop = Property(basename, schema)

    assert prop.basename == basename
    assert prop.name == to_property_name(basename)
    assert prop.title == "Title"
    assert prop.description == "Desc"
    assert prop.datatype == "string"
    assert prop.required is True
    assert prop.enum == {}


def test_property_nullable():
    schema = {"type": "string", "nullable": True}
    prop = Property("prop", schema)
    assert prop.required is False


def test_property_base64_binary():
    schema = {"type": "string", "contentEncoding": "base64"}
    prop = Property("prop", schema)
    assert prop.datatype == "binary"


def test_property_datetime_format():
    schema = {"type": "string", "format": "date-time"}
    prop = Property("prop", schema)
    assert prop.datatype == "datetime"


def test_property_enum():
    schema = {"type": "string", "enum": ["a", "b", "c"]}
    prop = Property("prop", schema)
    assert prop.enum == {"a": {"source": "a"}, "b": {"source": "b"}, "c": {"source": "c"}}


def test_property_enum_invalid_items():
    schema = {"type": "string", "enum": [{"label": "a"}, ["b"]]}
    prop = Property("prop", schema)
    assert prop.enum == {}
