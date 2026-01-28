import pytest
from spinta.manifests.components import ManifestPath

from spinta.manifests.open_api.helpers import create_openapi_manifest
from tests.manifests.open_api.conftest import MANIFEST, MANIFEST_WITH_SOAP_PREPARE


SUPPORTED_HTTP_METHODS = {"get", "head"}


@pytest.mark.parametrize("manifest_data", [MANIFEST, MANIFEST_WITH_SOAP_PREPARE])
def test_basic_structure(open_manifest_path_factory, manifest_data):
    open_manifest_path = open_manifest_path_factory(manifest_data)
    open_api_spec = create_openapi_manifest(open_manifest_path)

    expected_keys = {"openapi", "info", "servers", "tags", "externalDocs", "paths", "components"}
    actual_keys = set(open_api_spec.keys())

    missing_keys = expected_keys - actual_keys
    unexpected_keys = actual_keys - expected_keys

    error_messages = []
    if missing_keys:
        error_messages.append(f"Missing keys: {missing_keys}")
    if unexpected_keys:
        error_messages.append(f"Unexpected keys: {unexpected_keys}")

    assert actual_keys == expected_keys, "\n".join(error_messages)


def test_info(open_manifest_path: ManifestPath):
    open_api_spec = create_openapi_manifest(open_manifest_path)
    assert open_api_spec["info"]["summary"] == "Test title"
    assert open_api_spec["info"]["description"] == "Test description"


def test_components_schemas(open_manifest_path: ManifestPath):
    expected_models = ["Organization", "ProcessingUnit"]
    expected_schemas = ["", "Collection", "Changes", "Change"]
    all_model_schemas = [f"{model}{schema}" for model in expected_models for schema in expected_schemas]
    open_api_spec = create_openapi_manifest(open_manifest_path)
    assert set(all_model_schemas).issubset(set(open_api_spec["components"]["schemas"].keys()))


def test_components_paths(open_manifest_path: ManifestPath):
    expected_models = ["Organization", "ProcessingUnit"]
    expected_path_types = ["", "/{id}", "/:changes/{cid}"]

    dataset_name = "datasets/demo/system_data"
    all_model_paths = [
        f"/{dataset_name}/{model}{path_type}" for model in expected_models for path_type in expected_path_types
    ]
    open_api_spec = create_openapi_manifest(open_manifest_path)
    actual_paths = set(open_api_spec["paths"].keys())
    expected_paths = set(all_model_paths)

    assert expected_paths.issubset(actual_paths), f"Missing paths: {expected_paths - actual_paths}"

    assert "/version" in actual_paths
    assert "/health" in actual_paths


def test_model_path_contents(open_manifest_path: ManifestPath):
    dataset_name = "datasets/demo/system_data"

    # image and file
    model_properties = {
        "Organization": ["org_logo"],
        "ProcessingUnit": ["technical_specs"],
    }

    open_api_spec = create_openapi_manifest(open_manifest_path)

    paths = open_api_spec["paths"]

    for model_name, properties in model_properties.items():
        _test_collection_path_content(paths, dataset_name, model_name)
        _test_single_item_path_content(paths, dataset_name, model_name)
        _test_changes_path_content(paths, dataset_name, model_name)
        for prop_name in properties:
            _test_property_path_content(paths, dataset_name, model_name, prop_name)


def test_multiple_function_calls_do_not_duplicate_specification(open_manifest_path_factory):
    """During subsequent runs, specification generation should not duplicate values."""
    open_manifest_path = open_manifest_path_factory(MANIFEST)
    create_openapi_manifest(open_manifest_path)
    open_manifest_path.file.seek(0)
    open_api_spec = create_openapi_manifest(open_manifest_path)

    open_api_spec.pop("components")  # Components do not have a default initial value.
    open_api_spec.pop("paths")  # Paths are not part of the generated specification
    assert open_api_spec == {
        "openapi": "3.1.0",
        "info": {
            "version": "1.0.0",
            "title": "Universal application programming interface",
            "contact": {
                "email": "info@vssa.lt",
                "name": "VSSA",
                "url": "https://vssa.lrv.lt/",
            },
            "license": {
                "name": "CC-BY 4.0",
                "url": "https://creativecommons.org/licenses/by/4.0/",
            },
            "summary": "Test title",
            "description": "Test description",
        },
        "externalDocs": {"url": "https://ivpk.github.io/uapi"},
        "servers": [
            {
                "description": "Data access server",
                "url": "get.data.gov.lt",
            }
        ],
        "tags": [  # Utility is a default tag, others are generated from models. Should not be duplicated.
            {
                "name": "utility",
                "description": "Utility operations performed on the API itself",
            },
            {
                "name": "Organization",
                "description": "Operations with Organization",
            },
            {
                "name": "ProcessingUnit",
                "description": "Operations with ProcessingUnit",
            },
        ],
    }


def _validate_operation_id_contains(operation_id: str, path: str, *required_terms):
    """Validate that operation ID contains all required terms."""
    for term in required_terms:
        assert term in operation_id, f"OperationId '{operation_id}' should contain '{term}' for {path}"


def _validate_operation_structure(operation: dict, model_name: str, path: str, operation_type="GET"):
    """Validate basic operation structure and return the operation data."""

    assert operation_type.lower() in operation, f"Missing {operation_type} operation in {path}"

    op_data = operation[operation_type.lower()]
    assert "operationId" in op_data, f"Missing operationId in {operation_type} {path}"
    assert "responses" in op_data, f"Missing responses in {operation_type} {path}"
    assert op_data["tags"] == [model_name], f"Unexpected operation tags {operation['tags']}"

    return op_data


def _validate_get_response_schema(responses: dict, path: str, expected_ref: str):
    assert "200" in responses, f"Missing 200 response in GET {path}"

    response_200 = responses["200"]
    assert "content" in response_200, f"Missing content in 200 response for {path}"

    content = response_200["content"]
    assert "application/json" in content, f"Missing application/json in {path}"

    json_content = content["application/json"]
    assert "schema" in json_content, f"Missing schema in {path}"

    schema = json_content["schema"]
    assert "$ref" in schema, f"Missing $ref in {path} schema"

    assert schema["$ref"] == expected_ref, f"Schema ref should be '{expected_ref}', got '{schema['$ref']}' for {path}"


def _test_api_path(paths: dict, path: str, expected_ref: str, model_name: str, *additional_terms):
    assert path in paths, f"Missing path: {path}"

    operations = paths[path]
    operation_methods = list(operations.keys())

    for method in operation_methods:
        if method == "parameters":
            continue

        op_data = _validate_operation_structure(operations, model_name, path, method)
        _validate_operation_id_contains(op_data["operationId"], path, model_name, *additional_terms)

        if method.lower() == "get":
            _validate_get_response_schema(op_data["responses"], path, expected_ref)


def _test_collection_path_content(paths: dict, dataset_name: str, model_name: str):
    path = f"/{dataset_name}/{model_name}"
    expected_ref = f"#/components/schemas/{model_name}Collection"
    _test_api_path(paths, path, expected_ref, model_name)


def _test_changes_path_content(paths: dict, dataset_name: str, model_name: str):
    path = f"/{dataset_name}/{model_name}/:changes/{{cid}}"
    expected_ref = f"#/components/schemas/{model_name}Changes"
    _test_api_path(paths, path, expected_ref, model_name)


def _test_single_item_path_content(paths: dict, dataset_name: str, model_name: str):
    path = f"/{dataset_name}/{model_name}/{{id}}"
    expected_ref = f"#/components/schemas/{model_name}"
    _test_api_path(paths, path, expected_ref, model_name)


def _test_property_path_content(paths: dict, dataset_name: str, model_name: str, property_name: str):
    path = f"/{dataset_name}/{model_name}/{{id}}/{property_name}"

    assert path in paths, f"Missing property path: {path}"

    operations = paths[path]
    op_data = _validate_operation_structure(operations, model_name, path)

    _validate_operation_id_contains(op_data["operationId"], path, model_name, property_name)

    responses = op_data["responses"]
    assert "200" in responses, f"Missing 200 response in GET {path}"

    response_200 = responses["200"]
    assert "content" in response_200, f"Missing content in 200 response for {path}"

    content = response_200["content"]
    assert "application/json" in content, f"Missing application/json in {path}"

    json_content = content["application/json"]
    assert "schema" in json_content, f"Missing schema in {path}"

    schema = json_content["schema"]
    assert "$ref" in schema, f"Missing $ref in {path} schema"

    ref = schema["$ref"]
    assert ref.startswith("#/components/schemas/"), (
        f"Property path schema ref should start with '#/components/schemas/', got '{ref}'"
    )


def test_only_head_and_get_operations(open_manifest_path: ManifestPath):
    open_api_spec = create_openapi_manifest(open_manifest_path)

    paths = open_api_spec["paths"]
    allowed_methods = [method.lower() for method in SUPPORTED_HTTP_METHODS]

    for path, operations in paths.items():
        actual_methods = set(operations.keys())

        http_methods = {
            method.lower()
            for method in actual_methods
            if method.lower() in ["get", "post", "put", "patch", "delete", "head", "options", "trace"]
        }

        assert http_methods.issubset(allowed_methods), (
            f"Path '{path}' has disallowed HTTP methods: {http_methods - allowed_methods}. Only GET and HEAD are allowed."
        )


def test_components_paths_with_properties(open_manifest_path: ManifestPath):
    expected_models = ["Organization", "ProcessingUnit"]
    expected_path_types = ["", "/{id}", "/:changes/{cid}"]

    # image and file
    enabled_model_properties = {
        "Organization": ["org_logo"],
        "ProcessingUnit": ["technical_specs"],
    }

    disabled_model_properties = {
        "Organization": ["org_name", "annual_revenue", "coordinates", "established_date"],
        "ProcessingUnit": ["unit_name", "unit_type", "efficiency_rate", "capacity"],
    }

    dataset_name = "datasets/demo/system_data"
    all_expected_paths = []
    not_expected_paths = []

    for model in expected_models:
        for path_type in expected_path_types:
            all_expected_paths.append(f"/{dataset_name}/{model}{path_type}")

        for property_name in enabled_model_properties[model]:
            property_path = f"/{dataset_name}/{model}/{{id}}/{property_name}"
            all_expected_paths.append(property_path)

        for property_name in disabled_model_properties[model]:
            property_path = f"/{dataset_name}/{model}/{{id}}/{property_name}"
            not_expected_paths.append(property_path)

    open_api_spec = create_openapi_manifest(open_manifest_path)

    actual_paths = set(open_api_spec["paths"].keys())
    expected_paths = set(all_expected_paths)
    not_expected_paths = set(not_expected_paths)

    missing_paths = expected_paths - actual_paths
    assert not missing_paths, f"Missing paths: {missing_paths}"

    extraneous_paths = not_expected_paths & actual_paths
    assert not extraneous_paths, f"Extraneous paths: {extraneous_paths}"


def test_model_schema_content(open_manifest_path: ManifestPath):
    model_properties = {
        "Organization": ["org_name", "annual_revenue", "coordinates", "established_date"],
        "ProcessingUnit": ["unit_name", "unit_type", "efficiency_rate", "capacity"],
    }

    open_api_spec = create_openapi_manifest(open_manifest_path)

    schemas = open_api_spec["components"]["schemas"]

    for model_name, properties in model_properties.items():
        _test_base_model_schema(schemas, model_name, properties)
        _test_collection_schema(schemas, model_name)
        _test_changes_schema(schemas, model_name)
        _test_change_schema(schemas, model_name, properties)


def _test_base_model_schema(schemas: dict, model_name: str, expected_properties):
    schema = schemas[model_name]

    assert schema["type"] == "object"
    assert "properties" in schema
    assert "example" in schema

    properties = schema["properties"]

    standard_properties = ["_type", "_id", "_revision"]
    for std_prop in standard_properties:
        assert std_prop in properties, f"Missing standard property {std_prop} in {model_name}"

    for prop_name in expected_properties:
        assert prop_name in properties, f"Missing property {prop_name} in {model_name} schema"

        prop_schema = properties[prop_name]
        assert "type" in prop_schema or "$ref" in prop_schema, f"Property {prop_name} missing type/ref in {model_name}"

    example = schema["example"]
    assert example["_type"] == model_name
    assert "_id" in example
    assert "_revision" in example

    for prop_name in expected_properties:
        assert prop_name in example, f"Missing property {prop_name} in {model_name}"


def _test_collection_schema(schemas: dict, model_name: str):
    collection_schema_name = f"{model_name}Collection"
    schema = schemas[collection_schema_name]

    assert schema["type"] == "object"
    assert "properties" in schema

    properties = schema["properties"]
    assert "_type" in properties
    assert "_data" in properties

    data_property = properties["_data"]
    assert data_property["type"] == "array"
    assert "items" in data_property
    assert data_property["items"]["$ref"] == f"#/components/schemas/{model_name}"


def _test_changes_schema(schemas: dict, model_name: str):
    changes_schema_name = f"{model_name}Changes"
    schema = schemas[changes_schema_name]

    assert schema["type"] == "object"
    assert "properties" in schema

    properties = schema["properties"]
    assert "_type" in properties
    assert "_data" in properties

    data_property = properties["_data"]
    assert data_property["type"] == "array"
    assert "items" in data_property
    assert data_property["items"]["$ref"] == f"#/components/schemas/{model_name}Change"


def _test_change_schema(schemas: dict, model_name: str, expected_properties):
    change_schema_name = f"{model_name}Change"
    schema = schemas[change_schema_name]

    assert schema["type"] == "object"
    assert "properties" in schema
    assert schema.get("additionalProperties") is False

    properties = schema["properties"]

    change_properties = ["_cid", "_id", "_rev", "_txn", "_created", "_op", "_objectType"]
    for change_prop in change_properties:
        assert change_prop in properties, f"Missing change property {change_prop} in {change_schema_name}"

    op_property = properties["_op"]
    assert op_property["type"] == "string"
    assert "enum" in op_property
    expected_ops = ["insert", "patch", "delete"]
    assert set(op_property["enum"]) == set(expected_ops)

    object_type_property = properties["_objectType"]
    assert object_type_property["example"] == model_name

    for prop_name in expected_properties:
        assert prop_name in properties, f"Missing model property {prop_name} in {change_schema_name}"


def test_organization_schema_details(open_manifest_path: ManifestPath):
    open_api_spec = create_openapi_manifest(open_manifest_path)
    schemas = open_api_spec["components"]["schemas"]

    org_schema = schemas["Organization"]
    properties = org_schema["properties"]

    assert properties["org_name"]["type"] == "string"
    assert properties["annual_revenue"]["type"] == "number"
    assert properties["coordinates"]["type"] == "string"
    assert properties["established_date"]["type"] == "string"

    org_change_schema = schemas["OrganizationChange"]
    change_properties = org_change_schema["properties"]

    assert "_cid" in change_properties
    assert "_op" in change_properties
    assert "org_name" in change_properties
    assert "annual_revenue" in change_properties
    assert "coordinates" in change_properties
    assert "established_date" in change_properties


def test_processing_unit_schema_details(open_manifest_path: ManifestPath):
    open_api_spec = create_openapi_manifest(open_manifest_path)
    schemas = open_api_spec["components"]["schemas"]

    pu_schema = schemas["ProcessingUnit"]
    properties = pu_schema["properties"]

    assert properties["unit_name"]["type"] == "string"
    assert properties["unit_type"]["type"] == "string"
    assert "enum" in properties["unit_type"]
    expected_enum = ["FAC", "TRT", "OUT", "OTH"]
    assert set(properties["unit_type"]["enum"]) == set(expected_enum)

    assert properties["efficiency_rate"]["type"] == "number"
    assert properties["capacity"]["type"] == "integer"

    pu_change_schema = schemas["ProcessingUnitChange"]
    change_properties = pu_change_schema["properties"]

    assert "unit_type" in change_properties
    unit_type_change = change_properties["unit_type"]
    assert "enum" in unit_type_change
    assert set(unit_type_change["enum"]) == set(expected_enum)


def test_version_schema_structure(open_manifest_path: ManifestPath):
    open_api_spec = create_openapi_manifest(open_manifest_path)
    version_schema = open_api_spec["components"]["schemas"]["version"]

    properties = version_schema["properties"]

    assert set(properties.keys()) == {
        "api",
        "implementation",
        "dsa",
        "uapi",
        "build",
    }

    assert properties["api"]["properties"]["version"]["type"] == "string"

    implementation = properties["implementation"]["properties"]

    assert implementation["name"]["type"] == "string"
    assert implementation["version"]["type"] == "string"
    assert properties["dsa"]["properties"]["version"]["type"] == "string"
    assert properties["uapi"]["properties"]["version"]["type"] == "string"
    assert properties["build"]["properties"]["version"]["type"] == "string"
