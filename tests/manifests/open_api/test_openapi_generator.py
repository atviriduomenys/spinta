import json
import re

import pytest

from spinta import commands
from spinta.auth import get_scope_name
from spinta.core.enums import Action
from spinta.exceptions import DataServiceNotFound
from spinta.manifests.components import ManifestPath
from spinta.manifests.open_api.helpers import create_openapi_manifest, write_openapi_manifest
from spinta.manifests.open_api.openapi_config import PARAMETER_COMPONENTS, RESPONSE_COMPONENTS
from spinta.manifests.open_api.openapi_generator import AGENT_UTILITY_PATHS
from spinta.manifests.open_api.udts_config import UdtsConfig
from spinta.testing.manifest import load_manifest_get_context
from tests.manifests.open_api.conftest import (
    MANIFEST,
    MANIFEST_WITH_ARRAY_IN_REFERENCE,
    MANIFEST_WITH_ARRAY_LAYERS,
    MANIFEST_WITH_ARRAY_REFS,
    MANIFEST_WITH_COLLIDING_DATASETS,
    MANIFEST_WITH_COLLIDING_EXTERNAL_REFS,
    MANIFEST_WITH_COLLIDING_MODELS,
    MANIFEST_WITH_COLLIDING_OPERATION_IDS,
    MANIFEST_WITH_DECLARED_ID,
    MANIFEST_WITH_ENUM_VALUES,
    MANIFEST_WITH_INTERMEDIATE_TABLE,
    MANIFEST_WITH_NESTED_REF_LEVELS,
    MANIFEST_WITH_REF_SHAPES,
    MANIFEST_WITH_REFS,
    MANIFEST_WITH_SERVICES,
    MANIFEST_WITH_SOAP_PREPARE,
    MANIFEST_WITH_UNNAMABLE_NAMES,
)

SUPPORTED_HTTP_METHODS = {"get", "head"}


@pytest.mark.parametrize("manifest_data", [MANIFEST, MANIFEST_WITH_SOAP_PREPARE])
def test_basic_structure(open_manifest_path_factory, manifest_data):
    open_manifest_path = open_manifest_path_factory(manifest_data)
    open_api_spec = create_openapi_manifest(open_manifest_path)

    expected_keys = {"openapi", "info", "tags", "externalDocs", "paths", "components"}
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
    dataset_name = "datasets/demo/system_data"
    expected_models = ["Organization", "ProcessingUnit"]
    expected_schemas = ["", "Collection"]
    all_model_schemas = [
        f"{dataset_name.replace('/', '_')}_{model}{schema}" for model in expected_models for schema in expected_schemas
    ]
    open_api_spec = create_openapi_manifest(open_manifest_path)
    assert set(all_model_schemas).issubset(set(open_api_spec["components"]["schemas"].keys()))


def test_components_paths(open_manifest_path: ManifestPath):
    expected_models = ["Organization", "ProcessingUnit"]
    expected_path_types = ["", "/{id}"]

    dataset_name = "datasets/demo/system_data"
    all_model_paths = [
        f"/{dataset_name}/{model}{path_type}" for model in expected_models for path_type in expected_path_types
    ]
    open_api_spec = create_openapi_manifest(open_manifest_path)
    actual_paths = set(open_api_spec["paths"].keys())
    expected_paths = set(all_model_paths)

    assert expected_paths.issubset(actual_paths), f"Missing paths: {expected_paths - actual_paths}"

    # A whole manifest export has no data service base, so agent endpoints are
    # given at the addresses the agent serves them at, and the action form,
    # which only an API gateway routes, is left out.
    assert {"/version", "/health", "/auth/token"} <= actual_paths
    assert "/:version" not in actual_paths


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
        for prop_name in properties:
            _test_property_path_content(paths, dataset_name, model_name, prop_name)


def test_multiple_function_calls_do_not_duplicate_specification(open_manifest_path_factory):
    """During subsequent runs, specification generation should not duplicate values."""
    open_manifest_path = open_manifest_path_factory(MANIFEST)
    create_openapi_manifest(open_manifest_path)
    open_manifest_path.file.seek(0)
    open_api_spec = create_openapi_manifest(open_manifest_path, api_version="1.0.0")

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
        # Utility is a default tag, others are generated from models, none is
        # duplicated, and they are sorted, which is how a reader looks a name up.
        "tags": [
            {
                "name": "datasets_demo_system_data_Organization",
                "description": "Operations with datasets_demo_system_data_Organization",
            },
            {
                "name": "datasets_demo_system_data_ProcessingUnit",
                "description": "Operations with datasets_demo_system_data_ProcessingUnit",
            },
            {
                "name": "utility",
                "description": "Utility operations performed on the API itself",
            },
        ],
    }


def _validate_operation_id_contains(operation_id: str, path: str, *required_terms):
    """Validate that operation ID contains all required terms."""
    for term in required_terms:
        assert term in operation_id, f"OperationId '{operation_id}' should contain '{term}' for {path}"


def _validate_operation_structure(operation: dict, tag_name: str, path: str, operation_type="GET"):
    """Validate basic operation structure and return the operation data."""

    assert operation_type.lower() in operation, f"Missing {operation_type} operation in {path}"

    op_data = operation[operation_type.lower()]
    assert "operationId" in op_data, f"Missing operationId in {operation_type} {path}"
    assert "responses" in op_data, f"Missing responses in {operation_type} {path}"
    assert op_data["tags"] == [tag_name], f"Unexpected operation tags {op_data['tags']}"

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


def _test_api_path(paths: dict, path: str, expected_ref: str, model_name: str, tag_name: str, *additional_terms):
    assert path in paths, f"Missing path: {path}"

    operations = paths[path]
    operation_methods = list(operations.keys())

    for method in operation_methods:
        if method == "parameters":
            continue

        op_data = _validate_operation_structure(operations, tag_name, path, method)
        _validate_operation_id_contains(op_data["operationId"], path, model_name, *additional_terms)

        if method.lower() == "get":
            _validate_get_response_schema(op_data["responses"], path, expected_ref)


def _test_collection_path_content(paths: dict, dataset_name: str, model_name: str):
    api_path = f"/{dataset_name}/{model_name}"
    model_schema_name = f"{dataset_name.replace('/', '_')}_{model_name}"
    expected_ref = f"#/components/schemas/{model_schema_name}Collection"
    _test_api_path(paths, api_path, expected_ref, model_name, model_schema_name)


def _test_single_item_path_content(paths: dict, dataset_name: str, model_name: str):
    api_path = f"/{dataset_name}/{model_name}/{{id}}"
    model_schema_name = f"{dataset_name.replace('/', '_')}_{model_name}"
    expected_ref = f"#/components/schemas/{model_schema_name}"
    _test_api_path(paths, api_path, expected_ref, model_name, model_schema_name)


def _test_property_path_content(paths: dict, dataset_name: str, model_name: str, property_name: str):
    path = f"/{dataset_name}/{model_name}/{{id}}/{property_name}"
    model_schema_name = f"{dataset_name.replace('/', '_')}_{model_name}"

    assert path in paths, f"Missing property path: {path}"

    operations = paths[path]
    op_data = _validate_operation_structure(operations, model_schema_name, path)

    _validate_operation_id_contains(op_data["operationId"], path, model_name, property_name)

    responses = op_data["responses"]
    assert "200" in responses, f"Missing 200 response in GET {path}"

    response_200 = responses["200"]
    assert "content" in response_200, f"Missing content in 200 response for {path}"

    # Property endpoints are generated for file and image properties, which
    # serve the file content with the media type it was stored with.
    assert response_200["content"] == {"*/*": {"schema": {"type": "string", "format": "binary"}}}


def test_only_head_and_get_operations(open_manifest_path: ManifestPath):
    open_api_spec = create_openapi_manifest(open_manifest_path)

    paths = open_api_spec["paths"]
    allowed_methods = {method.lower() for method in SUPPORTED_HTTP_METHODS}

    for path, operations in paths.items():
        if path in ("/:token", "/auth/token"):
            continue

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
    expected_path_types = ["", "/{id}"]

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
    dataset_name = "datasets/demo/system_data"
    model_properties = {
        "Organization": ["org_name", "annual_revenue", "coordinates", "established_date"],
        "ProcessingUnit": ["unit_name", "unit_type", "efficiency_rate", "capacity"],
    }

    open_api_spec = create_openapi_manifest(open_manifest_path)

    schemas = open_api_spec["components"]["schemas"]

    for model_name, properties in model_properties.items():
        _test_base_model_schema(schemas, dataset_name, model_name, properties)
        _test_collection_schema(schemas, dataset_name, model_name)


def _test_base_model_schema(schemas: dict, dataset_name: str, model_name: str, expected_properties):
    model_schema_name = f"{dataset_name.replace('/', '_')}_{model_name}"
    schema = schemas[model_schema_name]

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
    # `_type` of a response is the full model name, see `spinta.commands.read`.
    assert example["_type"] == f"{dataset_name}/{model_name}"
    assert "_id" in example
    assert "_revision" in example

    for prop_name in expected_properties:
        assert prop_name in example, f"Missing property {prop_name} in {model_name}"


def _test_collection_schema(schemas: dict, dataset_name: str, model_name: str):
    model_schema_name = f"{dataset_name.replace('/', '_')}_{model_name}"
    collection_schema_name = f"{model_schema_name}Collection"
    schema = schemas[collection_schema_name]

    assert schema["type"] == "object"
    assert "properties" in schema

    # A listing answers with the objects and the next page, and no `_type`.
    properties = schema["properties"]
    assert set(properties) == {"_data", "_page"}

    data_property = properties["_data"]
    assert data_property["type"] == "array"
    assert "items" in data_property
    assert data_property["items"]["$ref"] == f"#/components/schemas/{model_schema_name}"


def test_organization_schema_details(open_manifest_path: ManifestPath):
    dataset_name = "datasets/demo/system_data"
    model_schema_name = f"{dataset_name.replace('/', '_')}_Organization"

    open_api_spec = create_openapi_manifest(open_manifest_path)
    schemas = open_api_spec["components"]["schemas"]

    org_schema = schemas[model_schema_name]
    properties = org_schema["properties"]

    assert properties["org_name"]["type"] == ["string", "null"]
    assert properties["annual_revenue"]["type"] == ["number", "null"]
    assert properties["coordinates"]["type"] == ["string", "null"]
    assert properties["established_date"]["type"] == ["string", "null"]


def test_processing_unit_schema_details(open_manifest_path: ManifestPath):
    dataset_name = "datasets/demo/system_data"
    model_schema_name = f"{dataset_name.replace('/', '_')}_ProcessingUnit"

    open_api_spec = create_openapi_manifest(open_manifest_path)
    schemas = open_api_spec["components"]["schemas"]

    pu_schema = schemas[model_schema_name]
    properties = pu_schema["properties"]

    assert properties["unit_name"]["type"] == ["string", "null"]

    # Optional enum properties list `null` too, otherwise `enum` would reject a
    # value that `type` allows.
    assert properties["unit_type"]["type"] == ["string", "null"]
    assert "enum" in properties["unit_type"]
    expected_enum = ["FAC", "TRT", "OUT", "OTH", None]
    assert set(properties["unit_type"]["enum"]) == set(expected_enum)

    assert properties["unit_version"]["type"] == ["integer", "null"]
    assert "enum" in properties["unit_version"]
    assert set(properties["unit_version"]["enum"]) == {1, 2, None}

    assert properties["unit_kind"]["type"] == ["string", "null"]
    assert "enum" in properties["unit_kind"]
    assert set(properties["unit_kind"]["enum"]) == {"A", "B", None}

    assert properties["efficiency_rate"]["type"] == ["number", "null"]
    assert properties["capacity"]["type"] == ["integer", "null"]


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


def test_cross_dataset_ref_schemas_created(open_manifest_path_factory):
    open_manifest_path = open_manifest_path_factory(MANIFEST_WITH_REFS)
    open_api_spec = create_openapi_manifest(open_manifest_path, main_dataset_name="datasets/gov/cemetery")

    schemas = open_api_spec["components"]["schemas"]

    assert "Territory" in schemas

    assert "datasets_gov_vssa_demo_Municipality" in schemas
    assert "datasets_gov_vssa_demo_County" in schemas


def test_cross_dataset_ref_schemas_have_only_ref_properties(open_manifest_path_factory):
    open_manifest_path = open_manifest_path_factory(MANIFEST_WITH_REFS)
    open_api_spec = create_openapi_manifest(open_manifest_path, main_dataset_name="datasets/gov/cemetery")

    schemas = open_api_spec["components"]["schemas"]

    # A reference of level 4 carries the identifier alone, so that is all its
    # schema holds; neither `_type` nor `_revision` is sent with it.
    municipality_schema = schemas["datasets_gov_vssa_demo_Municipality"]
    assert municipality_schema["type"] == "object"
    assert set(municipality_schema["properties"]) == {"_id"}

    county_schema = schemas["datasets_gov_vssa_demo_County"]
    assert county_schema["type"] == "object"
    county_props = county_schema["properties"]
    assert "id" not in county_props
    assert "title" not in county_props
    assert "population" not in county_props
    assert "_id" in county_props

    municipality_example = municipality_schema["example"]
    assert "_id" in municipality_example
    assert "id" not in municipality_example

    county_example = county_schema["example"]
    assert "_id" in county_example
    assert "id" not in county_example


def test_cross_dataset_ref_properties_use_correct_schema_refs(open_manifest_path_factory):
    open_manifest_path = open_manifest_path_factory(MANIFEST_WITH_REFS)
    open_api_spec = create_openapi_manifest(open_manifest_path, main_dataset_name="datasets/gov/cemetery")

    schemas = open_api_spec["components"]["schemas"]
    territory_schema = schemas["Territory"]
    properties = territory_schema["properties"]

    # Ref properties are not required, so they are wrapped to accept `null`.
    assert properties["city"]["anyOf"] == [
        {"$ref": "#/components/schemas/datasets_gov_vssa_demo_Municipality"},
        {"type": "null"},
    ]
    assert properties["region"]["anyOf"] == [
        {"$ref": "#/components/schemas/datasets_gov_vssa_demo_County"},
        {"type": "null"},
    ]


def test_main_model_ref_properties_have_proper_examples(open_manifest_path_factory):
    open_manifest_path = open_manifest_path_factory(MANIFEST_WITH_REFS)
    open_api_spec = create_openapi_manifest(open_manifest_path, main_dataset_name="datasets/gov/cemetery")

    schemas = open_api_spec["components"]["schemas"]
    territory_schema = schemas["Territory"]

    city_example = territory_schema["properties"]["city"]["example"]
    assert "_id" in city_example, "city example should contain global '_id' field"
    assert "id" not in city_example

    region_example = territory_schema["properties"]["region"]["example"]
    assert "_id" in region_example, "region example should contain global '_id' field"
    assert "id" not in region_example
    assert "title" not in region_example

    schema_example = territory_schema["example"]
    assert isinstance(schema_example["city"], dict)
    assert "_id" in schema_example["city"]
    assert "id" not in schema_example["city"]
    assert isinstance(schema_example["region"], dict)
    assert "_id" in schema_example["region"]
    assert "id" not in schema_example["region"]


def test_cross_dataset_ref_no_paths_for_referenced_models(open_manifest_path_factory):
    open_manifest_path = open_manifest_path_factory(MANIFEST_WITH_REFS)
    open_api_spec = create_openapi_manifest(open_manifest_path, main_dataset_name="datasets/gov/cemetery")

    paths = open_api_spec["paths"]

    assert "/datasets/gov/cemetery/Territory" in paths

    for path in paths:
        assert "Municipality" not in path, f"Unexpected path for referenced model: {path}"
        assert "County" not in path, f"Unexpected path for referenced model: {path}"


def test_cross_dataset_ref_without_filter_all_schemas_exist(open_manifest_path_factory):
    open_manifest_path = open_manifest_path_factory(MANIFEST_WITH_REFS)
    open_api_spec = create_openapi_manifest(open_manifest_path)

    schemas = open_api_spec["components"]["schemas"]

    assert "datasets_gov_cemetery_Territory" in schemas
    assert "datasets_gov_vssa_demo_Municipality" in schemas
    assert "datasets_gov_vssa_demo_County" in schemas


def test_circular_ref_creates_ref_only_schema_for_back_reference(open_manifest_path_factory):
    open_manifest_path = open_manifest_path_factory(MANIFEST_WITH_REFS)
    open_api_spec = create_openapi_manifest(open_manifest_path, main_dataset_name="datasets/gov/cemetery")

    schemas = open_api_spec["components"]["schemas"]

    assert "Territory" in schemas
    full_schema = schemas["Territory"]
    assert "vda_id" in full_schema["properties"]
    assert "cemetery" in full_schema["properties"]
    assert "city" in full_schema["properties"]
    assert "geometry" in full_schema["properties"]


def test_circular_ref_does_not_create_infinite_schemas(open_manifest_path_factory):
    open_manifest_path = open_manifest_path_factory(MANIFEST_WITH_REFS)
    open_api_spec = create_openapi_manifest(open_manifest_path, main_dataset_name="datasets/gov/cemetery")

    schemas = open_api_spec["components"]["schemas"]
    model_schema_names = [
        key
        for key in schemas
        if key.startswith("datasets_")
        or key in ("Territory", "TerritoryCollection", "TerritoryChanges", "TerritoryChange", "Territory_Ref")
    ]
    expected = {
        "Territory",
        "TerritoryCollection",
        "datasets_gov_vssa_demo_County",
        "datasets_gov_giscenter_grpk_Area",
        "datasets_gov_vssa_demo_Municipality",
    }
    assert set(model_schema_names) == expected


def test_api_version(open_manifest_path_factory):
    open_manifest_path = open_manifest_path_factory(MANIFEST)
    open_api_spec = create_openapi_manifest(open_manifest_path, api_version="2.1.8")
    assert open_api_spec["info"]["version"] == "2.1.8"


SERVICE_PATH = "datasets/gov/rc/jadis/at280/1"


def _service_spec(
    open_manifest_path_factory,
    service_path=SERVICE_PATH,
    config=None,
    manifest_data=MANIFEST_WITH_SERVICES,
    **kwargs,
):
    open_manifest_path = open_manifest_path_factory(manifest_data)
    return create_openapi_manifest(open_manifest_path, service_path=service_path, config=config, **kwargs)


def test_service_includes_all_its_datasets(open_manifest_path_factory):
    open_api_spec = _service_spec(open_manifest_path_factory)

    assert set(open_api_spec["paths"]) == {
        "/:version",
        "/:health",
        "/:token",
        "/version",
        "/health",
        "/auth/token",
        "/at280_israsas/DalyvioAsmensIsrasas",
        "/at280_israsas/DalyvioAsmensIsrasas/{id}",
        "/at280_israsas/Adresas",
        "/at280_israsas/Adresas/{id}",
        "/at280_adresai/Adresas",
        "/at280_adresai/Adresas/{id}",
    }


def test_service_filter_matches_on_segment_boundary(open_manifest_path_factory):
    """`.../at280/1` must not match `.../at280/10`."""
    open_api_spec = _service_spec(open_manifest_path_factory)

    assert not [path for path in open_api_spec["paths"] if "at280_kitas" in path]

    other = _service_spec(open_manifest_path_factory, service_path="datasets/gov/rc/jadis/at280/10")
    assert set(other["paths"]) == {
        "/:version",
        "/:health",
        "/:token",
        "/version",
        "/health",
        "/auth/token",
        "/at280_kitas/Adresas",
        "/at280_kitas/Adresas/{id}",
    }


def test_service_of_another_information_system_is_not_included(open_manifest_path_factory):
    open_api_spec = _service_spec(open_manifest_path_factory)

    assert not [path for path in open_api_spec["paths"] if "n249" in path]


def test_service_unknown_path_raises(open_manifest_path_factory):
    with pytest.raises(DataServiceNotFound) as error:
        _service_spec(open_manifest_path_factory, service_path="datasets/gov/rc/jadis/at280/2")

    assert "datasets/gov/rc/jadis/at280/1" in str(error.value)


def test_service_schema_names_are_unique_for_same_model_name(open_manifest_path_factory):
    """Datasets of one service can hold models of the same name."""
    open_api_spec = _service_spec(open_manifest_path_factory)

    schemas = open_api_spec["components"]["schemas"]
    assert "at280_israsas_Adresas" in schemas
    assert "at280_adresai_Adresas" in schemas
    assert "Adresas" not in schemas

    tags = {tag["name"] for tag in open_api_spec["tags"]}
    assert {"at280_israsas_Adresas", "at280_adresai_Adresas"}.issubset(tags)


def test_service_ref_between_datasets_uses_a_reference_schema(open_manifest_path_factory):
    """A reference carries what its level says, not the whole target model."""
    open_api_spec = _service_spec(open_manifest_path_factory)

    schemas = open_api_spec["components"]["schemas"]
    properties = schemas["at280_israsas_DalyvioAsmensIsrasas"]["properties"]
    assert properties["adresas"]["anyOf"] == [
        {"$ref": "#/components/schemas/at280_adresai_Adresas_Ref"},
        {"type": "null"},
    ]

    # The target keeps its full schema, holding every property of the model,
    # while the reference schema holds what a level 4 reference carries.
    assert "gatve" in schemas["at280_adresai_Adresas"]["properties"]
    assert set(schemas["at280_adresai_Adresas_Ref"]["properties"]) == {"_id"}


def test_model_schema_accepts_a_real_reference_value(open_manifest_path_factory):
    """A level 4 reference is serialized as `{"_id": ...}`."""
    open_api_spec = _service_spec(open_manifest_path_factory)

    schema = open_api_spec["components"]["schemas"]["at280_israsas_DalyvioAsmensIsrasas"]
    body = {
        "_type": "datasets/gov/rc/jadis/at280/1/at280_israsas/DalyvioAsmensIsrasas",
        "_id": "abdd1245-bbf9-4085-9366-f11c0f737c1d",
        "_revision": None,
        "kodas": "K1",
        "adresas": {"_id": "12345678-1234-5678-9abc-123456789012"},
    }

    assert not list(_validator(open_api_spec, schema).iter_errors(body))


def test_service_ref_to_missing_dataset_does_not_break_generation(open_manifest_path_factory):
    open_api_spec = _service_spec(open_manifest_path_factory, service_path="datasets/gov/rc/ntr/n249/1")

    properties = open_api_spec["components"]["schemas"]["n249_israsas_Israsas"]["properties"]
    assert "vieta" in properties


def test_service_utility_paths(open_manifest_path_factory):
    open_api_spec = _service_spec(open_manifest_path_factory)

    paths = open_api_spec["paths"]
    # An API gateway reaches an agent endpoint under the data service path, in
    # the action form it routes there; a client calling the agent reaches the
    # same endpoint at the address the agent serves it at. Both are given, and
    # the second carries a server of its own.
    assert paths["/:version"]["get"]["operationId"] == "apiVersion"
    assert paths["/:health"]["get"]["operationId"] == "apiHealth"
    assert paths["/:token"]["post"]["operationId"] == "apiToken"
    assert "servers" not in paths["/:version"]

    assert paths["/version"]["get"]["operationId"] == "apiVersionOfAgent"
    assert paths["/health"]["get"]["operationId"] == "apiHealthOfAgent"
    assert paths["/auth/token"]["post"]["operationId"] == "apiTokenOfAgent"


def test_agent_endpoints_are_the_routes_spinta_serves():
    """The address form has to be an address Spinta answers at."""
    import inspect
    import re

    from spinta.api import init

    routes = set(re.findall(r'Route\("([^"]+)"', inspect.getsource(init)))

    assert set(AGENT_UTILITY_PATHS) <= routes, f"not served: {sorted(set(AGENT_UTILITY_PATHS) - routes)}"


def test_service_agent_endpoints_drop_the_data_service_path(open_manifest_path_factory):
    """They are served by the agent, not under the data service path."""
    config = UdtsConfig(servers=[{"url": "https://get.data.gov.lt"}])
    open_api_spec = _service_spec(open_manifest_path_factory, config=config)

    assert open_api_spec["servers"] == [{"url": f"https://get.data.gov.lt/{SERVICE_PATH}"}]
    for path in ("/version", "/health", "/auth/token"):
        assert open_api_spec["paths"][path]["servers"] == [{"url": "https://get.data.gov.lt"}]


def test_service_health_is_not_authorized(open_manifest_path_factory):
    """A probe calls it without credentials, see `spinta.api.health`."""
    open_api_spec = _service_spec(open_manifest_path_factory)

    assert open_api_spec["paths"]["/:health"]["get"]["security"] == [{}]


def test_service_health_response_matches_what_spinta_answers(open_manifest_path_factory, app):
    """The document has to describe the probe Spinta actually serves."""
    jsonschema = pytest.importorskip("jsonschema")
    open_api_spec = _service_spec(open_manifest_path_factory)
    schemas = open_api_spec["components"]["schemas"]

    response = app.get("/health")

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    jsonschema.validate(response.json(), schemas["health"])


def test_service_security_schemes(open_manifest_path_factory):
    config = UdtsConfig(auth={"token_url": "https://rc-agentas.lt/auth/token"})
    open_api_spec = _service_spec(open_manifest_path_factory, config=config)

    schemes = open_api_spec["components"]["securitySchemes"]
    assert schemes["UAPI_auth"]["flows"]["clientCredentials"]["tokenUrl"] == "https://rc-agentas.lt/auth/token"
    assert schemes["UAPI_client"]["scheme"] == "basic"


def test_service_security_schemes_default_token_url(open_manifest_path_factory):
    config = UdtsConfig(servers=[{"url": "https://get.data.gov.lt"}])
    open_api_spec = _service_spec(open_manifest_path_factory, config=config)

    scheme = open_api_spec["components"]["securitySchemes"]["UAPI_auth"]
    assert scheme["flows"]["clientCredentials"]["tokenUrl"] == f"https://get.data.gov.lt/{SERVICE_PATH}/:token"


def test_service_servers_from_config(open_manifest_path_factory):
    config = UdtsConfig(
        servers=[
            {"url": "https://get.data.gov.lt", "description": "Production"},
            {"url": f"https://test-get.data.gov.lt/{SERVICE_PATH}", "description": "Testing"},
        ]
    )
    open_api_spec = _service_spec(open_manifest_path_factory, config=config)

    assert open_api_spec["servers"] == [
        {"url": f"https://get.data.gov.lt/{SERVICE_PATH}", "description": "Production"},
        {"url": f"https://test-get.data.gov.lt/{SERVICE_PATH}", "description": "Testing"},
    ]


def test_service_servers_without_config_are_relative(open_manifest_path_factory):
    open_api_spec = _service_spec(open_manifest_path_factory)

    assert open_api_spec["servers"] == [{"url": f"/{SERVICE_PATH}"}]


def test_service_info_from_config(open_manifest_path_factory):
    config = UdtsConfig(info={"title": "JADIS", "summary": "Data service", "version": "1"})
    open_api_spec = _service_spec(open_manifest_path_factory, config=config)

    info = open_api_spec["info"]
    assert info["title"] == "JADIS"
    assert info["summary"] == "Data service"
    assert info["version"] == "1"
    # Not taken from any single dataset of the service.
    assert info["description"] != "Išrašo duomenys"


def test_service_api_version_overrides_config(open_manifest_path_factory):
    config = UdtsConfig(info={"version": "1"})
    open_api_spec = _service_spec(open_manifest_path_factory, config=config, api_version="2.1.8")

    assert open_api_spec["info"]["version"] == "2.1.8"


def test_trace_headers_are_not_required(open_manifest_path_factory):
    open_api_spec = _service_spec(open_manifest_path_factory)

    parameters = open_api_spec["components"]["parameters"]
    assert parameters["traceparent"]["required"] is False
    assert parameters["tracestate"]["required"] is False


def test_revision_accepts_null(open_manifest_path_factory):
    open_api_spec = _service_spec(open_manifest_path_factory)

    properties = open_api_spec["components"]["schemas"]["at280_adresai_Adresas"]["properties"]
    assert properties["_revision"]["type"] == ["string", "null"]
    # Required properties keep their plain type.
    assert properties["id"]["type"] == "string"
    assert properties["gatve"]["type"] == ["string", "null"]


def _operation_ids(open_api_spec: dict) -> list[str]:
    return [
        operation["operationId"]
        for operations in open_api_spec["paths"].values()
        for method, operation in operations.items()
        if method != "parameters" and "operationId" in operation
    ]


def test_service_operation_ids_are_unique(open_manifest_path_factory):
    """Same model name in two datasets must not produce the same operation id."""
    open_api_spec = _service_spec(open_manifest_path_factory)

    operation_ids = _operation_ids(open_api_spec)
    assert len(operation_ids) == len(set(operation_ids))
    assert "getAllat280_israsas_Adresas" in operation_ids
    assert "getAllat280_adresai_Adresas" in operation_ids


def test_service_required_enum_property_is_not_nullable(open_manifest_path_factory):
    open_api_spec = _service_spec(open_manifest_path_factory)

    properties = open_api_spec["components"]["schemas"]["at280_adresai_Adresas"]["properties"]
    assert properties["id"]["type"] == "string"
    assert "enum" not in properties["id"]


def test_service_enum_lists_the_values_a_client_sees(open_manifest_path_factory):
    """`prepare` gives the value, `source` only fills in where it is missing."""
    open_api_spec = _service_spec(open_manifest_path_factory, manifest_data=MANIFEST_WITH_ENUM_VALUES)

    properties = open_api_spec["components"]["schemas"]["ds_Testamentas"]["properties"]

    # `0` and an empty string are values, not missing ones.
    assert properties["sudaryta"]["enum"] == [1, 0, None]
    assert properties["zyma"]["enum"] == ["", "V", None]


def test_service_enum_of_formulas_leaves_the_property_alone(open_manifest_path_factory):
    """A formula says what the data does, so there is no value to list."""
    open_api_spec = _service_spec(open_manifest_path_factory, manifest_data=MANIFEST_WITH_ENUM_VALUES)

    rusis = open_api_spec["components"]["schemas"]["ds_Testamentas"]["properties"]["rusis"]

    assert "enum" not in rusis
    assert rusis["type"] == ["integer", "null"]


def test_service_schema_names_hold_only_allowed_characters(open_manifest_path_factory):
    """A component name an institution gives has to pass `^[a-zA-Z0-9._-]+$`."""
    open_api_spec = _service_spec(open_manifest_path_factory, manifest_data=MANIFEST_WITH_UNNAMABLE_NAMES)

    assert "_duom_rink__Esybe" in open_api_spec["components"]["schemas"]


@pytest.mark.models("backends/postgres/Subitem")
def test_object_property_response_matches_what_spinta_answers(model, app, context):
    """The schema of an object property has to describe the subresource."""
    jsonschema = pytest.importorskip("jsonschema")
    app.authmodel(model, ["insert", "getone", "subobj_getone"])
    created = app.post(f"/{model}", json={"subobj": {"foo": "a", "bar": 1}}).json()

    response = app.get(f"/{model}/{created['_id']}/subobj")

    assert response.status_code == 200
    schemas = create_openapi_manifest(context.get("store").manifest)["components"]["schemas"]
    jsonschema.validate(response.json(), schemas["backends_postgres_Subitem_subobj"])


@pytest.mark.models("backends/postgres/Subitem")
def test_file_property_reference_matches_what_spinta_answers(model, app, context):
    """`:ref` answers with what is known about the file, not with the file."""
    jsonschema = pytest.importorskip("jsonschema")
    app.authmodel(model, ["insert", "getone", "pdf_getone"])
    created = app.post(f"/{model}", json={}).json()

    response = app.get(f"/{model}/{created['_id']}/pdf:ref")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    schemas = create_openapi_manifest(context.get("store").manifest)["components"]["schemas"]
    jsonschema.validate(response.json(), schemas["fileRef"])


@pytest.mark.models("backends/postgres/Subitem")
def test_query_example_shape_is_answered_by_spinta(model, app):
    """The query an API client builds out of the examples has to work.

    Which properties the examples name is checked where the document is built,
    see `test_collection_head_takes_the_query_parameter`; what is checked here
    is that a query of that shape, of real property names, is answered.
    """
    app.authmodel(model, ["insert", "getall", "search"])
    app.post(f"/{model}", json={"scalar": "a"})

    response = app.get(f"/{model}?_select=scalar&_limit=10&_sort=scalar")

    assert response.status_code == 200, response.json()


@pytest.mark.models("backends/postgres/Subitem")
def test_identifier_pattern_accepts_the_identifier_spinta_gives(model, app, open_manifest_path_factory):
    """A model keeping a UUID identifier keeps the pattern of one."""
    jsonschema = pytest.importorskip("jsonschema")
    app.authmodel(model, ["insert", "getone"])
    created = app.post(f"/{model}", json={}).json()
    parameters = _service_spec(open_manifest_path_factory)["components"]["parameters"]

    schema = parameters["id_at280_israsas_DalyvioAsmensIsrasas"]["schema"]
    assert "pattern" in schema
    jsonschema.validate(created["_id"], schema)


def test_declared_identifier_is_not_described_as_a_uuid(open_manifest_path_factory):
    """A model can declare `_id` of its own, and then it holds the data key."""
    jsonschema = pytest.importorskip("jsonschema")
    open_api_spec = _service_spec(open_manifest_path_factory, manifest_data=MANIFEST_WITH_DECLARED_ID)
    parameters = open_api_spec["components"]["parameters"]

    identifier = parameters["id_ds_Salis"]
    assert identifier["schema"] == {"type": "string"}
    # The value the data holds, which the pattern of a UUID would reject.
    jsonschema.validate("AE", identifier["schema"])
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate("AE", {**identifier["schema"], "pattern": PARAMETER_COMPONENTS["id"]["schema"]["pattern"]})

    assert open_api_spec["paths"]["/ds/Salis/{id}"]["parameters"][0] == {"$ref": "#/components/parameters/id_ds_Salis"}


def test_example_identifiers_are_not_all_one(open_manifest_path_factory):
    """One identifier everywhere reads as if every model answered the same object."""
    open_api_spec = _service_spec(open_manifest_path_factory)
    schemas = open_api_spec["components"]["schemas"]

    israsas = schemas["at280_israsas_DalyvioAsmensIsrasas"]["example"]
    adresas = schemas["at280_adresai_Adresas"]["example"]

    assert israsas["_id"] != adresas["_id"]
    assert israsas["_id"] != israsas["_revision"]

    # A request and the answer beside it speak about one object.
    identifier = open_api_spec["components"]["parameters"]["id_at280_israsas_DalyvioAsmensIsrasas"]
    assert identifier["schema"]["example"] == israsas["_id"]

    # A reference points at the example of what it references.
    assert israsas["adresas"]["_id"] == adresas["_id"]


def test_generating_twice_gives_the_same_document(open_manifest_path_factory):
    """A regenerated file has to differ only where the manifest did."""
    first = _service_spec(open_manifest_path_factory)
    second = _service_spec(open_manifest_path_factory)

    assert json.dumps(first) == json.dumps(second)


def test_service_requested_scopes_are_declared(open_manifest_path_factory):
    """Every scope an operation requests has to be declared in the flow."""
    open_api_spec = _service_spec(open_manifest_path_factory)

    declared = open_api_spec["components"]["securitySchemes"]["UAPI_auth"]["flows"]["clientCredentials"]["scopes"]
    requested = {
        scope
        for operations in open_api_spec["paths"].values()
        for method, operation in operations.items()
        if method != "parameters" and isinstance(operation, dict)
        for requirement in operation.get("security", [])
        for scope in requirement.get("UAPI_auth", [])
    }

    assert requested
    assert requested <= set(declared)


OTHER_SERVICE_PATH = "datasets/gov/rc/ntr/n249/1"


def _schema_refs(open_api_spec: dict) -> set[str]:
    refs = set()

    def walk(node):
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "$ref" and isinstance(value, str):
                    refs.add(value)
                else:
                    walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(open_api_spec)
    return refs


def test_every_schema_ref_resolves(open_manifest_path_factory):
    """A dangling `$ref` makes the whole document invalid."""
    open_api_spec = _service_spec(open_manifest_path_factory, service_path=OTHER_SERVICE_PATH)

    declared = set(open_api_spec["components"]["schemas"])
    used = {ref for ref in _schema_refs(open_api_spec) if ref.startswith("#/components/schemas/")}

    assert used
    assert {ref for ref in used if ref.rsplit("/", 1)[1] not in declared} == set()


def test_ref_to_missing_dataset_is_an_object(open_manifest_path_factory):
    """Such a `ref` is downgraded to an object, so it must not be typed a string."""
    open_api_spec = _service_spec(open_manifest_path_factory, service_path=OTHER_SERVICE_PATH)

    properties = open_api_spec["components"]["schemas"]["n249_israsas_Israsas"]["properties"]
    assert properties["vieta"]["type"] == ["object", "null"]


def test_yaml_output_has_no_anchors(open_manifest_path_factory, tmp_path):
    """Shared objects would be written as anchors and aliases.

    Two properties referencing one model outside the exported service is the
    case where the same example object used to be reused.
    """
    open_api_spec = _service_spec(open_manifest_path_factory, service_path=OTHER_SERVICE_PATH)

    output = tmp_path / "spec.yaml"
    write_openapi_manifest(open_api_spec, str(output))
    written = output.read_text(encoding="utf-8")

    assert "adresas2" in written
    assert "&id" not in written
    assert "*id" not in written


def test_token_response_requires_rfc_6749_fields(open_manifest_path_factory):
    """Without `required` the response validation would accept an empty body."""
    open_api_spec = _service_spec(open_manifest_path_factory)

    schema = open_api_spec["components"]["schemas"]["token"]
    assert schema["required"] == ["access_token", "token_type"]


def test_schema_names_of_colliding_dataset_paths_are_disambiguated(open_manifest_path_factory):
    """`a_b` and `a/b` map to one name, so one schema would replace the other."""
    open_manifest_path = open_manifest_path_factory(MANIFEST_WITH_COLLIDING_DATASETS)
    open_api_spec = create_openapi_manifest(open_manifest_path, service_path=SERVICE_PATH)

    schemas = open_api_spec["components"]["schemas"]
    assert {"a_b_C", "a_b_C_2"} <= set(schemas)

    # Each path references the schema of its own model.
    referenced = {
        path: operations["get"]["responses"]["200"]["content"]["application/json"]["schema"]["$ref"]
        for path, operations in open_api_spec["paths"].items()
        if path in ("/a/b/C/{id}", "/a_b/C/{id}")
    }
    assert len(set(referenced.values())) == 2
    for path, ref in referenced.items():
        properties = schemas[ref.rsplit("/", 1)[1]]["properties"]
        assert ("x" in properties) == (path == "/a_b/C/{id}")


def test_collection_schema_is_not_taken_by_another_model(open_manifest_path_factory):
    """A model named `DataCollection` must not replace the collection of `Data`."""
    open_manifest_path = open_manifest_path_factory(MANIFEST_WITH_COLLIDING_MODELS)
    open_api_spec = create_openapi_manifest(open_manifest_path, service_path=SERVICE_PATH)

    schemas = open_api_spec["components"]["schemas"]
    collection_ref = open_api_spec["paths"]["/ds/Data"]["get"]["responses"]["200"]["content"]["application/json"][
        "schema"
    ]["$ref"]

    assert "_data" in schemas[collection_ref.rsplit("/", 1)[1]]["properties"]
    assert "y" in schemas["ds_DataCollection_2"]["properties"]


def test_file_and_image_properties_are_objects(open_manifest_path: ManifestPath):
    """Spinta returns an object for them, the content is served by their endpoint."""
    open_api_spec = create_openapi_manifest(open_manifest_path)

    schemas = open_api_spec["components"]["schemas"]
    logo = schemas["datasets_demo_system_data_Organization"]["properties"]["org_logo"]
    specs = schemas["datasets_demo_system_data_ProcessingUnit"]["properties"]["technical_specs"]

    assert logo["anyOf"] == [{"$ref": "#/components/schemas/image"}, {"type": "null"}]
    assert specs["anyOf"] == [{"$ref": "#/components/schemas/file"}, {"type": "null"}]
    assert schemas["image"]["type"] == "object"
    assert schemas["file"]["type"] == "object"


def test_model_operations_accept_namespace_scopes(rc, open_manifest_path: ManifestPath):
    """Spinta accepts a scope of the model or of any namespace above it."""
    open_api_spec = create_openapi_manifest(open_manifest_path)

    context = load_manifest_get_context(rc, MANIFEST, ensure_backends=False)
    manifest = context.get("store").manifest
    model = commands.get_model(context, manifest, "datasets/demo/system_data/Organization")

    requested = [
        requirement["UAPI_auth"][0]
        for requirement in open_api_spec["paths"]["/datasets/demo/system_data/Organization/{id}"]["get"]["security"]
    ]

    assert requested == [
        get_scope_name(context, node, Action.GETONE, is_udts=True) for node in [model, model.ns, *model.ns.parents()]
    ]
    # The namespace of the dataset and the root namespace among them.
    assert "uapi:/datasets/demo/system_data/:getone" in requested
    assert "uapi:/:getone" in requested


def test_hidden_property_takes_its_own_scope_only(rc, open_manifest_path_factory):
    """`spinta.auth.authorized` does not widen a hidden property."""
    from spinta.manifests.open_api.openapi_generator import _authorized_nodes

    context = load_manifest_get_context(rc, MANIFEST, ensure_backends=False)
    manifest = context.get("store").manifest
    model = commands.get_model(context, manifest, "datasets/demo/system_data/Organization")
    prop = model.properties["org_logo"]

    prop.hidden = True
    try:
        assert _authorized_nodes(model, "property", ("org_logo", prop)) == [prop]
    finally:
        prop.hidden = False

    assert _authorized_nodes(model, "property", ("org_logo", prop))[:2] == [prop, model]


def test_model_operations_request_real_scopes(rc, open_manifest_path: ManifestPath):
    """Scopes have to be the ones Spinta itself checks, they are not `uapi:/`.

    Their length depends on `scope_max_length`, so they are compared against
    `spinta.auth`, which builds the scopes Spinta authorizes against.
    """
    open_api_spec = create_openapi_manifest(open_manifest_path)

    context = load_manifest_get_context(rc, MANIFEST, ensure_backends=False)
    manifest = context.get("store").manifest
    model = commands.get_model(context, manifest, "datasets/demo/system_data/Organization")

    def scope(node, action):
        return get_scope_name(context, node, action, is_udts=True)

    paths = open_api_spec["paths"]
    # A collection is read with `getall`, or with `search` when the request
    # narrows it down, and a token carrying either one is enough.
    collection = paths["/datasets/demo/system_data/Organization"]["get"]["security"]
    assert collection[0] == {"UAPI_auth": [scope(model, Action.GETALL)]}
    assert {"UAPI_auth": [scope(model, Action.SEARCH)]} in collection

    assert paths["/datasets/demo/system_data/Organization/{id}"]["get"]["security"][0] == {
        "UAPI_auth": [scope(model, Action.GETONE)],
    }
    assert paths["/datasets/demo/system_data/Organization/{id}/org_logo"]["get"]["security"][0] == {
        "UAPI_auth": [scope(model.properties["org_logo"], Action.GETONE)],
    }


def test_scope_max_length_is_honoured(open_manifest_path: ManifestPath):
    open_api_spec = create_openapi_manifest(open_manifest_path, scope_max_length=200)

    security = open_api_spec["paths"]["/datasets/demo/system_data/Organization/{id}"]["get"]["security"]
    assert security[0] == {"UAPI_auth": ["uapi:/datasets/demo/system_data/Organization/:getone"]}


def test_referenced_models_outside_the_service_get_own_schemas(open_manifest_path_factory):
    """External `a_b/C` and `a/b/C` map to one name, so one would replace the other."""
    open_manifest_path = open_manifest_path_factory(MANIFEST_WITH_COLLIDING_EXTERNAL_REFS)
    open_api_spec = create_openapi_manifest(open_manifest_path, service_path=SERVICE_PATH)

    properties = open_api_spec["components"]["schemas"]["ds_Israsas"]["properties"]
    first = properties["first"]["anyOf"][0]["$ref"]
    second = properties["second"]["anyOf"][0]["$ref"]

    assert first != second
    assert {first.rsplit("/", 1)[1], second.rsplit("/", 1)[1]} <= set(open_api_spec["components"]["schemas"])


def test_model_head_operations_request_scopes(rc, open_manifest_path: ManifestPath):
    """Spinta authorizes `HEAD` against the same actions as `GET`."""
    open_api_spec = create_openapi_manifest(open_manifest_path)

    context = load_manifest_get_context(rc, MANIFEST, ensure_backends=False)
    manifest = context.get("store").manifest
    model = commands.get_model(context, manifest, "datasets/demo/system_data/Organization")

    paths = open_api_spec["paths"]
    collection = paths["/datasets/demo/system_data/Organization"]["head"]["security"]
    assert collection[0] == {"UAPI_auth": [get_scope_name(context, model, Action.GETALL, is_udts=True)]}
    assert {"UAPI_auth": [get_scope_name(context, model, Action.SEARCH, is_udts=True)]} in collection

    assert paths["/datasets/demo/system_data/Organization/{id}"]["head"]["security"][0] == {
        "UAPI_auth": [get_scope_name(context, model, Action.GETONE, is_udts=True)],
    }


def test_scope_prefix_is_configurable(open_manifest_path: ManifestPath):
    """Deployments can override `scope_prefix_udts`."""
    open_api_spec = create_openapi_manifest(open_manifest_path, scope_prefix="kita:/", scope_max_length=200)

    security = open_api_spec["paths"]["/datasets/demo/system_data/Organization/{id}"]["get"]["security"]
    assert security[0] == {"UAPI_auth": ["kita:/datasets/demo/system_data/Organization/:getone"]}
    # Namespaces above the model are alternatives of their own.
    assert {"UAPI_auth": ["kita:/datasets/demo/system_data/:getone"]} in security


def test_file_and_image_schemas_use_runtime_field_names(open_manifest_path: ManifestPath):
    """Spinta names the file `_id`, see `spinta.types.file.components.FileData`."""
    open_api_spec = create_openapi_manifest(open_manifest_path)

    schemas = open_api_spec["components"]["schemas"]
    for name in ("file", "image"):
        # A response carries only these two, see `prepare_dtype_for_response`.
        assert set(schemas[name]["properties"]) == {"_id", "_content_type"}
        # Values are null once the file is deleted.
        assert schemas[name]["properties"]["_id"]["type"] == ["string", "null"]

    example = schemas["datasets_demo_system_data_ProcessingUnit"]["example"]["technical_specs"]
    assert set(example) == {"_id", "_content_type"}


def test_token_endpoint_errors(open_manifest_path_factory):
    """The token endpoint answers with an RFC 6749 error, or with a Spinta one.

    An unknown scope raises `InvalidScopes`, see `tests/test_auth.py`, while
    authlib answers a failed client authentication with an OAuth error.
    """
    open_api_spec = _service_spec(open_manifest_path_factory)

    responses = open_api_spec["paths"]["/:token"]["post"]["responses"]
    assert responses["400"] == {"$ref": "#/components/responses/tokenError400"}
    assert responses["401"] == {"$ref": "#/components/responses/tokenError401"}

    components = open_api_spec["components"]["responses"]
    alternatives = components["tokenError400"]["content"]["application/json"]["schema"]["anyOf"]
    assert alternatives[0] == {"$ref": "#/components/schemas/tokenError"}
    assert _envelope_shape(alternatives[1]) == _errors_envelope(
        {
            "anyOf": [
                {"$ref": "#/components/schemas/InvalidScopes"},
                {"$ref": "#/components/schemas/Error"},
            ],
        },
    )
    assert components["tokenError401"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/tokenError",
    }

    schema = open_api_spec["components"]["schemas"]["tokenError"]
    assert schema["required"] == ["error"]
    assert "invalid_client" in schema["properties"]["error"]["enum"]


def _errors_envelope(items: dict) -> dict:
    """The shape of the envelope, without the descriptions and examples of it."""
    return {"type": "object", "required": ["errors"], "properties": {"errors": {"type": "array", "items": items}}}


def _envelope_shape(schema: dict) -> dict:
    errors = schema["properties"]["errors"]
    return {
        "type": schema["type"],
        "required": schema["required"],
        "properties": {"errors": {"type": errors["type"], "items": errors["items"]}},
    }


def test_error_responses_use_the_spinta_envelope(open_manifest_path_factory):
    """Spinta answers with `{"errors": [...]}`, see `spinta.api.error_response`."""
    open_api_spec = _service_spec(open_manifest_path_factory)

    responses = open_api_spec["components"]["responses"]
    envelope = _envelope_shape(responses["error401"]["content"]["application/json"]["schema"])
    assert envelope == _errors_envelope(
        {
            "anyOf": [
                {"$ref": "#/components/schemas/AuthorizedClientsOnly"},
                {"$ref": "#/components/schemas/BasicAuthRequired"},
                {"$ref": "#/components/schemas/InvalidToken"},
                # Spinta answers with more error codes than a document lists.
                {"$ref": "#/components/schemas/Error"},
            ],
        },
    )

    # A status code with too many errors to name is answered for by `Error`.
    assert _envelope_shape(responses["error400"]["content"]["application/json"]["schema"]) == _errors_envelope(
        {"$ref": "#/components/schemas/Error"},
    )


def test_path_parameters_have_a_placeholder(open_manifest_path: ManifestPath):
    """A path parameter must name a template expression of its path."""
    open_api_spec = create_openapi_manifest(open_manifest_path)

    components = open_api_spec["components"]["parameters"]
    for path, operations in open_api_spec["paths"].items():
        parameters = list(operations.get("parameters", []))
        for method, operation in operations.items():
            if method != "parameters" and isinstance(operation, dict):
                parameters.extend(operation.get("parameters", []))

        for parameter in parameters:
            parameter = components[parameter["$ref"].rsplit("/", 1)[1]]
            if parameter["in"] == "path":
                assert f"{{{parameter['name']}}}" in path, f"{parameter['name']!r} has no placeholder in {path}"


def test_operation_ids_of_colliding_names_are_disambiguated(open_manifest_path_factory):
    """Model `A` with property `bc` and model `Ab` with property `c` build one id."""
    open_manifest_path = open_manifest_path_factory(MANIFEST_WITH_COLLIDING_OPERATION_IDS)
    open_api_spec = create_openapi_manifest(open_manifest_path, service_path=SERVICE_PATH)

    operation_ids = _operation_ids(open_api_spec)
    assert len(operation_ids) == len(set(operation_ids))


def test_token_request_example_uses_a_scope_of_the_service(open_manifest_path_factory):
    """A hardcoded example would disagree with a configured scope prefix."""
    open_api_spec = _service_spec(open_manifest_path_factory, scope_prefix="kita:/")

    content = open_api_spec["paths"]["/:token"]["post"]["requestBody"]["content"]
    example = content["application/x-www-form-urlencoded"]["schema"]["properties"]["scope"]["examples"]
    declared = open_api_spec["components"]["securitySchemes"]["UAPI_auth"]["flows"]["clientCredentials"]["scopes"]

    assert example[0] in declared
    assert example[0].startswith("kita:/")
    # A scope of a model of this data service, not of the agent, which the
    # widest of the declared alternatives, the root namespace, would be.
    assert example[0].startswith(f"kita:/{SERVICE_PATH}/")
    assert example[0] != sorted(declared)[0]


def test_authorized_operations_declare_authentication_errors(open_manifest_path_factory):
    """Response validation has to accept an ordinary authentication failure."""
    open_api_spec = _service_spec(open_manifest_path_factory)

    for path, operations in open_api_spec["paths"].items():
        for method, operation in operations.items():
            if method == "parameters" or not isinstance(operation, dict):
                continue
            if not any("UAPI_auth" in requirement for requirement in operation.get("security", [])):
                continue

            responses = operation["responses"]
            assert "401" in responses, f"{method} {path}"
            assert "403" in responses, f"{method} {path}"


def _validator(open_api_spec: dict, schema: dict):
    """Build a validator of a schema of the generated specification."""
    jsonschema = pytest.importorskip("jsonschema")

    schemas = open_api_spec["components"]["schemas"]
    resolved = json.dumps({"$defs": schemas, **schema}).replace("#/components/schemas/", "#/$defs/")
    return jsonschema.Draft202012Validator(json.loads(resolved))


def _error_body(code: str) -> dict:
    """Error as `spinta.api.error_response` builds it."""
    return {"errors": [{"type": "system", "code": code, "template": "t", "context": {}, "message": "m"}]}


@pytest.mark.parametrize(
    "response, body",
    [
        ("error400", _error_body("UniqueConstraint")),
        # Spinta answers with error codes beyond the ones the response names.
        ("error400", _error_body("SomeOtherError")),
        ("error401", _error_body("InvalidToken")),
        ("error404", _error_body("ItemDoesNotExist")),
        ("tokenError400", {"error": "invalid_client", "error_description": "Invalid client name"}),
        ("tokenError400", _error_body("InvalidScopes")),
    ],
)
def test_error_responses_accept_real_bodies(open_manifest_path_factory, response, body):
    open_api_spec = _service_spec(open_manifest_path_factory)

    schema = open_api_spec["components"]["responses"][response]["content"]["application/json"]["schema"]
    assert not list(_validator(open_api_spec, schema).iter_errors(body))


def test_model_schema_accepts_a_real_object(open_manifest_path: ManifestPath):
    """Values Spinta leaves empty come as null, and a file comes as an object."""
    open_api_spec = create_openapi_manifest(open_manifest_path)

    schema = open_api_spec["components"]["schemas"]["datasets_demo_system_data_ProcessingUnit"]
    body = {
        "_type": "datasets/demo/system_data/ProcessingUnit",
        "_id": "abdd1245-bbf9-4085-9366-f11c0f737c1d",
        "_revision": None,
        "unit_name": None,
        "unit_type": None,
        "technical_specs": {"_id": "specs.pdf", "_content_type": "application/pdf"},
    }

    assert not list(_validator(open_api_spec, schema).iter_errors(body))


def test_service_path_and_main_dataset_name_are_alternatives(open_manifest_path: ManifestPath):
    with pytest.raises(ValueError, match="not both"):
        create_openapi_manifest(
            open_manifest_path,
            main_dataset_name="datasets/demo/system_data",
            service_path=SERVICE_PATH,
        )


def test_yaml_output_has_no_anchors_from_the_configuration(open_manifest_path_factory, tmp_path):
    """A `--udts-cfg` anchor leaves one object reached from two places."""
    shared = {"raktas": "reiksme"}
    config = UdtsConfig(info={"x-bendra": shared, "x-kita": shared}, servers=[{"url": "https://get.data.gov.lt"}])
    open_api_spec = _service_spec(open_manifest_path_factory, config=config)

    output = tmp_path / "spec.yaml"
    write_openapi_manifest(open_api_spec, str(output))
    written = output.read_text(encoding="utf-8")

    assert "x-bendra" in written
    assert "&id" not in written
    assert "*id" not in written


def test_collection_head_takes_the_query_parameter(open_manifest_path_factory):
    """`HEAD` is narrowed down by the same query as `GET`, and takes `:search`."""
    open_api_spec = _service_spec(open_manifest_path_factory)

    operations = open_api_spec["paths"]["/at280_israsas/DalyvioAsmensIsrasas"]
    # Examples name properties of the model, so the query is a parameter of it.
    query = {"$ref": "#/components/parameters/query_at280_israsas_DalyvioAsmensIsrasas"}
    assert query in operations["head"]["parameters"]
    assert query in operations["get"]["parameters"]

    examples = open_api_spec["components"]["parameters"][query["$ref"].rsplit("/", 1)[1]]
    properties = examples["schema"]["properties"]
    assert properties["_select"]["example"] == "kodas,adresas"
    assert properties["_sort"]["example"] == "kodas"

    scopes = [requirement["UAPI_auth"][0] for requirement in operations["head"]["security"]]
    assert any(scope.endswith("/:search") for scope in scopes)


def test_one_referenced_model_gets_a_schema_per_shape(open_manifest_path_factory):
    """A reference carries an `_id` or the natural key, depending on its level."""
    open_manifest_path = open_manifest_path_factory(MANIFEST_WITH_REF_SHAPES)
    open_api_spec = create_openapi_manifest(open_manifest_path, service_path=SERVICE_PATH)

    schemas = open_api_spec["components"]["schemas"]
    global_ref = schemas["pirmas_A"]["properties"]["vieta"]["anyOf"][0]["$ref"].rsplit("/", 1)[1]
    local_ref = schemas["antras_B"]["properties"]["vieta"]["anyOf"][0]["$ref"].rsplit("/", 1)[1]

    assert global_ref != local_ref
    assert "_id" in schemas[global_ref]["properties"]
    assert "kodas" in schemas[local_ref]["properties"]
    assert "_id" not in schemas[local_ref]["properties"]


def test_model_schemas_require_nothing(rc, open_manifest_path_factory):
    """A response carries what the request selected, so nothing is always there.

    A required property of a manifest holds a value in the data; it reaches a
    response only when the request asks for it, and a hidden one is left out of
    an ordinary response altogether.
    """
    open_manifest_path = open_manifest_path_factory(MANIFEST_WITH_SERVICES)
    open_api_spec = create_openapi_manifest(open_manifest_path, service_path=SERVICE_PATH)

    schemas = open_api_spec["components"]["schemas"]
    model_schemas = [name for name in schemas if name.startswith("at280_")]

    assert model_schemas
    for name in model_schemas:
        assert "required" not in schemas[name], name

    # A property holding a value is still not nullable.
    assert schemas["at280_adresai_Adresas"]["properties"]["id"]["type"] == "string"
    assert schemas["at280_adresai_Adresas"]["properties"]["gatve"]["type"] == ["string", "null"]


def test_model_schema_accepts_a_projected_response(open_manifest_path_factory):
    """`?select(gatve)` answers with that property alone."""
    open_api_spec = _service_spec(open_manifest_path_factory)

    schema = open_api_spec["components"]["schemas"]["at280_adresai_Adresas"]

    assert not list(_validator(open_api_spec, schema).iter_errors({"gatve": "Vilniaus"}))


def test_file_download_declares_range_responses(open_manifest_path: ManifestPath):
    """A file is served by `FileResponse`, which answers a range request."""
    open_api_spec = create_openapi_manifest(open_manifest_path)

    operations = open_api_spec["paths"]["/datasets/demo/system_data/Organization/{id}/org_logo"]
    assert {"$ref": "#/components/parameters/Range"} in operations["parameters"]

    # `Range` is a parameter of the path, so a `HEAD` is ranged as well.
    assert "206" in operations["head"]["responses"]
    assert "416" in operations["head"]["responses"]

    responses = operations["get"]["responses"]
    assert "416" in responses
    # A partial response carries the part of the file that was asked for.
    assert responses["206"]["content"] == {"*/*": {"schema": {"type": "string", "format": "binary"}}}
    # A response of a status that carries no body keeps none.
    assert "content" not in responses["304"]


def test_error_responses_name_their_status(open_manifest_path_factory):
    open_api_spec = _service_spec(open_manifest_path_factory)

    assert open_api_spec["components"]["responses"]["error401"]["description"] == "Unauthorized"
    # Read operations do not answer 409, so the response is not emitted.
    assert RESPONSE_COMPONENTS["error409"]["description"] == "Conflict"


def test_array_reference_uses_the_schema_of_its_item(open_manifest_path_factory):
    """The item property carries the level the reference schema is built from."""
    open_manifest_path = open_manifest_path_factory(MANIFEST_WITH_ARRAY_REFS)
    open_api_spec = create_openapi_manifest(open_manifest_path, service_path=SERVICE_PATH)

    schemas = open_api_spec["components"]["schemas"]
    items = schemas["ds_Israsas"]["properties"]["kalbos"]["items"]
    # An optional item accepts a null of the list as well.
    referenced = items["anyOf"][0]["$ref"].rsplit("/", 1)[1]

    assert referenced in schemas
    # Level 3 of the item carries the natural key, not a global `_id`.
    assert "kodas" in schemas[referenced]["properties"]
    assert "_id" not in schemas[referenced]["properties"]


def test_nested_reference_keeps_its_own_level(open_manifest_path_factory):
    """A level 4 reference inside a natural key carries an `_id`, not a key."""
    open_manifest_path = open_manifest_path_factory(MANIFEST_WITH_NESTED_REF_LEVELS)
    open_api_spec = create_openapi_manifest(open_manifest_path, service_path=SERVICE_PATH)

    schemas = open_api_spec["components"]["schemas"]
    outer = schemas["ds_A"]["properties"]["bref"]["anyOf"][0]["$ref"].rsplit("/", 1)[1]
    # Level 3 of `bref` carries the natural key of the target, which is `cref`.
    assert "cref" in schemas[outer]["properties"]

    inner = schemas[outer]["properties"]["cref"]["anyOf"][0]["$ref"].rsplit("/", 1)[1]
    # Level 4 of `cref` carries a global identifier, not the key of its target.
    assert "_id" in schemas[inner]["properties"]
    assert "kodas" not in schemas[inner]["properties"]


def test_array_through_an_intermediate_table_is_a_list(open_manifest_path_factory):
    """Such an array holds the intermediate table in `model`, as a reference does."""
    open_manifest_path = open_manifest_path_factory(MANIFEST_WITH_INTERMEDIATE_TABLE)
    open_api_spec = create_openapi_manifest(open_manifest_path, service_path=SERVICE_PATH)

    schema = open_api_spec["components"]["schemas"]["ds_Israsas"]
    kalbos = schema["properties"]["kalbos"]

    assert kalbos["type"] == ["array", "null"]
    # Items are of the model the array item refers to, not of the intermediate,
    # and an empty item comes as a null of the list.
    assert kalbos["items"]["anyOf"] == [
        {"$ref": "#/components/schemas/ds_Kalba_Ref"},
        {"type": "null"},
    ]
    assert isinstance(schema["example"]["kalbos"], list)


def test_optional_array_item_accepts_null(open_manifest_path_factory):
    """An empty item is serialized as a null of the list."""
    open_manifest_path = open_manifest_path_factory(MANIFEST_WITH_INTERMEDIATE_TABLE)
    open_api_spec = create_openapi_manifest(open_manifest_path, service_path=SERVICE_PATH)

    schema = open_api_spec["components"]["schemas"]["ds_Israsas"]

    assert not list(_validator(open_api_spec, schema).iter_errors({"kalbos": [None]}))


def test_dynamic_array_holds_anything(open_manifest_path_factory):
    """Such an array declares no item property, see `spinta.types.array.link`."""
    open_manifest_path = open_manifest_path_factory(MANIFEST_WITH_ARRAY_LAYERS)
    open_api_spec = create_openapi_manifest(open_manifest_path, service_path=SERVICE_PATH)

    zymos = open_api_spec["components"]["schemas"]["ds_Israsas"]["properties"]["zymos"]

    assert zymos == {"type": ["array", "null"], "example": []}


def test_arrays_of_arrays_keep_every_layer(open_manifest_path_factory):
    open_manifest_path = open_manifest_path_factory(MANIFEST_WITH_ARRAY_LAYERS)
    open_api_spec = create_openapi_manifest(open_manifest_path, service_path=SERVICE_PATH)

    schemas = open_api_spec["components"]["schemas"]
    outer = schemas["ds_Israsas"]["properties"]["kalbos"]
    inner = outer["items"]

    assert outer["type"] == ["array", "null"]
    assert inner["type"] == ["array", "null"]
    # A schema of the innermost reference is built, so the `$ref` resolves.
    assert inner["items"]["anyOf"][0]["$ref"].rsplit("/", 1)[1] in schemas


def test_array_among_reference_properties_stays_a_list(open_manifest_path_factory):
    """A reference schema keeps the array layers of the property it holds."""
    open_manifest_path = open_manifest_path_factory(MANIFEST_WITH_ARRAY_IN_REFERENCE)
    open_api_spec = create_openapi_manifest(open_manifest_path, service_path=SERVICE_PATH)

    schemas = open_api_spec["components"]["schemas"]
    reference = schemas["ds_A"]["properties"]["bref"]["anyOf"][0]["$ref"].rsplit("/", 1)[1]
    kalbos = schemas[reference]["properties"]["kalbos"]

    assert kalbos["type"] == ["array", "null"]
    assert kalbos["items"]["anyOf"][0]["$ref"].rsplit("/", 1)[1] in schemas


@pytest.mark.models("backends/postgres/City")
def test_listing_schema_matches_what_spinta_answers(model, app, context):
    """A listing carries `_data` and `_page`, which the schema has to say."""
    jsonschema = pytest.importorskip("jsonschema")
    app.authmodel(model, ["insert", "getall", "search"])
    app.post(f"/{model}", json={"title": "Vilnius"})
    spec = create_openapi_manifest(context.get("store").manifest)
    schemas = spec["components"]["schemas"]

    response = app.get(f"/{model}?_limit=1")

    assert response.status_code == 200, response.json()
    name = f"{model.replace('/', '_')}Collection"
    resolver = jsonschema.RefResolver.from_schema({"components": {"schemas": schemas}})
    jsonschema.validate(response.json(), {**schemas[name], "components": {"schemas": schemas}}, resolver=resolver)
    assert set(response.json()) <= set(schemas[name]["properties"])


def test_no_component_is_left_unused(open_manifest_path_factory):
    """A component nothing points at reads as a leftover, and a linter says so."""
    open_api_spec = _service_spec(open_manifest_path_factory)
    components = open_api_spec["components"]

    for kind in ("parameters", "headers", "responses"):
        referenced = set(re.findall(rf'"#/components/{kind}/([^"]+)"', json.dumps(open_api_spec)))
        assert set(components.get(kind, {})) == referenced, kind


def test_every_schema_carries_a_description(open_manifest_path_factory):
    """Asked for by the linters an API gateway is checked with."""
    open_api_spec = _service_spec(open_manifest_path_factory)

    undescribed = [
        name for name, schema in open_api_spec["components"]["schemas"].items() if not schema.get("description")
    ]

    assert undescribed == []


def test_every_operation_answers_a_rate_limit(open_manifest_path_factory):
    """Rate limiting is applied in front of the service, not by Spinta."""
    open_api_spec = _service_spec(open_manifest_path_factory)

    for path, operations in open_api_spec["paths"].items():
        for method, operation in operations.items():
            if method in ("parameters", "servers"):
                continue
            assert "429" in operation["responses"], f"{method} {path}"


@pytest.mark.models("backends/postgres/Report")
def test_error_schema_accepts_the_error_spinta_answers(model, app, context):
    """An error object holds five fields, see `spinta.exceptions.error_response`.

    The schema says so and accepts nothing else, so this checks a real one
    against it rather than the five fields being right by memory.
    """
    jsonschema = pytest.importorskip("jsonschema")
    app.authmodel(model, ["getall", "search"])

    response = app.get(f"/{model}?_select=no_such_property")

    assert response.status_code == 400
    assert sorted(response.json()["errors"][0]) == ["code", "context", "message", "template", "type"]
    components = create_openapi_manifest(context.get("store").manifest)["components"]
    schema = components["responses"]["error400"]["content"]["application/json"]["schema"]
    resolver = jsonschema.RefResolver.from_schema({"components": components})
    jsonschema.validate(response.json(), {**schema, "components": components}, resolver=resolver)


@pytest.mark.models("backends/postgres/Report")
def test_error_responses_accept_the_errors_spinta_answers(model, app, context):
    """Every error named in the document has to be one Spinta really answers.

    The named ones pin `code` and `template` to what the class carries, so a
    document naming an error that does not exist, or a template that drifted,
    fails here instead of in an API gateway.
    """
    jsonschema = pytest.importorskip("jsonschema")
    components = create_openapi_manifest(context.get("store").manifest)["components"]

    def check(response_name: str, response) -> None:
        schema = components["responses"][response_name]["content"]["application/json"]["schema"]
        resolver = jsonschema.RefResolver.from_schema({"components": components})
        jsonschema.validate(response.json(), {**schema, "components": components}, resolver=resolver)

    app.authorize([])
    # `authlib` answers this one, so it carries a code and a message alone.
    forbidden = app.get(f"/{model}")
    assert forbidden.json()["errors"][0]["code"] == "InsufficientScopeError"
    check("error403", forbidden)

    app.authmodel(model, ["getone", "getall", "search"])
    check("error404", app.get(f"/{model}/4d741843-4e94-4890-81e9-7ca01b1f96e8"))
    check("error404", app.get("/nera/tokio/Modelio"))
    check("error400", app.get(f"/{model}?_select=no_such_property"))


def test_named_errors_carry_the_template_of_their_class():
    """Copied beside the class, a template drifts; taken from it, it cannot."""
    from spinta import exceptions
    from spinta.manifests.open_api.openapi_config import NAMED_ERRORS

    for errors in NAMED_ERRORS.values():
        for name, schema in errors.items():
            assert schema["properties"]["template"]["const"] == getattr(exceptions, name).template


@pytest.mark.models("backends/postgres/Report")
def test_limit_bounds_are_the_ones_spinta_holds_to(model, app, open_manifest_path_factory):
    """A bound the document states has to be one the service really applies.

    `_limit` below one is refused and there is no upper bound at all, not even
    the width of an integer, so stating one would have a gateway validating
    requests refuse a request Spinta answers.
    """
    app.authmodel(model, ["insert", "getall", "search"])
    app.post(f"/{model}", json={"status": "ok"})

    assert app.get(f"/{model}?_limit=0").status_code == 400
    assert app.get(f"/{model}?_limit=-1").status_code == 400
    assert app.get(f"/{model}?_limit=99999999999999999999").status_code == 200

    parameters = _service_spec(open_manifest_path_factory)["components"]["parameters"]
    limit = parameters["query_at280_israsas_DalyvioAsmensIsrasas"]["schema"]["properties"]["_limit"]
    assert limit["minimum"] == 1
    assert "maximum" not in limit
