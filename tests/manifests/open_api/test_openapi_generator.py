import pytest

from spinta import commands
from spinta.auth import get_scope_name
from spinta.core.enums import Action
from spinta.exceptions import DataServiceNotFound
from spinta.manifests.components import ManifestPath
from spinta.manifests.open_api.helpers import create_openapi_manifest, write_openapi_manifest
from spinta.manifests.open_api.udts_config import UdtsConfig
from spinta.testing.manifest import load_manifest_get_context
from tests.manifests.open_api.conftest import (
    MANIFEST,
    MANIFEST_WITH_COLLIDING_DATASETS,
    MANIFEST_WITH_COLLIDING_EXTERNAL_REFS,
    MANIFEST_WITH_COLLIDING_MODELS,
    MANIFEST_WITH_REFS,
    MANIFEST_WITH_SERVICES,
    MANIFEST_WITH_SOAP_PREPARE,
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

    # Agent level endpoints are given in action form, `/health` is not
    # implemented by Spinta API, so it must not be declared.
    assert "/:version" in actual_paths
    assert "/:token" in actual_paths
    assert "/health" not in actual_paths


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
        "tags": [  # Utility is a default tag, others are generated from models. Should not be duplicated.
            {
                "name": "utility",
                "description": "Utility operations performed on the API itself",
            },
            {
                "name": "datasets_demo_system_data_Organization",
                "description": "Operations with datasets_demo_system_data_Organization",
            },
            {
                "name": "datasets_demo_system_data_ProcessingUnit",
                "description": "Operations with datasets_demo_system_data_ProcessingUnit",
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
        if path == "/:token":
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
    assert example["_type"] == model_name
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

    properties = schema["properties"]
    assert "_type" in properties
    assert "_data" in properties

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

    municipality_schema = schemas["datasets_gov_vssa_demo_Municipality"]
    assert municipality_schema["type"] == "object"
    municipality_props = municipality_schema["properties"]
    assert "id" not in municipality_props
    assert "_type" in municipality_props
    assert "_id" in municipality_props
    assert "_revision" in municipality_props

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


def _service_spec(open_manifest_path_factory, service_path=SERVICE_PATH, config=None, **kwargs):
    open_manifest_path = open_manifest_path_factory(MANIFEST_WITH_SERVICES)
    return create_openapi_manifest(open_manifest_path, service_path=service_path, config=config, **kwargs)


def test_service_includes_all_its_datasets(open_manifest_path_factory):
    open_api_spec = _service_spec(open_manifest_path_factory)

    assert set(open_api_spec["paths"]) == {
        "/:version",
        "/:token",
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
    assert set(other["paths"]) == {"/:version", "/:token", "/at280_kitas/Adresas", "/at280_kitas/Adresas/{id}"}


def test_service_of_another_information_system_is_not_included(open_manifest_path_factory):
    open_api_spec = _service_spec(open_manifest_path_factory)

    assert not [path for path in open_api_spec["paths"] if "n249" in path]


def test_service_unknown_path_raises(open_manifest_path_factory):
    with pytest.raises(DataServiceNotFound) as error:
        _service_spec(open_manifest_path_factory, service_path="datasets/gov/rc/jadis/at280/2")

    assert "datasets/gov/rc/jadis/at280/1" in str(error.value)


def test_service_schema_names_are_unique_for_same_model_name(open_manifest_path_factory):
    """Data sets of one service can hold models of the same name."""
    open_api_spec = _service_spec(open_manifest_path_factory)

    schemas = open_api_spec["components"]["schemas"]
    assert "at280_israsas_Adresas" in schemas
    assert "at280_adresai_Adresas" in schemas
    assert "Adresas" not in schemas

    tags = {tag["name"] for tag in open_api_spec["tags"]}
    assert {"at280_israsas_Adresas", "at280_adresai_Adresas"}.issubset(tags)


def test_service_ref_between_datasets_uses_full_schema(open_manifest_path_factory):
    open_api_spec = _service_spec(open_manifest_path_factory)

    schemas = open_api_spec["components"]["schemas"]
    properties = schemas["at280_israsas_DalyvioAsmensIsrasas"]["properties"]
    assert properties["adresas"]["anyOf"] == [
        {"$ref": "#/components/schemas/at280_adresai_Adresas"},
        {"type": "null"},
    ]


def test_service_ref_to_missing_dataset_does_not_break_generation(open_manifest_path_factory):
    open_api_spec = _service_spec(open_manifest_path_factory, service_path="datasets/gov/rc/ntr/n249/1")

    properties = open_api_spec["components"]["schemas"]["n249_israsas_Israsas"]["properties"]
    assert "vieta" in properties


def test_service_utility_paths(open_manifest_path_factory):
    open_api_spec = _service_spec(open_manifest_path_factory)

    paths = open_api_spec["paths"]
    assert "/health" not in paths
    assert paths["/:version"]["get"]["operationId"] == "apiVersion"
    assert paths["/:token"]["post"]["operationId"] == "apiToken"


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
    # Not taken from any single data set of the service.
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
    """Same model name in two data sets must not produce the same operation id."""
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
    assert paths["/datasets/demo/system_data/Organization"]["get"]["security"] == [
        {"UAPI_auth": [scope(model, Action.GETALL)]},
        {"UAPI_auth": [scope(model, Action.SEARCH)]},
    ]
    assert paths["/datasets/demo/system_data/Organization/{id}"]["get"]["security"] == [
        {"UAPI_auth": [scope(model, Action.GETONE)]},
    ]
    assert paths["/datasets/demo/system_data/Organization/{id}/org_logo"]["get"]["security"] == [
        {"UAPI_auth": [scope(model.properties["org_logo"], Action.GETONE)]},
    ]


def test_scope_max_length_is_honoured(open_manifest_path: ManifestPath):
    open_api_spec = create_openapi_manifest(open_manifest_path, scope_max_length=200)

    security = open_api_spec["paths"]["/datasets/demo/system_data/Organization/{id}"]["get"]["security"]
    assert security == [{"UAPI_auth": ["uapi:/datasets/demo/system_data/Organization/:getone"]}]


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
    assert paths["/datasets/demo/system_data/Organization"]["head"]["security"] == [
        {"UAPI_auth": [get_scope_name(context, model, Action.GETALL, is_udts=True)]},
        {"UAPI_auth": [get_scope_name(context, model, Action.SEARCH, is_udts=True)]},
    ]
    assert paths["/datasets/demo/system_data/Organization/{id}"]["head"]["security"] == [
        {"UAPI_auth": [get_scope_name(context, model, Action.GETONE, is_udts=True)]},
    ]


def test_scope_prefix_is_configurable(open_manifest_path: ManifestPath):
    """Deployments can override `scope_prefix_udts`."""
    open_api_spec = create_openapi_manifest(open_manifest_path, scope_prefix="kita:/", scope_max_length=200)

    security = open_api_spec["paths"]["/datasets/demo/system_data/Organization/{id}"]["get"]["security"]
    assert security == [{"UAPI_auth": ["kita:/datasets/demo/system_data/Organization/:getone"]}]


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


def test_token_endpoint_errors_are_oauth_errors(open_manifest_path_factory):
    """The token endpoint answers with an RFC 6749 error, not with a Spinta one."""
    open_api_spec = _service_spec(open_manifest_path_factory)

    responses = open_api_spec["paths"]["/:token"]["post"]["responses"]
    assert responses["400"] == {"$ref": "#/components/responses/tokenError400"}
    assert responses["401"] == {"$ref": "#/components/responses/tokenError401"}

    for name in ("tokenError400", "tokenError401"):
        response = open_api_spec["components"]["responses"][name]
        assert response["content"]["application/json"]["schema"] == {"$ref": "#/components/schemas/tokenError"}

    schema = open_api_spec["components"]["schemas"]["tokenError"]
    assert schema["required"] == ["error"]
    assert "invalid_client" in schema["properties"]["error"]["enum"]
