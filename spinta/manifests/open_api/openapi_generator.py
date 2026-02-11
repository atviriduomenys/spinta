from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any

from spinta.core.context import configure_context, create_context
from spinta.cli.manifest import _read_and_return_manifest
from spinta.dimensions.enum.components import EnumItem
from spinta.manifests.open_api.openapi_config import (
    BASE_TAGS,
    COMMON_SCHEMAS,
    HEADER_COMPONENTS,
    INFO,
    EXTERNAL_DOCS,
    STANDARD_OBJECT_PROPERTIES,
    CHANGE_SCHEMA_EXAMPLE,
    RESPONSE_COMPONENTS,
    PROPERTY_TYPES_IN_PATHS,
    PATHS_CONFIG,
    PARAMETER_COMPONENTS,
    PROPERTY_EXAMPLE,
    PROPERTY_MAPPING,
    SERVERS,
    VERSION,
)
from spinta.types.datatype import DataType
from spinta.utils.schema import NA
from spinta.components import Model

UTILITY_PATHS = ["/version", "/health"]

EXAMPLE_UUID_REF_ID = "12345678-1234-5678-9abc-123456789012"
EXAMPLE_UUID_OBJECT_ID = "abdd1245-bbf9-4085-9366-f11c0f737c1d"
EXAMPLE_UUID_REVISION = "16dabe62-61e9-4549-a6bd-07cecfbc3508"


@dataclass
class TypeMapping:
    """Type mapping configuration for OpenAPI schemas"""

    mappings: dict[str, dict[str, Any]] = field(
        default_factory=lambda: PROPERTY_MAPPING,
    )


@dataclass
class ExampleValues:
    """Example values for different data types"""

    values: dict[str, Any] = field(default_factory=lambda: PROPERTY_EXAMPLE)


def _get_schema_name(model: Model) -> str:
    """Convert model name to unique OpenAPI schema name.

    e.g. 'datasets/gov/vssa/demo/Municipality' -> 'datasets_gov_vssa_demo_Municipality'
    """
    return model.name.replace("/", "_")


class OpenAPISchemaRegistry:
    """Central registry for OpenAPI schema definitions and type mappings"""

    def __init__(self):
        self.type_mapping = TypeMapping()
        self.example_values = ExampleValues()
        self.standard_object_properties = STANDARD_OBJECT_PROPERTIES


class DataTypeHandler:
    """Handles data type conversions and schema generation"""

    def __init__(self, schema_registry: OpenAPISchemaRegistry):
        self.schema_registry = schema_registry

    def get_dtype_name(self, dtype) -> str:
        """Extract consistent data type name from dtype object"""
        if hasattr(dtype, "name"):
            return dtype.name
        return getattr(dtype, "__class__", type(dtype)).__name__.lower()

    def is_reference_type(self, dtype) -> bool:
        """Check if dtype is a reference to another model"""
        return hasattr(dtype, "model") and dtype.model is not None

    def is_array_type(self, dtype) -> bool:
        """Check if dtype represents an array/list"""
        return hasattr(dtype, "items")

    def is_enum_property(self, model_property) -> bool:
        """Check if property has enum values"""
        return hasattr(model_property, "enum") and model_property.enum

    def get_enum_value(self, dtype: DataType, enum_item: EnumItem) -> Any:
        """Converts enum value to property type"""
        value = enum_item.prepare if enum_item.prepare and enum_item.prepare is not NA else enum_item.source
        return dtype.load(value)

    def get_enum_values(self, model_property) -> list[str]:
        """Extract enum values from property"""
        if not self.is_enum_property(model_property):
            return []

        enum = model_property.enum
        if isinstance(enum, dict):
            return [self.get_enum_value(model_property.dtype, enum_value) for enum_value in enum.values()]
        else:
            return [enum_prop.strip('"') for enum_prop in enum]

    def convert_to_openapi_schema(self, model_property, schemas: dict | None = None) -> dict[str, Any]:
        """Convert a model property to OpenAPI schema"""

        dtype = model_property.dtype

        if self.is_enum_property(model_property):
            enum_values = self.get_enum_values(model_property)
            dtype_name = self.get_dtype_name(dtype)
            return {
                **copy.deepcopy(self.schema_registry.type_mapping.mappings.get(dtype_name, {"type": "string"})),
                **{"enum": enum_values, "example": enum_values[0] if enum_values else "UNKNOWN"},
            }

        if self.is_reference_type(dtype):
            ref_schema_name = _get_schema_name(dtype.model)
            example = {"_type": dtype.model.basename, "_id": EXAMPLE_UUID_REF_ID}
            if schemas and (ref_schema := schemas.get(ref_schema_name)) and "example" in ref_schema:
                example = ref_schema["example"]
            return {
                "$ref": f"#/components/schemas/{ref_schema_name}",
                "example": example,
            }

        if self.is_array_type(dtype):
            items_schema = self.convert_to_openapi_schema(dtype.items, schemas=schemas)
            example_item = items_schema.get("example", "example_item")
            return {"type": "array", "items": items_schema, "example": [example_item]}

        dtype_name = self.get_dtype_name(dtype)
        return self.schema_registry.type_mapping.mappings.get(
            dtype_name, {"type": "string", "example": "Example value"}
        )

    def get_example_value(self, model_property, schemas: dict | None = None) -> Any:
        """Generate example values for properties. When schemas is provided, use ref schema example for reference types."""
        dtype = model_property.dtype

        if self.is_enum_property(model_property):
            enum_values = self.get_enum_values(model_property)
            return enum_values[0] if enum_values else "UNKNOWN"

        if self.is_reference_type(dtype):
            if schemas and (ref_schema := schemas.get(_get_schema_name(dtype.model))) and "example" in ref_schema:
                return ref_schema["example"]
            return {"_type": dtype.model.basename, "_id": EXAMPLE_UUID_REF_ID}

        if self.is_array_type(dtype):
            item_example = self.get_example_value(dtype.items, schemas=schemas)
            return [item_example]

        dtype_name = self.get_dtype_name(dtype)
        return self.schema_registry.example_values.values.get(dtype_name, "Example value")


class PathGenerator:
    """Handles OpenAPI path generation and operations"""

    def __init__(self, dtype_handler: DataTypeHandler):
        self.dtype_handler = dtype_handler

    def should_create_property_endpoint(self, model_property) -> bool:
        """Determine if a property should have its own endpoint"""
        dtype_name = self.dtype_handler.get_dtype_name(model_property.dtype)
        return dtype_name in PROPERTY_TYPES_IN_PATHS

    def create_path_mappings(self, model: Model, dataset_name: str) -> list[tuple[str, str, str, tuple | None]]:
        """Create path mappings for a model"""
        actual_model_name = model.basename

        path_mappings = [
            ("/{model_name}", f"/{dataset_name}/{actual_model_name}", "collection", None),
            ("/{model_name}/{id}", f"/{dataset_name}/{actual_model_name}/{{id}}", "single", None),
            ("/{model_name}/:changes/{cid}", f"/{dataset_name}/{actual_model_name}/:changes/{{cid}}", "changes", None),
        ]

        for prop_name, model_property in model.get_given_properties().items():
            if self.should_create_property_endpoint(model_property):
                template_path = "/{model_name}/{id}/{field}"
                actual_path = f"/{dataset_name}/{actual_model_name}/{{id}}/{prop_name}"
                path_mappings.append((template_path, actual_path, "property", (prop_name, model_property)))

        return path_mappings

    def create_model_path(
        self,
        path_key: str,
        path_config: dict,
        model: Model,
        path_type: str = "collection",
        model_property: tuple | None = None,
    ) -> dict[str, Any]:
        """Model-specific path creation with schema references"""
        return self.create_path(path_config, model=model, path_type=path_type, model_property=model_property)

    def create_path(
        self, path_config: dict, model=None, path_type: str = None, model_property: tuple | None = None
    ) -> dict[str, Any]:
        """Generic path creation for both utility and model endpoints"""
        operations = {}

        if "parameters" in path_config:
            operations["parameters"] = self._build_parameter_refs(path_config["parameters"])

        for method_name, method_config in path_config.items():
            if method_name == "parameters":
                continue

            operations[method_name] = self._build_operation(
                method_config, model=model, path_type=path_type, model_property=model_property
            )

        return operations

    def _build_operation(
        self, method_config: dict, model=None, path_type: str = None, model_property: tuple | None = None
    ) -> dict[str, Any]:
        """Build a single operation (get, head, etc.)"""
        operation = {}

        if model:
            operation["tags"] = [model.basename]
        elif "tags" in method_config:
            operation["tags"] = method_config["tags"]

        if "security" in method_config:
            operation["security"] = method_config["security"]

        for spec_field in ["summary", "description"]:
            if spec_field in method_config:
                operation[spec_field] = method_config[spec_field]

        if "operationId" in method_config:
            model_name = model.basename if model else None
            operation["operationId"] = self._build_operation_id(
                method_config["operationId"], model_name=model_name, model_property=model_property
            )

        if "parameters" in method_config:
            operation["parameters"] = self._build_parameter_refs(method_config["parameters"])

        operation["responses"] = self._build_responses(
            method_config.get("responses", {}), model, path_type, model_property
        )

        return operation

    def _build_operation_id(self, base_id: str, model_name: str = None, model_property: tuple | None = None) -> str:
        """Build operation ID with optional model and property names"""
        property_name = model_property[0] if model_property else ""
        model_suffix = model_name or ""
        return f"{base_id}{model_suffix}{property_name}"

    def _build_parameter_refs(self, parameters: list) -> list[dict]:
        """Build parameter reference objects"""
        return [{"$ref": f"#/components/parameters/{param}"} for param in parameters]

    def _build_responses(
        self, responses_config: dict, model=None, path_type: str = None, model_property: tuple | None = None
    ) -> dict[str, Any]:
        """Build all responses for an operation"""
        responses = {}
        for status_code, response_config in responses_config.items():
            responses[status_code] = self._build_response(
                status_code, response_config, model, path_type, model_property
            )
        return responses

    def _build_response(
        self,
        status_code: str,
        response_config: dict,
        model=None,
        path_type: str = None,
        model_property: tuple | None = None,
    ) -> dict[str, Any]:
        """Build a single response object"""

        if "$ref" in response_config:
            return {"$ref": f"#/components/responses/{response_config['$ref']}"}

        response = {}

        if "description" in response_config:
            response["description"] = response_config["description"]

        if "headers" in response_config:
            response["headers"] = {
                header: {"$ref": f"#/components/headers/{header}"} for header in response_config["headers"]
            }

        if "content" in response_config and status_code == "200":
            response["content"] = self._build_response_content(
                response_config["content"], model, path_type, model_property
            )

        return response

    def _build_response_content(
        self, content_config: dict, model=None, path_type: str = None, model_property: tuple | None = None
    ) -> dict[str, Any]:
        """Build response content with schema references"""
        content = {}

        for media_type, media_config in content_config.items():
            schema_ref = self._resolve_schema_ref(media_config.get("schema"), model, path_type, model_property)
            content[media_type] = {"schema": {"$ref": schema_ref}}

        return content

    def _resolve_schema_ref(
        self,
        schema_name: str = None,
        model=None,
        path_type: str = None,
        model_property: tuple | None = None,
    ) -> str:
        """Resolve the appropriate schema reference"""
        model_schema_name = _get_schema_name(model) if model else None
        if model_schema_name and path_type:
            return self._get_model_schema_ref(model_schema_name, path_type, model_property)

        if schema_name:
            return f"#/components/schemas/{schema_name}"

        return f"#/components/schemas/{model_schema_name or 'object'}"

    def _get_model_schema_ref(self, model_schema_name: str, path_type: str, model_property: tuple | None) -> str:
        """Get schema reference for model endpoints based on path type"""
        if path_type == "collection":
            return f"#/components/schemas/{model_schema_name}Collection"
        elif path_type == "changes":
            return f"#/components/schemas/{model_schema_name}Changes"
        elif path_type == "property" and model_property:
            property_dtype = model_property[1].dtype
            base_type = self.dtype_handler.get_dtype_name(property_dtype)
            return f"#/components/schemas/{base_type}"
        else:
            return f"#/components/schemas/{model_schema_name}"


class ComponentSchemaBuilder:
    """Creates OpenAPI component schemas (parameters, responses, headers) from path configs."""

    def _ensure_components_path(self, spec: dict, *path: str):
        current = spec
        for key in path:
            current = current.setdefault(key, {})
        return current

    def create_components_for_path(self, spec: dict, path_config: dict):
        """Create all component schemas (parameters, responses, headers) for a path config."""
        self._create_component_schemas(spec, path_config, "parameters")
        self._create_component_schemas(spec, path_config, "responses")
        self._create_component_schemas(spec, path_config, "headers")

    def _create_component_schemas(self, spec: dict, path_config: dict, component_type: str):
        components = self._ensure_components_path(spec, "components", component_type)

        type_map = {
            "parameters": (PARAMETER_COMPONENTS, self._create_parameter, self._collect_parameter_refs),
            "responses": (RESPONSE_COMPONENTS, self._create_response, self._collect_response_refs),
            "headers": (HEADER_COMPONENTS, self._create_header, self._collect_header_refs),
        }

        config_source, creator_func, collector_func = type_map[component_type]

        for ref_key in collector_func(path_config):
            if ref_key in components:
                continue

            config = config_source.get(ref_key)
            if not config:
                raise ValueError(f"No config found for {component_type[:-1]}: {ref_key}")

            components[ref_key] = creator_func(config)

    def _collect_parameter_refs(self, path_config: dict) -> set:
        refs = set()
        if "parameters" in path_config:
            refs.update(path_config["parameters"])
        for method, method_config in path_config.items():
            if method == "parameters" or not isinstance(method_config, dict):
                continue
            if "parameters" in method_config:
                refs.update(method_config["parameters"])
        return refs

    def _collect_response_refs(self, path_config: dict) -> set:
        refs = set()
        for method, method_config in path_config.items():
            if method == "parameters" or not isinstance(method_config, dict):
                continue
            for _, response_config in method_config.get("responses", {}).items():
                if isinstance(response_config, dict) and "$ref" in response_config:
                    refs.add(response_config["$ref"])
        return refs

    def _collect_header_refs(self, path_config: dict) -> set:
        refs = set()
        for method, method_config in path_config.items():
            if method == "parameters" or not isinstance(method_config, dict):
                continue
            for _, response_config in method_config.get("responses", {}).items():
                if isinstance(response_config, dict) and "$ref" not in response_config:
                    if "headers" in response_config:
                        refs.update(response_config["headers"])
        return refs

    def _create_from_config(self, config: dict, field_mapping: dict) -> dict[str, Any]:
        component = {}
        for config_key, component_key in field_mapping.items():
            if config_key in config:
                component[component_key] = config[config_key]
        return component

    def _create_parameter(self, config: dict) -> dict[str, Any]:
        return self._create_from_config(
            config,
            {"name": "name", "in": "in", "description": "description", "required": "required", "schema": "schema"},
        )

    def _create_header(self, config: dict) -> dict[str, Any]:
        return self._create_from_config(
            config, {"description": "description", "required": "required", "schema": "schema"}
        )

    def _create_response(self, config: dict) -> dict[str, Any]:
        response = {}

        if "description" in config:
            response["description"] = config["description"]

        if "headers" in config:
            response["headers"] = {header: {"$ref": f"#/components/headers/{header}"} for header in config["headers"]}

        if "content" in config:
            response["content"] = {}
            for media_type, content_config in config["content"].items():
                schema_config = content_config.get("schema")
                if schema_config:
                    response["content"][media_type] = {"schema": self._build_schema_ref(schema_config)}

        return response

    def _build_schema_ref(self, schema_config) -> dict[str, Any]:
        if isinstance(schema_config, str):
            return {"$ref": f"#/components/schemas/{schema_config}"}
        elif isinstance(schema_config, dict) and "oneOf" in schema_config:
            return {"oneOf": [{"$ref": f"#/components/schemas/{schema}"} for schema in schema_config["oneOf"]]}
        return schema_config


class SchemaGenerator:
    """Handles OpenAPI schema generation for models."""

    def __init__(self, dtype_handler: DataTypeHandler, schema_registry: OpenAPISchemaRegistry):
        self.dtype_handler = dtype_handler
        self.schema_registry = schema_registry

    def create_all_model_schemas(
        self,
        models: dict,
        main_dataset_schema_names: set[str] | None = None,
    ) -> dict[str, Any]:
        """Build all model schemas (main + collection + changes + refs) in one pass.

        Returns the complete schemas dict for components/schemas.

        Ref schemas are built before main schemas so that main model examples
        can include proper ref model examples instead of generic placeholders.
        """
        schemas = {}

        for model in models.values():
            self._create_referenced_model_schemas(schemas, model, main_dataset_schema_names)

        for model in models.values():
            schema_name = _get_schema_name(model)
            schemas[schema_name] = self._create_model_schema(model, schemas)
            schemas[f"{schema_name}Collection"] = self._create_collection_schema(model)
            schemas[f"{schema_name}Changes"] = self._create_changes_schema(model)
            schemas[f"{schema_name}Change"] = self._create_change_schema(model, schemas)

        return schemas

    def _create_model_schema(self, model, schemas: dict | None = None) -> dict[str, Any]:
        properties = self.schema_registry.standard_object_properties.copy()
        required_fields = []

        for prop_name, model_property in model.get_given_properties().items():
            prop_schema = self.dtype_handler.convert_to_openapi_schema(model_property, schemas=schemas)
            properties[prop_name] = prop_schema

            if hasattr(model_property.dtype, "required") and model_property.dtype.required:
                required_fields.append(prop_name)

        schema = {"type": "object", "properties": properties, "example": self._create_example(model, schemas=schemas)}

        if required_fields:
            schema["required"] = required_fields

        return schema

    def _create_example(
        self,
        model,
        property_filter: set[str] | None = None,
        schemas: dict | None = None,
    ) -> dict[str, Any]:
        example = {
            "_type": model.basename,
            "_id": EXAMPLE_UUID_OBJECT_ID,
            "_revision": EXAMPLE_UUID_REVISION,
        }
        for prop_name, model_property in model.get_given_properties().items():
            if property_filter and prop_name not in property_filter:
                continue
            example[prop_name] = self.dtype_handler.get_example_value(model_property, schemas=schemas)
        return example

    def _create_collection_schema(self, model) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "_type": {"type": "string"},
                "_data": {"type": "array", "items": {"$ref": f"#/components/schemas/{_get_schema_name(model)}"}},
            },
        }

    def _create_changes_schema(self, model) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "_type": {"type": "string"},
                "_data": {"type": "array", "items": {"$ref": f"#/components/schemas/{_get_schema_name(model)}Change"}},
            },
        }

    def _create_change_schema(self, model, schemas: dict | None = None) -> dict[str, Any]:
        properties = copy.deepcopy(CHANGE_SCHEMA_EXAMPLE)
        properties["_objectType"]["example"] = model.basename

        for prop_name, model_property in model.get_given_properties().items():
            prop_schema = self.dtype_handler.convert_to_openapi_schema(model_property, schemas=schemas)

            if "required" in prop_schema:
                del prop_schema["required"]
            properties[prop_name] = prop_schema

        return {"type": "object", "properties": properties, "additionalProperties": False}

    def _create_referenced_model_schemas(
        self,
        schemas: dict,
        model,
        main_dataset_schema_names: set[str] | None,
    ) -> None:
        for _, model_property in model.get_given_properties().items():
            dtype = model_property.dtype

            if self.dtype_handler.is_array_type(dtype):
                dtype = dtype.items.dtype if hasattr(dtype, "items") else dtype

            if not self.dtype_handler.is_reference_type(dtype):
                continue

            ref_model = dtype.model
            ref_schema_name = _get_schema_name(ref_model)

            # Skip if already created or if this is a main dataset model (gets full schema later)
            if ref_schema_name in schemas:
                continue
            if main_dataset_schema_names is not None and ref_schema_name in main_dataset_schema_names:
                continue

            refprops = getattr(dtype, "refprops", None) or []
            schemas[ref_schema_name] = self._build_ref_model_schema(
                schemas,
                ref_model,
                refprops,
                main_dataset_schema_names,
            )

    def _resolve_nested_ref_schema_name(
        self,
        schemas: dict,
        nested_ref_model: Model,
        nested_refprops: list,
        main_dataset_schema_names: set[str] | None,
    ) -> str:
        base_name = _get_schema_name(nested_ref_model)
        is_main_dataset_model = main_dataset_schema_names is not None and base_name in main_dataset_schema_names
        schema_name = f"{base_name}_Ref" if is_main_dataset_model else base_name

        if schema_name not in schemas:
            schemas[schema_name] = self._build_ref_model_schema(
                schemas,
                nested_ref_model,
                nested_refprops,
                main_dataset_schema_names,
            )

        return schema_name

    def _build_ref_model_schema(
        self,
        schemas: dict,
        model,
        refprops: list,
        main_dataset_schema_names: set[str] | None,
    ) -> dict[str, Any]:
        properties = self.schema_registry.standard_object_properties.copy()
        required_fields = []

        refprop_names = {prop.name for prop in refprops if hasattr(prop, "name")}

        for prop_name, model_property in model.get_given_properties().items():
            if refprop_names and prop_name not in refprop_names:
                continue

            dtype = model_property.dtype
            inner_dtype = dtype
            if self.dtype_handler.is_array_type(inner_dtype):
                inner_dtype = inner_dtype.items.dtype if hasattr(inner_dtype, "items") else inner_dtype

            if self.dtype_handler.is_reference_type(inner_dtype):
                nested_refprops = getattr(inner_dtype, "refprops", None) or []
                schema_name = self._resolve_nested_ref_schema_name(
                    schemas,
                    inner_dtype.model,
                    nested_refprops,
                    main_dataset_schema_names,
                )
                ref_schema = schemas.get(schema_name)
                example = ref_schema.get("example") if ref_schema else None
                if example is None:
                    example = {
                        "_type": inner_dtype.model.basename,
                        "_id": EXAMPLE_UUID_REF_ID,
                    }
                prop_schema = {
                    "$ref": f"#/components/schemas/{schema_name}",
                    "example": example,
                }
            else:
                prop_schema = self.dtype_handler.convert_to_openapi_schema(model_property)

            properties[prop_name] = prop_schema

            if hasattr(model_property.dtype, "required") and model_property.dtype.required:
                required_fields.append(prop_name)

        example = {
            "_type": model.basename,
            "_id": EXAMPLE_UUID_OBJECT_ID,
            "_revision": EXAMPLE_UUID_REVISION,
        }
        for prop_name, model_property in model.get_given_properties().items():
            if refprop_names and prop_name not in refprop_names:
                continue
            example[prop_name] = self.dtype_handler.get_example_value(model_property, schemas=schemas)

        schema = {"type": "object", "properties": properties, "example": example}

        if required_fields:
            schema["required"] = required_fields

        return schema


class OpenAPIGenerator:
    """Generate OpenAPI specs using manifest data"""

    def __init__(self, main_dataset_name: str | None = None, api_version: str | None = None):
        self.main_dataset_name = main_dataset_name
        self.api_version = api_version if api_version is not None else ""

        self.schema_registry = OpenAPISchemaRegistry()
        self.dtype_handler = DataTypeHandler(self.schema_registry)

        self.schema_generator = SchemaGenerator(self.dtype_handler, self.schema_registry)
        self.path_generator = PathGenerator(self.dtype_handler)
        self.component_builder = ComponentSchemaBuilder()

    def generate_spec(self, manifest) -> dict[str, Any]:
        """Generate complete OpenAPI specification."""
        specification = {
            "openapi": VERSION,
            "info": copy.deepcopy(INFO),
            "externalDocs": copy.deepcopy(EXTERNAL_DOCS),
            "servers": copy.deepcopy(SERVERS),
            "tags": copy.deepcopy(BASE_TAGS),
            "components": {},
        }
        specification["info"]["version"] = self.api_version

        datasets, models = self._extract_manifest_data(manifest)

        if self.main_dataset_name is not None:
            datasets, models = self._filter_by_main_dataset(datasets, models)

        self._override_info(specification, datasets)
        self._set_tags(specification, models)

        main_dataset_schema_names = {_get_schema_name(m) for m in models.values()}
        model_schemas = self.schema_generator.create_all_model_schemas(models, main_dataset_schema_names)
        specification.setdefault("components", {}).setdefault("schemas", {}).update(model_schemas)

        self._create_paths(specification, datasets, models)

        self._create_component_schemas(specification)
        self._add_common_schemas(specification)

        return specification

    def _extract_manifest_data(self, manifest) -> tuple[Any, dict]:
        context = create_context()
        manifests = [manifest]
        context = configure_context(context, manifests)
        rows = _read_and_return_manifest(context, manifests, check_config=False, ensure_backends=False)

        datasets = rows.get_objects()["dataset"].items()
        models = rows.get_objects()["model"]

        return datasets, models

    def _filter_by_main_dataset(self, datasets: Any, models: dict) -> tuple[list, dict]:
        datasets_list = list(datasets)
        filtered_datasets = [(name, dataset) for name, dataset in datasets_list if name == self.main_dataset_name]
        if not filtered_datasets:
            raise ValueError(
                f"Dataset {self.main_dataset_name!r} not found in manifest. "
                f"Available: {[name for name, _ in datasets_list]}"
            )
        filtered_models = {
            key: model
            for key, model in models.items()
            if (
                hasattr(model, "external")
                and hasattr(model.external, "dataset")
                and model.external.dataset.name == self.main_dataset_name
            )
        }
        return filtered_datasets, filtered_models

    def _override_info(self, spec: dict[str, Any], datasets: dict):
        _, dataset = next(iter(datasets))
        spec["info"]["summary"] = dataset.title
        spec["info"]["description"] = dataset.description

    def _set_tags(self, spec: dict[str, Any], models: dict):
        description = "Operations with"
        for model in models.values():
            model_schema_name = model.basename
            spec["tags"].append({"name": model_schema_name, "description": f"{description} {model_schema_name}"})

    def _create_paths(self, spec: dict[str, Any], datasets: Any, models: dict):
        paths = {}

        for path in UTILITY_PATHS:
            path_config = PATHS_CONFIG.get(path)
            if not path_config:
                raise ValueError(f"No config found for path: {path}")
            paths[path] = self.path_generator.create_path(path_config)

        for dataset_name, _ in datasets:
            for model in self._get_dataset_models(dataset_name, models):
                for path_key, actual_path, path_type, model_property in self.path_generator.create_path_mappings(
                    model,
                    dataset_name,
                ):
                    path_config = PATHS_CONFIG.get(path_key)
                    if not path_config:
                        raise ValueError(f"No config found for path: {path_key}")

                    paths[actual_path] = self.path_generator.create_model_path(
                        path_key,
                        path_config,
                        model,
                        path_type,
                        model_property,
                    )

        spec["paths"] = paths

    def _create_component_schemas(self, spec: dict[str, Any]):
        """Build all component schemas (parameters, responses, headers) from all path configs."""
        for path_config in PATHS_CONFIG.values():
            self.component_builder.create_components_for_path(spec, path_config)

    def _add_common_schemas(self, spec: dict[str, Any]) -> None:
        schemas = spec.setdefault("components", {}).setdefault("schemas", {})
        for schema_name, schema_config in COMMON_SCHEMAS.items():
            if schema_name not in schemas:
                schemas[schema_name] = schema_config

    def _get_dataset_models(self, dataset_name: str, models: dict) -> list:
        return [
            model
            for model in models.values()
            if (
                hasattr(model, "external")
                and hasattr(model.external, "dataset")
                and model.external.dataset.name == dataset_name
            )
        ]
