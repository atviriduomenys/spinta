from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any

from spinta.core.context import configure_context, create_context
from spinta.cli.manifest import _read_and_return_manifest
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

    def get_enum_values(self, model_property) -> list[str]:
        """Extract enum values from property"""
        if not self.is_enum_property(model_property):
            return []

        enum = model_property.enum
        if isinstance(enum, dict):
            return list(enum.keys())
        else:
            return [enum_prop.strip('"') for enum_prop in enum]

    def convert_to_openapi_schema(self, model_property) -> dict[str, Any]:
        """Convert a model property to OpenAPI schema"""

        dtype = model_property.dtype

        if self.is_enum_property(model_property):
            enum_values = self.get_enum_values(model_property)
            return {"type": "string", "enum": enum_values, "example": enum_values[0] if enum_values else "UNKNOWN"}

        if self.is_reference_type(dtype):
            return {
                "$ref": f"#/components/schemas/{dtype.model.basename}",
                "example": {"_type": dtype.model.basename, "_id": "12345678-1234-5678-9abc-123456789012"},
            }

        if self.is_array_type(dtype):
            items_schema = self.convert_to_openapi_schema(dtype.items)
            example_item = items_schema.get("example", "example_item")
            return {"type": "array", "items": items_schema, "example": [example_item]}

        dtype_name = self.get_dtype_name(dtype)
        return self.schema_registry.type_mapping.mappings.get(
            dtype_name, {"type": "string", "example": "Example value"}
        )

    def get_example_value(self, model_property) -> Any:
        """Generate example values for properties"""
        dtype = model_property.dtype

        if self.is_enum_property(model_property):
            enum_values = self.get_enum_values(model_property)
            return enum_values[0] if enum_values else "UNKNOWN"

        if self.is_reference_type(dtype):
            return {"_type": dtype.model.basename, "_id": "12345678-1234-5678-9abc-123456789012"}

        if self.is_array_type(dtype):
            item_example = self.get_example_value(dtype.items)
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

    def create_path_mappings(self, model, dataset_name: str) -> list[tuple[str, str, str, tuple | None]]:
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
        model_name: str,
        path_type: str = "collection",
        model_property: tuple | None = None,
    ) -> dict[str, Any]:
        """Model-specific path creation with schema references"""
        return self.create_path(path_config, model_name=model_name, path_type=path_type, model_property=model_property)

    def create_path(
        self, path_config: dict, model_name: str = None, path_type: str = None, model_property: tuple | None = None
    ) -> dict[str, Any]:
        """Generic path creation for both utility and model endpoints"""
        operations = {}

        if "parameters" in path_config:
            operations["parameters"] = self._build_parameter_refs(path_config["parameters"])

        for method_name, method_config in path_config.items():
            if method_name == "parameters":
                continue

            operations[method_name] = self._build_operation(
                method_config, model_name=model_name, path_type=path_type, model_property=model_property
            )

        return operations

    def _build_operation(
        self, method_config: dict, model_name: str = None, path_type: str = None, model_property: tuple | None = None
    ) -> dict[str, Any]:
        """Build a single operation (get, head, etc.)"""
        operation = {}

        if model_name:
            operation["tags"] = [model_name]
        elif "tags" in method_config:
            operation["tags"] = method_config["tags"]

        if "security" in method_config:
            operation["security"] = method_config["security"]

        for spec_field in ["summary", "description"]:
            if spec_field in method_config:
                operation[spec_field] = method_config[spec_field]

        if "operationId" in method_config:
            operation["operationId"] = self._build_operation_id(
                method_config["operationId"], model_name, model_property
            )

        if "parameters" in method_config:
            operation["parameters"] = self._build_parameter_refs(method_config["parameters"])

        operation["responses"] = self._build_responses(
            method_config.get("responses", {}), model_name, path_type, model_property
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
        self, responses_config: dict, model_name: str = None, path_type: str = None, model_property: tuple | None = None
    ) -> dict[str, Any]:
        """Build all responses for an operation"""
        responses = {}
        for status_code, response_config in responses_config.items():
            responses[status_code] = self._build_response(
                status_code, response_config, model_name, path_type, model_property
            )
        return responses

    def _build_response(
        self,
        status_code: str,
        response_config: dict,
        model_name: str = None,
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
                response_config["content"], model_name, path_type, model_property
            )

        return response

    def _build_response_content(
        self, content_config: dict, model_name: str = None, path_type: str = None, model_property: tuple | None = None
    ) -> dict[str, Any]:
        """Build response content with schema references"""
        content = {}

        for media_type, media_config in content_config.items():
            schema_ref = self._resolve_schema_ref(media_config.get("schema"), model_name, path_type, model_property)
            content[media_type] = {"schema": {"$ref": schema_ref}}

        return content

    def _resolve_schema_ref(
        self,
        schema_name: str = None,
        model_name: str = None,
        path_type: str = None,
        model_property: tuple | None = None,
    ) -> str:
        """Resolve the appropriate schema reference"""

        if model_name and path_type:
            return self._get_model_schema_ref(model_name, path_type, model_property)

        if schema_name:
            return f"#/components/schemas/{schema_name}"

        return f"#/components/schemas/{model_name or 'object'}"

    def _get_model_schema_ref(self, model_name: str, path_type: str, model_property: tuple | None) -> str:
        """Get schema reference for model endpoints based on path type"""
        if path_type == "collection":
            return f"#/components/schemas/{model_name}Collection"
        elif path_type == "changes":
            return f"#/components/schemas/{model_name}Changes"
        elif path_type == "property" and model_property:
            property_dtype = model_property[1].dtype
            base_type = self.dtype_handler.get_dtype_name(property_dtype)
            return f"#/components/schemas/{base_type}"
        else:
            return f"#/components/schemas/{model_name}"


class SchemaGenerator:
    """Handles OpenAPI schema generation for models"""

    def __init__(self, dtype_handler: DataTypeHandler, schema_registry: OpenAPISchemaRegistry):
        self.dtype_handler = dtype_handler
        self.schema_registry = schema_registry

    def _ensure_components_path(self, spec: dict, *path: str):
        """Ensure nested components path exists in spec"""

        current = spec
        for key in path:
            current = current.setdefault(key, {})
        return current

    def create_model_schema(self, model) -> dict[str, Any]:
        """Create OpenAPI schema for a model"""

        properties = self.schema_registry.standard_object_properties.copy()
        required_fields = []

        for prop_name, model_property in model.get_given_properties().items():
            prop_schema = self.dtype_handler.convert_to_openapi_schema(model_property)
            properties[prop_name] = prop_schema

            if hasattr(model_property.dtype, "required") and model_property.dtype.required:
                required_fields.append(prop_name)

        schema = {"type": "object", "properties": properties, "example": self.create_example_object(model)}

        if required_fields:
            schema["required"] = required_fields

        return schema

    def create_model_schemas(self, spec, config: dict, model, path_type):
        """Create model-specific schemas based on path type"""

        schemas = self._ensure_components_path(spec, "components", "schemas")
        model_name = model.basename

        if path_type in ("collection", "single", "changes") and model_name not in schemas:
            schemas[model_name] = self.create_model_schema(model)

        if path_type == "collection":
            schemas[f"{model_name}Collection"] = self.create_collection_schema(model)

        elif path_type == "changes":
            schemas[f"{model_name}Changes"] = self.create_changes_schema(model)
            schemas[f"{model_name}Change"] = self.create_change_schema(model)

    def create_path_component_schemas(self, spec: dict, path_config: dict, component_type: str):
        """Generic method to create component schemas from path config"""

        components = self._ensure_components_path(spec, "components", component_type)

        type_map = {
            "parameters": (PARAMETER_COMPONENTS, self.create_parameter_component, self._collect_parameter_refs),
            "responses": (RESPONSE_COMPONENTS, self.create_response_component, self._collect_response_refs),
            "headers": (HEADER_COMPONENTS, self.create_header_component, self._collect_header_refs),
        }

        if component_type not in type_map:
            raise ValueError(f"Unknown component type: {component_type}")

        config_source, creator_func, collector_func = type_map[component_type]

        refs = collector_func(path_config)

        for ref_key in refs:
            if ref_key in components:
                continue

            config = config_source.get(ref_key)
            if not config:
                raise ValueError(f"No config found for {component_type[:-1]}: {ref_key}")

            components[ref_key] = creator_func(config)

    def _collect_parameter_refs(self, path_config: dict) -> set:
        """Collect parameter references from path config"""
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
        """Collect response references from path config"""
        refs = set()

        for method, method_config in path_config.items():
            if method == "parameters" or not isinstance(method_config, dict):
                continue

            for status_code, response_config in method_config.get("responses", {}).items():
                if isinstance(response_config, dict) and "$ref" in response_config:
                    refs.add(response_config["$ref"])

        return refs

    def _collect_header_refs(self, path_config: dict) -> set:
        """Collect header references from path config"""
        refs = set()

        for method, method_config in path_config.items():
            if method == "parameters" or not isinstance(method_config, dict):
                continue

            for status_code, response_config in method_config.get("responses", {}).items():
                if isinstance(response_config, dict) and "$ref" not in response_config:
                    if "headers" in response_config:
                        refs.update(response_config["headers"])

        return refs

    def create_path_parameters_schemas(self, spec: dict, path_config: dict):
        """Create parameter components from path configuration"""
        self.create_path_component_schemas(spec, path_config, "parameters")

    def create_path_response_schemas(self, spec: dict, path_config: dict):
        """Create response components from path configuration"""
        self.create_path_component_schemas(spec, path_config, "responses")

    def create_path_header_schemas(self, spec: dict, path_config: dict):
        """Create header components from path configuration"""
        self.create_path_component_schemas(spec, path_config, "headers")

    def create_common_schemas(self, spec: dict):
        """Create common error and type schemas"""
        schemas = self._ensure_components_path(spec, "components", "schemas")

        for schema_name, schema_config in COMMON_SCHEMAS.items():
            if schema_name not in schemas:
                schemas[schema_name] = schema_config

    def create_component_from_config(self, config: dict, field_mapping: dict) -> dict[str, Any]:
        """Generic method to create a component from config based on field mapping"""
        component = {}
        for config_key, component_key in field_mapping.items():
            if config_key in config:
                component[component_key] = config[config_key]
        return component

    def create_parameter_component(self, config: dict) -> dict[str, Any]:
        """Create a parameter component from config"""
        return self.create_component_from_config(
            config,
            {"name": "name", "in": "in", "description": "description", "required": "required", "schema": "schema"},
        )

    def create_header_component(self, config: dict) -> dict[str, Any]:
        """Create a header component from config"""
        return self.create_component_from_config(
            config, {"description": "description", "required": "required", "schema": "schema"}
        )

    def create_response_component(self, config: dict) -> dict[str, Any]:
        """Create a response component from config"""
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
        """Convert schema config to proper $ref format"""
        if isinstance(schema_config, str):
            return {"$ref": f"#/components/schemas/{schema_config}"}
        elif isinstance(schema_config, dict) and "oneOf" in schema_config:
            return {"oneOf": [{"$ref": f"#/components/schemas/{schema}"} for schema in schema_config["oneOf"]]}
        return schema_config

    def create_collection_schema(self, model) -> dict[str, Any]:
        """Create collection schema for a model"""
        return {
            "type": "object",
            "properties": {
                "_type": {"type": "string"},
                "_data": {"type": "array", "items": {"$ref": f"#/components/schemas/{model.basename}"}},
            },
        }

    def create_example_object(self, model) -> dict[str, Any]:
        """Create example object for a model"""
        example = {
            "_type": model.basename,
            "_id": "abdd1245-bbf9-4085-9366-f11c0f737c1d",
            "_revision": "16dabe62-61e9-4549-a6bd-07cecfbc3508",
        }

        for prop_name, model_property in model.get_given_properties().items():
            example[prop_name] = self.dtype_handler.get_example_value(model_property)

        return example

    def create_changes_schema(self, model) -> dict[str, Any]:
        """Create changes schema for a model"""
        return {
            "type": "object",
            "properties": {
                "_type": {"type": "string"},
                "_data": {"type": "array", "items": {"$ref": f"#/components/schemas/{model.basename}Change"}},
            },
        }

    def create_change_schema(self, model) -> dict[str, Any]:
        """Create change schema for a model with actual model properties"""
        properties = copy.deepcopy(CHANGE_SCHEMA_EXAMPLE)
        properties["_objectType"]["example"] = model.basename

        for prop_name, model_property in model.get_given_properties().items():
            prop_schema = self.dtype_handler.convert_to_openapi_schema(model_property)

            if "required" in prop_schema:
                del prop_schema["required"]
            properties[prop_name] = prop_schema

        return {"type": "object", "properties": properties, "additionalProperties": False}


class OpenAPIGenerator:
    """Generate OpenAPI specs using manifest data"""

    def __init__(self):
        self.schema_registry = OpenAPISchemaRegistry()
        self.dtype_handler = DataTypeHandler(self.schema_registry)

        self.schema_generator = SchemaGenerator(self.dtype_handler, self.schema_registry)
        self.path_generator = PathGenerator(self.dtype_handler)

    def generate_spec(self, manifest) -> dict[str, Any]:
        """Generate complete OpenAPI specification"""

        spec = {
            "openapi": VERSION,
            "info": INFO,
            "externalDocs": EXTERNAL_DOCS,
            "servers": SERVERS,
            "tags": BASE_TAGS,
            "components": {},
        }

        datasets, models = self._extract_manifest_data(manifest)

        self._override_info(spec, datasets)
        self._set_tags(spec, models)
        self.create_paths(spec, datasets, models)
        self.schema_generator.create_common_schemas(spec)

        return spec

    def _extract_manifest_data(self, manifest) -> tuple[Any, dict]:
        """Extract datasets and models from manifest"""

        context = create_context()
        manifests = [manifest]
        context = configure_context(context, manifests)
        rows = _read_and_return_manifest(context, manifests, check_config=False, load_backends=False)

        datasets = rows.get_objects()["dataset"].items()
        models = rows.get_objects()["model"]

        return datasets, models

    def _override_info(self, spec: dict[str, Any], datasets: dict):
        """Override info section with dataset information"""

        _, dataset = next(iter(datasets))
        spec["info"]["summary"] = dataset.title
        spec["info"]["description"] = dataset.description

    def _set_tags(self, spec: dict[str, Any], models: dict):
        """Override tags with model names"""

        description = "Operations with"

        for model in models.values():
            model_schema_name = model.basename
            spec["tags"].append({"name": model_schema_name, "description": f"{description} {model_schema_name}"})

    def _override_schemas_section(self, spec: dict[str, Any], models: dict):
        """Override schemas section with model schemas"""

        existing_schemas = spec["components"]["schemas"]

        for model in models.values():
            model_schema_name = model.basename

            model_schema = self.schema_generator.create_model_schema(model, models)
            existing_schemas[model_schema_name] = model_schema

            collection_schema = self.schema_generator.create_collection_schema(model)
            existing_schemas[f"{model_schema_name}Collection"] = collection_schema

            changes_schema = self.schema_generator.create_changes_schema(model)
            existing_schemas[f"{model_schema_name}Changes"] = changes_schema

            change_schema = self.schema_generator.create_change_schema(model)
            existing_schemas[f"{model_schema_name}Change"] = change_schema

    def create_paths(self, spec: dict[str, Any], datasets: Any, models: dict):
        paths = {}
        utility_paths = ["/version", "/health"]

        for path in utility_paths:
            path_config = PATHS_CONFIG.get(path)

            if not path_config:
                raise ValueError(f"No config found for path: {path}")

            paths[path] = self.path_generator.create_path(path_config)

        for dataset_name, _ in datasets:
            dataset_models = self._get_dataset_models(dataset_name, models)

            for model in dataset_models:
                self._create_model_paths(spec, paths, model, dataset_name)

        spec["paths"] = paths

    def _get_dataset_models(self, dataset_name: str, models: dict) -> list:
        """Get all models belonging to a specific dataset"""
        return [
            model
            for model in models.values()
            if (
                hasattr(model, "external")
                and hasattr(model.external, "dataset")
                and model.external.dataset.name == dataset_name
            )
        ]

    def _create_model_paths(self, spec: dict[str, Any], paths: dict[str, Any], model, dataset_name: str):
        path_mappings = self.path_generator.create_path_mappings(model, dataset_name)

        for path_key, actual_path, path_type, model_property in path_mappings:
            path_config = PATHS_CONFIG.get(path_key)
            if not path_config:
                raise ValueError(f"No config found for path: {path_key}")

            updated_operations = self.path_generator.create_model_path(
                path_key, path_config, model.basename, path_type, model_property
            )
            paths[actual_path] = updated_operations
            self.schema_generator.create_model_schemas(spec, path_config, model, path_type)
            self.schema_generator.create_path_response_schemas(spec, path_config)
            self.schema_generator.create_path_parameters_schemas(spec, path_config)
            self.schema_generator.create_path_header_schemas(spec, path_config)
