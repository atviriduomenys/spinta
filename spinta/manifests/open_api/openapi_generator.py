from __future__ import annotations

from dataclasses import dataclass, field
import json
import copy
from pathlib import Path
from typing import Any

from spinta.core.context import configure_context, create_context
from spinta.cli.manifest import _read_and_return_manifest


SUPPORTED_HTTP_METHODS = {"GET", "HEAD"}
PROPERTY_TYPES_IN_PATHS = {"file", "image"}
SUPPORTED_MEDIA_TYPES = {"application/json"}



@dataclass
class TypeMapping:
    """Type mapping configuration for OpenAPI schemas"""

    mappings: dict[str, dict[str, Any]] = field(
        default_factory=lambda: {
            "string": {"type": "string"},
            "integer": {"type": "integer"},
            "number": {"type": "number"},
            "boolean": {"type": "boolean"},
            "datetime": {"type": "string", "format": "date-time"},
            "date": {"type": "string", "format": "date"},
            "time": {"type": "string", "format": "time"},
            "text": {"type": "string"},
            "binary": {"type": "string", "format": "binary"},
            "file": {"type": "string", "format": "binary"},
            "geometry": {"type": "string", "description": "Geometry data in WKT format"},
            "money": {"type": "number"},
        }
    )


@dataclass
class ExampleValues:
    """Example values for different data types"""

    values: dict[str, Any] = field(
        default_factory=lambda: {
            "string": "Example string",
            "integer": 42,
            "number": 123.45,
            "boolean": True,
            "datetime": "2025-09-23T11:44:11.753Z",
            "date": "2025-09-23",
            "time": "11:44:11",
            "text": "Example text content",
            "binary": "base64encodeddata==",
            "file": {"_name": "example.pdf", "_content_type": "application/pdf", "_size": 1024},
            "geometry": "POINT (6088198 505579)",
            "money": 99.99,
        }
    )


class OpenAPISchemaRegistry:
    """Central registry for OpenAPI schema definitions and type mappings"""

    def __init__(self):
        self.type_mapping = TypeMapping()
        self.example_values = ExampleValues()
        self.standard_object_properties = {
            "_type": {"type": "string"},
            "_id": {"type": "string", "format": "uuidv4"},
            "_revision": {"type": "string"},
        }


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

    def convert_to_openapi_schema(self, model_property, all_models: dict = None) -> dict[str, Any]:
        """Convert a model property to OpenAPI schema"""
        if all_models is None:
            all_models = {}

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
            items_schema = self.convert_to_openapi_schema(dtype.items, all_models)
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

    def update_operation_references(
        self,
        operations: dict[str, Any],
        model_name: str,
        path_type: str = "collection",
        model_property: tuple | None = None,
    ) -> dict[str, Any]:
        """Update schema references in operations"""
        updated_operations = {}

        for method, operation in operations.items():
            if method.upper() not in SUPPORTED_HTTP_METHODS:
                continue

            updated_operation = copy.deepcopy(operation)
            updated_operation["tags"] = [model_name]
            
            if "operationId" in updated_operation:
                property_name = model_property[0] if model_property else ""
                updated_operation["operationId"] = updated_operation["operationId"] + model_name + property_name
            
            response_200 = updated_operation["responses"]["200"]
    
            if "content" in response_200:
                filtered_content = {
                    media_type: content_schema 
                    for media_type, content_schema in response_200["content"].items()
                    if media_type in SUPPORTED_MEDIA_TYPES
                }
                response_200["content"] = filtered_content 
                self._update_200_response_schema(response_200, model_name, path_type, model_property)

            updated_operations[method] = updated_operation

        return updated_operations

    def _update_200_response_schema(self, response_200: dict, model_name: str, path_type: str, model_property: tuple | None):
        """Update 200 response schema reference in operation"""

        schema = response_200["content"]["application/json"]["schema"]

        if path_type == "collection":
            new_ref = f"#/components/schemas/{model_name}Collection"
        elif path_type == "single":
            new_ref = f"#/components/schemas/{model_name}"
        elif path_type == "changes":
            new_ref = f"#/components/schemas/{model_name}Changes"
        elif path_type == "property" and model_property:
            property_dtype = model_property[1].dtype
            base_type = self.dtype_handler.get_dtype_name(property_dtype)
            new_ref = f"#/components/schemas/{base_type}"
        else:
            new_ref = f"#/components/schemas/{model_name}"

        schema["$ref"] = new_ref


class SchemaGenerator:
    """Handles OpenAPI schema generation for models"""

    def __init__(self, dtype_handler: DataTypeHandler, schema_registry: OpenAPISchemaRegistry):
        self.dtype_handler = dtype_handler
        self.schema_registry = schema_registry

    def create_model_schema(self, model, all_models: dict) -> dict[str, Any]:
        """Create OpenAPI schema for a model"""
        properties = self.schema_registry.standard_object_properties.copy()
        required_fields = []

        for prop_name, model_property in model.get_given_properties().items():
            prop_schema = self.dtype_handler.convert_to_openapi_schema(model_property, all_models)
            properties[prop_name] = prop_schema

            if hasattr(model_property.dtype, "required") and model_property.dtype.required:
                required_fields.append(prop_name)

        schema = {"type": "object", "properties": properties, "example": self.create_example_object(model)}

        if required_fields:
            schema["required"] = required_fields

        return schema

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

        properties = {
            "_cid": {"type": "integer", "example": 11},
            "_id": {"type": "string", "format": "uuidv4", "example": "abdd1245-bbf9-4085-9366-f11c0f737c1d"},
            "_rev": {"type": "string", "format": "uuidv4", "example": "16dabe62-61e9-4549-a6bd-07cecfbc3508"},
            "_txn": {"type": "string", "example": "792a5029-63c9-4c07-995c-cbc063aaac2c"},
            "_created": {"type": "string", "format": "date-time", "example": "2021-07-30T14:03:14.645198"},
            "_op": {"type": "string", "enum": ["insert", "patch", "delete"]},
            "_objectType": {"type": "string", "example": model.basename},
        }

        for prop_name, model_property in model.get_given_properties().items():
            prop_schema = self.dtype_handler.convert_to_openapi_schema(model_property)

            if "required" in prop_schema:
                del prop_schema["required"]
            properties[prop_name] = prop_schema

        return {"type": "object", "properties": properties, "additionalProperties": False}


class TemplateBasedOpenAPIGenerator:
    """Generate OpenAPI specs using a template and manifest data"""

    def __init__(self, template_path: str):
        self.template_path = self._resolve_template_path(template_path)
        self.template = self._load_template()

        self.schema_registry = OpenAPISchemaRegistry()
        self.dtype_handler = DataTypeHandler(self.schema_registry)
        self.path_generator = PathGenerator(self.dtype_handler)
        self.schema_generator = SchemaGenerator(self.dtype_handler, self.schema_registry)

    def _resolve_template_path(self, template_path: str) -> Path:
        """Resolve template path, handling both relative and absolute paths"""

        path = Path(template_path)

        if path.is_absolute():
            return path

        return Path(__file__).parent / path

    def _load_template(self) -> dict[str, Any]:
        """Load the OpenAPI template from JSON file"""
        try:
            with open(self.template_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            message = f"Template file not found: {self.template_path}"
            raise FileNotFoundError(message)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in template file {self.template_path}: {e}")

    def generate_spec(self, manifest) -> dict[str, Any]:
        """Generate complete OpenAPI specification"""
        spec = copy.deepcopy(self.template)

        datasets, models = self._extract_manifest_data(manifest)
        
        self._override_info(spec, datasets)
        self._override_tags(spec, models)
        self._override_schemas_section(spec, models)
        self._override_paths_section(spec, datasets, models)

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
    
    def _override_tags(self, spec: dict[str, Any], models: dict):
        """Override tags with model names"""
        
        spec["tags"] = []
        spec["tags"].append(
            {
                "name": "utility",
                "description": "Utility operations performed on the API itself"
            }
        )
        description = "Operations with"
        
        for model in models.values():
            model_schema_name = model.basename
            spec["tags"].append(
                {
                    "name": model_schema_name,
                    "description": f"{description} {model_schema_name}"
                }
            )
            
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

    def _override_paths_section(self, spec: dict[str, Any], datasets: Any, models: dict):
        """Replace template paths with actual dataset/model paths"""

        original_paths = spec["paths"].copy()
        new_paths = {}

        for dataset_name, _ in datasets:
            dataset_models = self._get_dataset_models(dataset_name, models)

            for model in dataset_models:
                self._replace_model_paths_in_template(original_paths, new_paths, model, dataset_name)

        spec["paths"] = new_paths
        spec["paths"]["/version"] = original_paths["/version"]
        spec["paths"]["/health"] = original_paths["/health"]

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

    def _replace_model_paths_in_template(
        self, original_paths: dict[str, Any], new_paths: dict[str, Any], model, dataset_name: str
    ):
        """Replace template paths for a specific model with actual paths"""
        path_mappings = self.path_generator.create_path_mappings(model, dataset_name)

        for template_path, actual_path, path_type, model_property in path_mappings:
            if template_path in original_paths:
                template_operations = original_paths[template_path].copy()

                updated_operations = self.path_generator.update_operation_references(
                    template_operations, model.basename, path_type, model_property
                )
                new_paths[actual_path] = updated_operations
