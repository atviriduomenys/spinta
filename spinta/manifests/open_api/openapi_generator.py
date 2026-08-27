from __future__ import annotations

import copy
from dataclasses import dataclass, field
from functools import partial
from typing import Any, Callable, Union

from spinta.cli.manifest import _read_and_return_manifest
from spinta.components import Model, Namespace, Property
from spinta.config import CONFIG
from spinta.core.context import configure_context, create_context
from spinta.core.enums import Action, Level
from spinta.dimensions.enum.components import EnumItem
from spinta.exceptions import DataServiceNotFound
from spinta.manifests.components import ManifestPath
from spinta.manifests.open_api.openapi_config import (
    BASE_TAGS,
    COMMON_SCHEMAS,
    EXTERNAL_DOCS,
    HEADER_COMPONENTS,
    INFO,
    PARAMETER_COMPONENTS,
    PATH_TYPE_ACTIONS,
    PATHS_CONFIG,
    PROPERTY_EXAMPLE,
    PROPERTY_MAPPING,
    PROPERTY_TYPES_IN_PATHS,
    RESPONSE_COMPONENTS,
    ROOT_SCOPE_TEMPLATE,
    SCOPE_DESCRIPTION,
    SCOPE_TEMPLATE,
    SECURITY_SCHEMES,
    STANDARD_OBJECT_PROPERTIES,
    VERSION,
)
from spinta.manifests.open_api.service import (
    datasets_under_service,
    find_services,
    relative_path,
    service_schema_name,
)
from spinta.manifests.open_api.udts_config import TOKEN_PATH, UdtsConfig
from spinta.types.datatype import DataType
from spinta.utils.schema import NA
from spinta.utils.scopes import name_to_scope

AUTH_SCHEME = "UAPI_auth"

#: Agent level endpoints, given in action form, because an API gateway exposes
#: each data service under its own context path and routes these separately.
UTILITY_PATHS = ["/:version", "/:token"]

GLOBAL_ID_LEVEL_THRESHOLD = 4

#: Used when the generator is called without a loaded Spinta configuration.
DEFAULT_SCOPE_PREFIX = CONFIG["scope_prefix_udts"]
DEFAULT_SCOPE_MAX_LENGTH = CONFIG["scope_max_length"]

#: Scope a node is authorized against, see `spinta.auth`.
ScopeNameFunc = Callable[[Union[Model, Property, Namespace], Action], str]


def _reference_shape(model_property, dtype) -> tuple:
    level = getattr(model_property, "level", None)
    refprops = getattr(dtype, "refprops", None) or []
    return getattr(level, "value", level), tuple(prop.name for prop in refprops if hasattr(prop, "name"))


def _authorized_nodes(model: Model, path_type: str, model_property: tuple | None) -> list[Model | Property | Namespace]:
    """Nodes a scope of which authorizes the operation.

    Spinta accepts a scope of the node itself or of any namespace above it, see
    `spinta.auth.authorized`, so a token carrying a data service or a namespace
    scope authorizes a model of it. A hidden property takes its own scope only.
    """
    if path_type == "property" and model_property:
        prop = model_property[1]
        if getattr(prop, "hidden", False):
            return [prop]
        nodes = [prop, model]
    else:
        nodes = [model]

    namespace = getattr(model, "ns", None)
    if namespace is not None:
        nodes += [namespace, *namespace.parents()]

    return nodes


def default_scope_name(
    node: Model | Property,
    action: Action,
    prefix: str = DEFAULT_SCOPE_PREFIX,
    maxlen: int = DEFAULT_SCOPE_MAX_LENGTH,
) -> str:
    """Build a scope the way `spinta.auth.get_scope_name` does.

    Used when the generator is called without the configured scope formatter,
    which is where a deployment can replace this.
    """
    if isinstance(node, Namespace):
        name = node.name
    elif isinstance(node, Property):
        name = f"{node.model.model_type()}/@{node.place}"
    else:
        name = node.model_type()

    return name_to_scope(
        SCOPE_TEMPLATE if name else ROOT_SCOPE_TEMPLATE,
        name,
        maxlen=maxlen,
        params={"prefix": prefix, "action": action.value},
        is_udts=True,
    )


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


def _requested_scopes(spec: dict[str, Any], scheme: str) -> set[str]:
    """Collect scopes that operations request from a given security scheme."""
    scopes = set()
    for operations in spec.get("paths", {}).values():
        for method, operation in operations.items():
            if method == "parameters" or not isinstance(operation, dict):
                continue
            for requirement in operation.get("security", []):
                scopes.update(requirement.get(scheme, []))
    return scopes


def _model_dataset_name(model: Model) -> str | None:
    if hasattr(model, "external") and hasattr(model.external, "dataset"):
        return model.external.dataset.name
    return None


def _derived_schema_names(name: str) -> tuple[str, ...]:
    """Names of all schemas a model of a given name gets.

    A model named `X` also takes `XCollection` and `X_Ref`, so a model named
    `XCollection` would silently replace the collection schema of `X`.
    """
    return name, f"{name}Collection", f"{name}_Ref"


class SchemaNamer:
    """Resolves component schema names for models.

    Models included in the generated specification are named by the given
    naming function, all other models, referenced from the included ones, keep
    their full underscored name, which is unique across the whole manifest.
    """

    def __init__(
        self,
        models: dict[str, Model],
        all_models: dict[str, Model] | None = None,
        name_included: Callable[[Model], str] = _get_schema_name,
        reserved: set[str] | None = None,
    ):
        self._included = {model.name for model in models.values()}
        self._names: dict[str, str] = {}
        self._ref_shapes: dict[str, dict[Any, str]] = {}
        self._taken = set(reserved or ())

        for model in sorted(models.values(), key=lambda model: model.name):
            self._assign(model, name_included(model))

        # Models referenced from the included ones get a schema of their own.
        for model in sorted((all_models or {}).values(), key=lambda model: model.name):
            if model.name not in self._names:
                self._assign(model, _get_schema_name(model))

    def _assign(self, model: Model, base: str) -> None:
        # Path separators become underscores, so different data set paths can
        # produce one name, `a_b` and `a/b` for example, and one schema would
        # then silently replace the other.
        name = base
        number = 1
        while any(derived in self._taken for derived in _derived_schema_names(name)):
            number += 1
            name = f"{base}_{number}"
        self._taken.update(_derived_schema_names(name))
        self._names[model.name] = name

    def is_included(self, model: Model) -> bool:
        return model.name in self._included

    def name(self, model: Model) -> str:
        return self._names.get(model.name) or _get_schema_name(model)

    def ref_name(self, model: Model, shape: Any) -> str:
        """Name of a partial schema of a model referenced from an included one.

        Such a schema holds what the reference carries, which depends on its
        level and reference properties, see `_build_ref_model_schema`. One model
        can be referenced in more than one shape, and each shape is a schema of
        its own, otherwise the first one would answer for all of them.
        """
        shapes = self._ref_shapes.setdefault(model.name, {})
        if shape not in shapes:
            # A model of the specification keeps its full schema under its own
            # name, so its reference schemas are named apart.
            base = f"{self.name(model)}_Ref" if self.is_included(model) else self.name(model)
            name = base
            number = 1
            while shapes and any(derived in self._taken for derived in _derived_schema_names(name)):
                number += 1
                name = f"{base}_{number}"
            self._taken.update(_derived_schema_names(name))
            shapes[shape] = name
        return shapes[shape]


def _nullable(schema: dict[str, Any]) -> dict[str, Any]:
    """Allow `null` in a property schema.

    Spinta returns `null` for every property that has no value, so anything not
    listed in `required` has to accept it, otherwise response validation fails.
    """
    if "$ref" in schema:
        ref = {key: value for key, value in schema.items() if key != "example"}
        nullable = {"anyOf": [ref, {"type": "null"}]}
        if "example" in schema:
            nullable["example"] = schema["example"]
        return nullable

    schema = copy.deepcopy(schema)
    dtype = schema.get("type")
    if isinstance(dtype, str):
        schema["type"] = [dtype, "null"]
    elif isinstance(dtype, list) and "null" not in dtype:
        schema["type"] = [*dtype, "null"]

    # `type` and `enum` are validated together, so a value has to be listed in
    # both for `null` to be accepted.
    enum = schema.get("enum")
    if isinstance(enum, list) and None not in enum:
        schema["enum"] = [*enum, None]

    return schema


class OpenAPISchemaRegistry:
    """Central registry for OpenAPI schema definitions and type mappings"""

    def __init__(self):
        self.type_mapping = TypeMapping()
        self.example_values = ExampleValues()
        self.standard_object_properties = STANDARD_OBJECT_PROPERTIES


class DataTypeHandler:
    """Handles data type conversions and schema generation"""

    def __init__(self, schema_registry: OpenAPISchemaRegistry, namer: SchemaNamer):
        self.schema_registry = schema_registry
        self.namer = namer

    def _ref_schema_name(self, model_property, dtype) -> str:
        return self.namer.ref_name(dtype.model, _reference_shape(model_property, dtype))

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

    def convert_to_openapi_schema(
        self,
        model_property,
        schemas: dict | None = None,
    ) -> dict[str, Any]:
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
            ref_schema_name = self._ref_schema_name(model_property, dtype)
            example = {"_type": dtype.model.basename, "_id": EXAMPLE_UUID_REF_ID}
            if schemas and (ref_schema := schemas.get(ref_schema_name)) and "example" in ref_schema:
                example = copy.deepcopy(ref_schema["example"])
            return {"$ref": f"#/components/schemas/{ref_schema_name}", "example": example}

        if self.is_array_type(dtype):
            items_schema = self.convert_to_openapi_schema(dtype.items, schemas=schemas)
            example_item = items_schema.get("example", "example_item")
            return {"type": "array", "items": items_schema, "example": [example_item]}

        dtype_name = self.get_dtype_name(dtype)
        return copy.deepcopy(
            self.schema_registry.type_mapping.mappings.get(dtype_name, {"type": "string", "example": "Example value"})
        )

    def get_example_value(
        self,
        model_property,
        schemas: dict | None = None,
    ) -> Any:
        """Generate example values for properties. When schemas is provided, use ref schema example for reference types."""
        dtype = model_property.dtype

        if self.is_enum_property(model_property):
            enum_values = self.get_enum_values(model_property)
            return enum_values[0] if enum_values else "UNKNOWN"

        if self.is_reference_type(dtype):
            ref_schema_name = self._ref_schema_name(model_property, dtype)
            if schemas and (ref_schema := schemas.get(ref_schema_name)) and "example" in ref_schema:
                return copy.deepcopy(ref_schema["example"])
            return {"_type": dtype.model.basename, "_id": EXAMPLE_UUID_REF_ID}

        if self.is_array_type(dtype):
            return [self.get_example_value(dtype.items, schemas=schemas)]

        dtype_name = self.get_dtype_name(dtype)
        return copy.deepcopy(self.schema_registry.example_values.values.get(dtype_name, "Example value"))


class PathGenerator:
    """Handles OpenAPI path generation and operations"""

    def __init__(self, dtype_handler: DataTypeHandler, namer: SchemaNamer, scope_name: ScopeNameFunc):
        self.dtype_handler = dtype_handler
        self.namer = namer
        self.scope_name = scope_name
        self.operation_ids: set[str] = set()

    def should_create_property_endpoint(self, model_property) -> bool:
        """Determine if a property should have its own endpoint"""
        dtype_name = self.dtype_handler.get_dtype_name(model_property.dtype)
        return dtype_name in PROPERTY_TYPES_IN_PATHS

    def create_path_mappings(self, model: Model, path_prefix: str) -> list[tuple[str, str, str, tuple | None]]:
        """Create path mappings for a model"""
        model_path = "/".join(part for part in (path_prefix, model.basename) if part)

        path_mappings = [
            ("/{model_name}", f"/{model_path}", "collection", None),
            ("/{model_name}/{id}", f"/{model_path}/{{id}}", "single", None),
        ]

        for prop_name, model_property in model.get_given_properties().items():
            if self.should_create_property_endpoint(model_property):
                template_path = "/{model_name}/{id}/{field}"
                actual_path = f"/{model_path}/{{id}}/{prop_name}"
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
        self, path_config: dict, model: Model | None = None, path_type: str = None, model_property: tuple | None = None
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
        self,
        method_config: dict,
        model: Model | None = None,
        path_type: str = None,
        model_property: tuple | None = None,
    ) -> dict[str, Any]:
        """Build a single operation (get, head, etc.)"""
        operation = {}

        if model:
            operation["tags"] = [self.namer.name(model)]
        elif "tags" in method_config:
            operation["tags"] = method_config["tags"]

        if "security" in method_config:
            operation["security"] = self._build_security(method_config["security"], model, path_type, model_property)

        if "requestBody" in method_config:
            operation["requestBody"] = copy.deepcopy(method_config["requestBody"])

        for spec_field in ["summary", "description"]:
            if spec_field in method_config:
                operation[spec_field] = method_config[spec_field]

        if "operationId" in method_config:
            model_name = self.namer.name(model) if model else None
            operation["operationId"] = self._build_operation_id(
                method_config["operationId"], model_name=model_name, model_property=model_property
            )

        if "parameters" in method_config:
            operation["parameters"] = self._build_parameter_refs(method_config["parameters"])

        operation["responses"] = self._build_responses(
            method_config.get("responses", {}), model, path_type, model_property
        )

        return operation

    def _build_security(
        self,
        security: list[dict],
        model: Model | None,
        path_type: str | None,
        model_property: tuple | None,
    ) -> list[dict]:
        """Fill in the scopes a model operation authorizes against.

        Each accepted action is given as a separate security requirement,
        because a token carrying any one of them is enough.
        """
        if model is None or path_type not in PATH_TYPE_ACTIONS:
            return copy.deepcopy(security)

        requirements = []
        for requirement in security:
            if AUTH_SCHEME not in requirement:
                requirements.append(copy.deepcopy(requirement))
                continue
            requirements.extend(
                {AUTH_SCHEME: [scope]} for scope in self._model_scopes(model, path_type, model_property)
            )
        return requirements

    def _model_scopes(self, model: Model, path_type: str, model_property: tuple | None) -> list[str]:
        nodes = _authorized_nodes(model, path_type, model_property)
        return [self.scope_name(node, action) for action in PATH_TYPE_ACTIONS[path_type] for node in nodes]

    def _build_operation_id(self, base_id: str, model_name: str = None, model_property: tuple | None = None) -> str:
        """Build operation ID with optional model and property names.

        Names are concatenated, which is not injective, model `A` with property
        `bc` and model `Ab` with property `c` build one id, so a colliding one
        gets a number suffix.
        """
        property_name = model_property[0] if model_property else ""
        model_suffix = model_name or ""

        base = f"{base_id}{model_suffix}{property_name}"
        operation_id = base
        number = 1
        while operation_id in self.operation_ids:
            number += 1
            operation_id = f"{base}_{number}"

        self.operation_ids.add(operation_id)
        return operation_id

    def _build_parameter_refs(self, parameters: list) -> list[dict]:
        """Build parameter reference objects"""
        return [{"$ref": f"#/components/parameters/{param}"} for param in parameters]

    def _build_responses(
        self,
        responses_config: dict,
        model: Model | None = None,
        path_type: str = None,
        model_property: tuple | None = None,
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
        model: Model | None = None,
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

        if "content" in response_config:
            response["content"] = self._build_response_content(
                response_config["content"], model, path_type, model_property
            )

        return response

    def _build_response_content(
        self,
        content_config: dict,
        model: Model | None = None,
        path_type: str = None,
        model_property: tuple | None = None,
    ) -> dict[str, Any]:
        """Build response content with schema references"""
        content = {}

        for media_type, media_config in content_config.items():
            schema = media_config.get("schema")
            if isinstance(schema, dict):
                content[media_type] = {"schema": copy.deepcopy(schema)}
            else:
                content[media_type] = {"schema": {"$ref": self._resolve_schema_ref(schema, model, path_type)}}

        return content

    def _resolve_schema_ref(
        self,
        schema_name: str = None,
        model: Model | None = None,
        path_type: str = None,
    ) -> str:
        """Resolve the appropriate schema reference"""
        model_schema_name = self.namer.name(model) if model else None
        if model_schema_name and path_type:
            # A collection is wrapped into a `_data` envelope.
            suffix = "Collection" if path_type == "collection" else ""
            return f"#/components/schemas/{model_schema_name}{suffix}"

        if schema_name:
            return f"#/components/schemas/{schema_name}"

        return f"#/components/schemas/{model_schema_name or 'object'}"


class ComponentSchemaBuilder:
    """Creates OpenAPI component schemas (parameters, responses, headers) from path configs."""

    def _ensure_components_path(self, spec: dict, *path: str):
        current = spec
        for key in path:
            current = current.setdefault(key, {})
        return current

    def create_components_for_path(self, spec: dict, path_config: dict) -> None:
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

        if isinstance(schema_config, dict):
            if "oneOf" in schema_config:
                return {"oneOf": [self._build_schema_ref(schema) for schema in schema_config["oneOf"]]}

            if "anyOf" in schema_config:
                return {"anyOf": [self._build_schema_ref(schema) for schema in schema_config["anyOf"]]}

            # Spinta answers with an envelope, see `spinta.api.error_response`.
            if "errors" in schema_config:
                errors = [self._build_schema_ref(schema) for schema in schema_config["errors"]]
                # Error schemas are not discriminated, every one of them accepts
                # any error object, and Spinta answers with error codes beyond
                # the ones named here, so the alternatives are not exclusive.
                items = errors[0] if len(errors) == 1 else {"anyOf": errors}
                return {
                    "type": "object",
                    "required": ["errors"],
                    "properties": {"errors": {"type": "array", "items": items}},
                }

        return schema_config


class SchemaGenerator:
    """Handles OpenAPI schema generation for models."""

    def __init__(self, dtype_handler: DataTypeHandler, schema_registry: OpenAPISchemaRegistry, namer: SchemaNamer):
        self.dtype_handler = dtype_handler
        self.schema_registry = schema_registry
        self.namer = namer

    def create_all_model_schemas(self, models: dict) -> dict[str, Any]:
        """Build all model schemas (main + collection + refs) in one pass.

        Returns the complete schemas dict for components/schemas.

        Ref schemas are built before main schemas so that main model examples
        can include proper ref model examples instead of generic placeholders.
        """
        schemas = {}

        for model in models.values():
            self._create_referenced_model_schemas(schemas, model)

        for model in models.values():
            schema_name = self.namer.name(model)
            schemas[schema_name] = self._create_model_schema(model, schemas)
            schemas[f"{schema_name}Collection"] = self._create_collection_schema(model, schema_name)

        return schemas

    def _create_model_schema(
        self,
        model,
        schemas: dict | None = None,
    ) -> dict[str, Any]:
        properties = copy.deepcopy(self.schema_registry.standard_object_properties)

        for prop_name, model_property in model.get_given_properties().items():
            prop_schema = self.dtype_handler.convert_to_openapi_schema(model_property, schemas=schemas)

            # Required in a manifest means the data holds a value, not that a
            # response carries the property: a request selects what it wants,
            # see `spinta.backends.helpers.get_select_prop_names`, and a hidden
            # property is left out of an ordinary response altogether. So
            # nothing is listed as required, while a property that always holds
            # a value is not made nullable.
            if not getattr(model_property.dtype, "required", False):
                prop_schema = _nullable(prop_schema)

            properties[prop_name] = prop_schema

        return {
            "type": "object",
            "properties": properties,
            "example": self._create_example(model, schemas=schemas),
        }

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

    def _create_collection_schema(self, model, schema_name: str) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "_type": {"type": "string"},
                "_data": {"type": "array", "items": {"$ref": f"#/components/schemas/{schema_name}"}},
            },
        }

    def _create_referenced_model_schemas(self, schemas: dict, model: Model) -> None:
        for model_property in model.get_given_properties().values():
            # An array holds its reference in the item property, which carries a
            # level of its own, and the schema of a reference is built from it.
            # `convert_to_openapi_schema` reads the item property as well, so
            # naming the schema from the array would name one it never builds.
            if self.dtype_handler.is_array_type(model_property.dtype) and hasattr(model_property.dtype, "items"):
                model_property = model_property.dtype.items

            dtype = model_property.dtype

            if not self.dtype_handler.is_reference_type(dtype):
                continue

            ref_model = dtype.model
            ref_schema_name = self.namer.ref_name(ref_model, _reference_shape(model_property, dtype))
            if ref_schema_name in schemas:
                continue

            ref_level = getattr(model_property, "level", None)
            refprops = getattr(dtype, "refprops", None) or []

            # Placed before building, because a model can be reached from itself.
            schemas[ref_schema_name] = {}
            schemas[ref_schema_name] = self._build_ref_model_schema(schemas, ref_model, refprops, ref_level)

    def _resolve_nested_ref_schema_name(
        self,
        schemas: dict,
        nested_ref_model: Model,
        nested_refprops: list,
        ref_level: Level | None = None,
    ) -> str:
        shape = (
            getattr(ref_level, "value", ref_level),
            tuple(prop.name for prop in nested_refprops if hasattr(prop, "name")),
        )
        schema_name = self.namer.ref_name(nested_ref_model, shape)

        if schema_name not in schemas:
            # Placed before building, because a model can be reached from itself.
            schemas[schema_name] = {}
            schemas[schema_name] = self._build_ref_model_schema(
                schemas,
                nested_ref_model,
                nested_refprops,
                ref_level,
            )

        return schema_name

    def _build_ref_model_schema(
        self,
        schemas: dict,
        model,
        refprops: list,
        ref_level: Level | int | None = None,
    ) -> dict[str, Any]:
        level_value: int | None = None
        if ref_level is not None:
            level_value = ref_level.value if isinstance(ref_level, Level) else int(ref_level)

        is_global_ref = level_value is not None and level_value >= GLOBAL_ID_LEVEL_THRESHOLD

        if is_global_ref:
            properties = copy.deepcopy(self.schema_registry.standard_object_properties)
            example = {
                "_type": model.basename,
                "_id": EXAMPLE_UUID_OBJECT_ID,
                "_revision": EXAMPLE_UUID_REVISION,
            }
            return {"type": "object", "properties": properties, "example": example}

        properties = copy.deepcopy(self.schema_registry.standard_object_properties)

        refprop_names = {prop.name for prop in refprops if hasattr(prop, "name")}

        for prop_name, model_property in model.get_given_properties().items():
            if refprop_names and prop_name not in refprop_names:
                continue

            dtype = model_property.dtype

            # A reference carries what its own level says, also when it is
            # reached through another reference, so the level of the one being
            # built does not apply to it. An array holds it in the item.
            inner_property = model_property
            if self.dtype_handler.is_array_type(dtype) and hasattr(dtype, "items"):
                inner_property = dtype.items
            inner_dtype = inner_property.dtype

            if self.dtype_handler.is_reference_type(inner_dtype):
                nested_refprops = getattr(inner_dtype, "refprops", None) or []
                schema_name = self._resolve_nested_ref_schema_name(
                    schemas,
                    inner_dtype.model,
                    nested_refprops,
                    getattr(inner_property, "level", None),
                )
                ref_schema = schemas.get(schema_name)
                example = copy.deepcopy(ref_schema.get("example")) if ref_schema else None
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
                prop_schema = self.dtype_handler.convert_to_openapi_schema(model_property, schemas=schemas)

            # A response carries what it was asked for, see
            # `_create_model_schema`, so nothing is listed as required here.
            if not getattr(model_property.dtype, "required", False):
                prop_schema = _nullable(prop_schema)

            properties[prop_name] = prop_schema

        example = {
            "_type": model.basename,
            "_id": EXAMPLE_UUID_OBJECT_ID,
            "_revision": EXAMPLE_UUID_REVISION,
        }
        for prop_name, model_property in model.get_given_properties().items():
            if refprop_names and prop_name not in refprop_names:
                continue
            example[prop_name] = self.dtype_handler.get_example_value(model_property, schemas=schemas)

        if level_value is not None and level_value < GLOBAL_ID_LEVEL_THRESHOLD:
            properties.pop("_id", None)
            example.pop("_id", None)

        return {"type": "object", "properties": properties, "example": example}


class OpenAPIGenerator:
    """Generate OpenAPI specs using manifest data"""

    def __init__(
        self,
        main_dataset_name: str | None = None,
        api_version: str | None = None,
        service_path: str | None = None,
        config: UdtsConfig | None = None,
        scope_name: ScopeNameFunc | None = None,
        scope_prefix: str = DEFAULT_SCOPE_PREFIX,
        scope_max_length: int = DEFAULT_SCOPE_MAX_LENGTH,
    ):
        if main_dataset_name is not None and service_path is not None:
            # One covers a data service with all of its data sets, the other a
            # single data set, so silently taking one of them would export
            # something the caller did not ask for.
            raise ValueError("Give either `main_dataset_name` or `service_path`, not both.")

        self.main_dataset_name = main_dataset_name
        self.api_version = api_version if api_version is not None else ""
        self.service_path = service_path
        self.config = config if config is not None else UdtsConfig()
        self.scope_name = scope_name or partial(
            default_scope_name,
            prefix=scope_prefix,
            maxlen=scope_max_length,
        )

        self.component_builder = ComponentSchemaBuilder()

    def generate_spec(self, manifest) -> dict[str, Any]:
        """Generate complete OpenAPI specification."""
        specification = {
            "openapi": VERSION,
            "info": copy.deepcopy(INFO),
            "externalDocs": copy.deepcopy(EXTERNAL_DOCS),
            "tags": copy.deepcopy(BASE_TAGS),
            "components": {},
        }
        specification["info"]["version"] = self.api_version

        datasets, all_models = self._extract_manifest_data(manifest)
        models = all_models

        # Common schemas are added to the same dict, and the base tags to the
        # same list, so a model must not take a name of either; OpenAPI wants
        # unique tag names too. A model name starts with an upper case letter,
        # see `spinta.types.model.load`, so it can not take a base tag name as
        # they are written today, but the names are allocated once and this
        # keeps that true.
        reserved = set(COMMON_SCHEMAS) | {tag["name"] for tag in BASE_TAGS}

        if self.service_path is not None:
            datasets, models = self._filter_by_service_path(datasets, models)

            def name_included(model: Model) -> str:
                return service_schema_name(model, self.service_path)
        elif self.main_dataset_name is not None:
            datasets, models = self._filter_by_main_dataset(datasets, models)

            def name_included(model: Model) -> str:
                return model.basename
        else:
            name_included = _get_schema_name

        namer = SchemaNamer(models, all_models, name_included, reserved)

        self.schema_registry = OpenAPISchemaRegistry()
        self.dtype_handler = DataTypeHandler(self.schema_registry, namer)
        self.schema_generator = SchemaGenerator(self.dtype_handler, self.schema_registry, namer)
        self.path_generator = PathGenerator(self.dtype_handler, namer, self.scope_name)
        self.namer = namer

        self._set_servers(specification)
        self._override_info(specification, datasets)
        self._set_tags(specification, models)

        model_schemas = self.schema_generator.create_all_model_schemas(models)
        specification.setdefault("components", {}).setdefault("schemas", {}).update(model_schemas)

        self._create_paths(specification, datasets, models)

        self._create_component_schemas(specification)
        self._add_common_schemas(specification)
        self._add_security_schemes(specification)

        return specification

    def _extract_manifest_data(self, manifest) -> tuple[Any, dict]:
        if isinstance(manifest, ManifestPath):
            context = create_context()
            manifests = [manifest]
            context = configure_context(context, manifests)
            manifest = _read_and_return_manifest(context, manifests, check_config=False, ensure_backends=False)

        datasets = manifest.get_objects()["dataset"].items()
        models = manifest.get_objects()["model"]

        return datasets, models

    def _filter_by_service_path(self, datasets: Any, models: dict) -> tuple[list, dict]:
        """Select all data sets of one UDTS data service."""
        datasets_list = list(datasets)
        dataset_names = [name for name, _ in datasets_list]
        names = set(datasets_under_service(dataset_names, self.service_path))

        if not names:
            raise DataServiceNotFound(
                service=self.service_path,
                available=", ".join(find_services(dataset_names)) or "none",
            )

        filtered_datasets = [(name, dataset) for name, dataset in datasets_list if name in names]
        filtered_models = {key: model for key, model in models.items() if _model_dataset_name(model) in names}
        return filtered_datasets, filtered_models

    def _filter_by_main_dataset(self, datasets: Any, models: dict) -> tuple[list, dict]:
        datasets_list = list(datasets)
        filtered_datasets = [(name, dataset) for name, dataset in datasets_list if name == self.main_dataset_name]
        if not filtered_datasets:
            raise ValueError(
                f"Dataset {self.main_dataset_name!r} not found in manifest. "
                f"Available: {[name for name, _ in datasets_list]}"
            )
        filtered_models = {
            key: model for key, model in models.items() if _model_dataset_name(model) == self.main_dataset_name
        }
        return filtered_datasets, filtered_models

    def _set_servers(self, spec: dict[str, Any]) -> None:
        """One entry per environment, each ending with the data service path.

        An API gateway derives the API context path from the path part of the
        first server URL, and model paths are relative to it.
        """
        if self.service_path is None:
            return
        spec["servers"] = self.config.resolve_servers(self.service_path)
        if self.config.external_docs:
            spec["externalDocs"] = copy.deepcopy(self.config.external_docs)

    def _override_info(self, spec: dict[str, Any], datasets: dict):
        if self.service_path is not None:
            # A data service holds many data sets, so its description can not be
            # taken from any single one of them.
            spec["info"].update(copy.deepcopy(self.config.info))
            if self.api_version:
                spec["info"]["version"] = self.api_version
            return

        _, dataset = next(iter(datasets))
        spec["info"]["summary"] = dataset.title
        spec["info"]["description"] = dataset.description

    def _add_security_schemes(self, spec: dict[str, Any]) -> None:
        schemes = copy.deepcopy(SECURITY_SCHEMES)
        flow = schemes[AUTH_SCHEME]["flows"]["clientCredentials"]
        flow["tokenUrl"] = self.config.resolve_token_url(spec.get("servers", []))
        scopes = sorted(_requested_scopes(spec, AUTH_SCHEME))
        flow["scopes"] = {scope: SCOPE_DESCRIPTION for scope in scopes}
        spec.setdefault("components", {})["securitySchemes"] = schemes

        self._set_scope_example(spec)

    def _set_scope_example(self, spec: dict[str, Any]) -> None:
        """Show a scope of one model of this data service in the token request.

        Namespaces above the model are requested as alternatives too, and the
        root namespace of the agent is among them, so a scope is taken from a
        data path, where the narrowest one comes first, and not from the
        declared ones, where sorting would put the widest first.
        """
        token_path = spec.get("paths", {}).get(TOKEN_PATH)
        if not token_path:
            return

        scope = next(
            (
                requirement[AUTH_SCHEME][0]
                for path, operations in spec.get("paths", {}).items()
                if path not in UTILITY_PATHS
                for method, operation in operations.items()
                if method != "parameters" and isinstance(operation, dict)
                for requirement in operation.get("security", [])
                if requirement.get(AUTH_SCHEME)
            ),
            None,
        )
        if scope is None:
            return

        content = token_path["post"]["requestBody"]["content"]["application/x-www-form-urlencoded"]
        content["schema"]["properties"]["scope"]["examples"] = [scope]

    def _set_tags(self, spec: dict[str, Any], models: dict):
        description = "Operations with"
        for model in models.values():
            model_schema_name = self.namer.name(model)
            spec["tags"].append({"name": model_schema_name, "description": f"{description} {model_schema_name}"})

    def _create_paths(self, spec: dict[str, Any], datasets: Any, models: dict):
        paths = {}

        for path in UTILITY_PATHS:
            path_config = PATHS_CONFIG.get(path)
            if not path_config:
                raise ValueError(f"No config found for path: {path}")
            paths[path] = self.path_generator.create_path(path_config)

        for dataset_name, _ in datasets:
            # Model paths are relative to the data service base, which is given
            # in `servers`.
            path_prefix = relative_path(dataset_name, self.service_path) if self.service_path else dataset_name
            for model in self._get_dataset_models(dataset_name, models):
                for path_key, actual_path, path_type, model_property in self.path_generator.create_path_mappings(
                    model,
                    path_prefix,
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
                schemas[schema_name] = copy.deepcopy(schema_config)

    def _get_dataset_models(self, dataset_name: str, models: dict) -> list:
        return [model for model in models.values() if _model_dataset_name(model) == dataset_name]
