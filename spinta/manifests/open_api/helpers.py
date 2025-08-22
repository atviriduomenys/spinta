from __future__ import annotations

import json
import re
from collections.abc import Generator
from pathlib import Path
from typing import Any
import warnings

from spinta.core.ufuncs import Expr
from spinta.utils.naming import Deduplicator, to_code_name, to_dataset_name, to_model_name, to_property_name

from spinta.exceptions import NotImplementedFeature

SUPPORTED_PARAMETER_LOCATIONS = {"query", "header", "path"}
DEFAULT_DATASET_NAME = "default"
SCHEMA_REF_KEY = "$ref"
DEFAULT_PROPERTY_DATATYPE = "string"
OPENAPI = "openapi"
SWAGGER = "swagger"


def replace_url_parameters(endpoint: str) -> str:
    """Replaces parameters in given endpoint to their codenames.

    e.g. /api/cities/{cityId}/ -> /api/cities/{city_id}
    """
    return re.sub(r"{([^{}]+)}", lambda match: f"{{{to_code_name(match.group(1))}}}", endpoint)


def read_file_data_and_transform_to_json(path: Path) -> dict:
    with Path(path).open() as file:
        return json.load(file)


def get_namespace_schema(info: dict, title: str, dataset_prefix: str) -> Generator[tuple[None, dict], None, None]:
    yield (
        None,
        {
            "type": "ns",
            "name": dataset_prefix,
            "title": info.get("summary", title),
            "description": info.get("description", ""),
        },
    )


def get_resource_parameters(parameters: list[dict]) -> dict[str, dict]:
    resource_parameters = {}
    for index, value in enumerate(parameters):
        name = value["name"]
        location = value["in"] if value["in"] in SUPPORTED_PARAMETER_LOCATIONS else ""
        resource_parameters[f"parameter_{index}"] = {
            "name": to_code_name(name),
            "source": [name],
            "prepare": [Expr(location)],
            "type": "param",
            "description": value.get("description", ""),
        }

    return resource_parameters


model_deduplicator = Deduplicator()


class Model:
    def __init__(
        self,
        dataset: str,
        resource: str,
        basename: str,
        json_schema: dict,
        source: str | None = None,
        parent: Model | None = None,
    ) -> None:
        self.dataset: str = dataset
        self.resource: str = resource
        self.basename: str = basename
        self.json_schema: dict = json_schema
        self.source: str = basename if source is None else source
        self.title: str = self.json_schema.get("title", "")
        self.description: str = self.json_schema.get("description", "")
        self.name: str = model_deduplicator(f"{self.dataset}/{to_model_name(self.basename)}")
        self.children: list[Model] = []
        self.properties: list[Property] = []
        self.parent = parent
        self.add_properties()

    def __repr__(self) -> str:
        return f"<Model {self.name}>"

    def add_properties(self) -> None:
        for property_name, json_schema in self.json_schema.get("properties", {}).items():
            prop = self.add_property(property_name, json_schema)

            if json_schema.get("type") == "object":
                self._add_ref_model(prop)

            elif json_schema.get("type") == "array":
                json_schema = json_schema.get("items", {})
                if json_schema.get("type") == "object":
                    self._add_backref_model(prop)
                else:
                    prop = self.add_property(property_name, json_schema, source="")
                    prop.name += "[]"

    def add_property(self, name: str, json_schema: dict, **kwargs: Any) -> Property:
        prop = Property(name, json_schema, **kwargs)
        self.properties.append(prop)
        return prop

    def add_child(self, basename: str, json_schema: dict) -> Model:
        """
        TODO: Children model "source" should be empty and name should have "/:part" suffix. e.g. f"{self.name}/:part"
        Link to task https://github.com/atviriduomenys/spinta/issues/997
        """
        model = Model(self.dataset, self.resource, basename, json_schema, source="", parent=self)
        self.children.append(model)
        return model

    def _add_ref_model(self, parent_prop: Property) -> None:
        parent_prop.ref = self.add_child(parent_prop.basename, parent_prop.json_schema)

    def _add_backref_model(self, parent_prop: Property) -> None:
        json_schema = parent_prop.json_schema.get("items", {})
        child_model = self.add_child(parent_prop.basename, json_schema)

        backref_prop = self.add_property(parent_prop.basename, {}, datatype="backref", source="", ref_model=child_model)
        backref_prop.name += "[]"
        """
        TODO: Temporary workaround — adds a ref to the backref's model. 
        This should be removed once proper backref handling is implemented.

        See task: https://github.com/atviriduomenys/spinta/issues/1314
        """
        child_model.add_property(self.basename, {}, datatype="ref", source="", ref_model=self)

    def get_node_schema_dicts(self) -> list[dict]:
        schema_dict = [
            {
                "type": "model",
                "name": self.name,
                "title": self.title,
                "description": self.description,
                "external": {
                    "dataset": self.dataset,
                    "resource": self.resource,
                    "name": self.source,
                },
                "properties": {prop.name: prop.get_node_schema_dict() for prop in self.properties},
            }
        ]
        for child in self.children:
            schema_dict.extend(child.get_node_schema_dicts())
        return schema_dict


class Property:
    def __init__(
        self,
        basename: str,
        json_schema: dict,
        datatype: str | None = None,
        source: str | None = None,
        ref_model: Model | None = None,
    ):
        self.basename: str = basename
        self.json_schema: dict = json_schema
        self.name: str = to_property_name(basename)
        self.title: str = json_schema.get("title", "")
        self.description: str = json_schema.get("description", "")
        self.source: str = basename if source is None else source
        self.datatype: str = datatype or self.get_datatype()
        self.ref: Model = ref_model
        self.enum: dict = self.get_enums(self.json_schema.get("enum", []))
        self.required: bool = not bool(json_schema.get("nullable", False))

    def get_datatype(self) -> str:
        """Returns "Property" data type as a string. If type cannot be detected, defaults to "string"."""

        data_type = self.json_schema.get("type", DEFAULT_PROPERTY_DATATYPE)

        basic_types = {
            "boolean": "boolean",
            "integer": "integer",
            "number": "number",
            "array": "array",
            "object": "ref",
        }

        date_time_types = {"date-time": "datetime", "date": "date", "time": "time"}

        if data_type in basic_types:
            return basic_types[data_type]

        if data_type == "string":
            if self.json_schema.get("contentEncoding") == "base64":
                return "binary"
            if (string_format := self.json_schema.get("format")) in date_time_types:
                return date_time_types[string_format]

        return DEFAULT_PROPERTY_DATATYPE

    def get_enums(self, items: list) -> dict:
        enum = {}
        for item in items:
            if isinstance(item, (list, dict)):  # Only handling primitive type enums
                return {}
            enum[item] = {"source": item}
        return enum

    def get_node_schema_dict(self) -> dict:
        schema = {
            "type": self.datatype,
            "title": self.title,
            "description": self.description,
            "external": {"name": self.source},
            "required": self.required,
        }

        if self.datatype in ["ref", "backref"]:
            schema["model"] = self.ref.name
            schema["external"]["prepare"] = Expr("expand")

        if self.enum:
            schema["enums"] = {"": self.enum}

        return schema


def get_nested_value(search_key: str, data: dict) -> Any:
    if not isinstance(data, dict):
        return None
    for key, value in data.items():
        if key == search_key:
            return value
        result = get_nested_value(search_key, value)
        if result is not None:
            return result
    return None


def get_schema_from_response(response: dict, root: dict) -> dict:
    json_schema = {}
    if OPENAPI in root:
        for content_type, content in response.get("content", {}).items():
            if content_type == "application/json" or content_type.endswith("+json"):
                json_schema = content.get("schema", {})
                break
    elif SWAGGER in root:
        json_schema = response.get("schema", {})
    else:
        json_schema = get_nested_value("schema", response) or {}

    if get_nested_value(SCHEMA_REF_KEY, json_schema):
        raise NotImplementedFeature(feature="Reading OpenAPI with '$ref' structure")
    return json_schema


def get_model_schemas(dataset_name: str, resource_name: str, response: dict, root: dict) -> list[dict]:
    if not (json_schema := get_schema_from_response(response, root)):
        return []

    if json_schema.get("type") == "array":
        json_schema = json_schema.get("items", {})
        root_source = ".[]"
    else:
        root_source = "."

    if json_schema.get("type") == "object":
        basename = json_schema.get("title", response["description"]) or f"{dataset_name} {resource_name}"
        model_schema = json_schema
        model = Model(dataset_name, resource_name, basename, model_schema, source=root_source)

        return model.get_node_schema_dicts()

    return []


def get_dataset_schemas(data: dict, dataset_prefix: str) -> Generator[tuple[None, dict]]:  # noqa: C901
    datasets = {}
    models = []
    tag_metadata = {tag["name"]: tag.get("description", "") for tag in data.get("tags", {})}

    for api_endpoint, api_metadata in data.get("paths", {}).items():
        for http_method, http_method_metadata in api_metadata.items():
            tags = http_method_metadata.get("tags", [])
            dataset_name = to_dataset_name("_".join(tags)) or DEFAULT_DATASET_NAME  # Default dataset if no tags given.
            if dataset_name not in datasets:
                datasets[dataset_name] = {
                    "type": "dataset",
                    "name": f"{dataset_prefix}/{dataset_name}",
                    "title": ", ".join(tags),
                    "description": ", ".join(tag_metadata[tag] for tag in tags if tag in tag_metadata),
                    "resources": {},
                }

            resource_name = to_code_name(f"{api_endpoint}/{http_method}")
            resource_parameters = get_resource_parameters(http_method_metadata.get("parameters", {}))

            datasets[dataset_name]["resources"][resource_name] = {
                "type": "dask/json",
                "id": http_method_metadata.get("operationId", ""),
                "external": replace_url_parameters(api_endpoint),
                "prepare": Expr("http", method=http_method.upper(), body="form"),
                "title": http_method_metadata.get("summary", ""),
                "params": resource_parameters,
                "description": http_method_metadata.get("description", ""),
            }
            if response_200 := http_method_metadata.get("responses", {}).get("200", {}):
                models += get_model_schemas(f"{dataset_prefix}/{dataset_name}", resource_name, response_200, data)

    if not datasets:
        dataset_name = DEFAULT_DATASET_NAME
        datasets[dataset_name] = {
            "type": "dataset",
            "name": f"{dataset_prefix}/{dataset_name}",
            "title": "",
            "description": "",
            "resources": {},
        }

    for dataset in datasets.values():
        yield None, dataset

    for model in models:
        yield None, model


def read_open_api_manifest(path: Path) -> Generator[tuple[None, dict]]:
    """Read & Convert OpenAPI Schema structure to DSA.

    OpenAPI Schema specification: https://spec.openapis.org/oas/latest.html.
    """
    data = read_file_data_and_transform_to_json(path)

    if not any(spec in data for spec in ("openapi", "swagger")):
        warnings.warn(
            "Unknown specification type. Only OpenAPI 3.* and Swagger 2.0 allowed. Trying to read schema anyway.",
            UserWarning,
        )

    info = data["info"]
    title = info["title"]
    dataset_prefix = f"services/{to_dataset_name(title)}"

    yield from get_namespace_schema(info, title, dataset_prefix)

    yield from get_dataset_schemas(data, dataset_prefix)
