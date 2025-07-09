from __future__ import annotations

import json
import re
from collections.abc import Generator
from pathlib import Path

from spinta.core.ufuncs import Expr
from spinta.utils.naming import to_code_name, to_dataset_name, to_model_name

SUPPORTED_PARAMETER_LOCATIONS = {"query", "header", "path"}
DEFAULT_DATASET_NAME = "default"


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


class Model:
    def __init__(
        self, dataset: str, resource: str, basename: str, source: str, json_schema: dict, parent: Model | None = None
    ) -> None:
        self.dataset: str = dataset
        self.resource: str = resource
        self.basename: str = basename
        self.json_schema: dict = json_schema
        self.parent: Model | None = parent
        self.source: str = source
        self.title: str = self.json_schema.get("title")
        self.description: str = self.json_schema.get("description")
        self.name: str = f"{self.dataset}/{to_model_name(self.basename)}"
        self.children: list[Model] = []
        self.properties: list[Property] = []
        self.extract_children()

    def add_children(self, basename: str, json_schema: dict) -> None:
        self.children.append(Model(self.dataset, self.resource, basename, basename, json_schema, self))

    def extract_children(self) -> None:
        for property_name, property_metadata in self.json_schema.get("properties", {}).items():
            if property_metadata.get("type") == "object":
                self.add_children(property_name, property_metadata)
            elif property_metadata.get("items", {}).get("type") == "object":
                self.add_children(property_name, property_metadata["items"])

    def get_node_schema_dicts(self) -> list[dict]:
        result = [
            {
                "type": "model",
                "name": self.name,
                "title": self.title,
                "description": self.description,
                "access": "open",
                "external": {"dataset": self.dataset, "resource": self.resource, "name": self.source},
            }
        ]
        for child in self.children:
            result.extend(child.get_node_schema_dicts())
        return result

    def __repr__(self) -> str:
        return f"<Model {self.name}>"


class Property:
    pass


def get_model_schemas(dataset_name: str, resource_name: str, response_200: dict) -> list[dict]:
    if not (json_schema := response_200.get("content", {}).get("application/json", {}).get("schema", {})):
        return []

    json_type = json_schema.get("type")

    basename = json_schema.get("title", response_200["description"])

    if json_type == "object":
        schema = json_schema
        source = "."

    elif json_type == "array" and json_schema.get("items", {}).get("type") == "object":
        schema = json_schema.get("items", {})
        source = ".[]"
    else:
        return []

    model = Model(dataset_name, resource_name, basename, source, schema)

    return model.get_node_schema_dicts()


def get_dataset_schemas(data: dict, dataset_prefix: str) -> Generator[tuple[None, dict]]:
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
            response_200 = http_method_metadata.get("responses", {}).get("200", {})
            models += get_model_schemas(f"{dataset_prefix}/{dataset_name}", resource_name, response_200)

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

    if models:
        for model in models:
            yield None, model


def read_open_api_manifest(path: Path) -> Generator[tuple[None, dict]]:
    """Read & Convert OpenAPI Schema structure to DSA.

    OpenAPI Schema specification: https://spec.openapis.org/oas/latest.html.
    """
    data = read_file_data_and_transform_to_json(path)

    info = data["info"]
    title = info["title"]
    dataset_prefix = f"services/{to_dataset_name(title)}"

    yield from get_namespace_schema(info, title, dataset_prefix)

    yield from get_dataset_schemas(data, dataset_prefix)
