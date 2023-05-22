import pathlib
import json
from typing import List, Dict, Union, TypedDict, Any, Tuple

from urllib.request import urlopen

from spinta.manifests.dict.components import DictFormat
from spinta.manifests.helpers import TypeDetector
from spinta.utils.naming import Deduplicator, to_model_name, to_property_name


def read_schema(manifest_type: DictFormat, path: str):
    if path.startswith(('http://', 'https://')):
        value = urlopen(path).read()
    else:
        with pathlib.Path(path).open(encoding='utf-8-sig') as f:
            value = f.read()

    converted = {}
    mapping_meta = _MappingMeta(
        is_blank_node=False,
        blank_node_name="model1",
        blank_node_source=".",
        seperator="",
        recursive_descent=""
    )
    if manifest_type == DictFormat.JSON:
        mapping_meta["seperator"] = "."
        mapping_meta["recursive_descent"] = "."
        converted = json.loads(value)
    elif manifest_type in (DictFormat.XML, DictFormat.HTML):
        mapping_meta["seperator"] = "/"
        mapping_meta["recursive_descent"] = "/.."
        # Add xml load
    mapping_meta["is_blank_node"] = _is_blank_node(converted)
    dataset_structure: _MappedDataset = {
        "dataset": "dataset",
        "resource": "resource",
        "models": {}
    }

    create_type_detectors(dataset_structure, converted, mapping_meta)

    yield None, {
        'type': 'dataset',
        'name': dataset_structure["dataset"],
        'resources': {
            dataset_structure["resource"]: {
                'type': manifest_type.value,
                'external': path,
            },
        },
    }

    dedup_model = Deduplicator('{}')
    blank_model = ""
    mapped_models: Dict[Tuple, str] = {}

    for model in dataset_structure["models"].values():
        for model_source, m in model.items():
            new_model = to_model_name(m["name"])
            new_model = dedup_model(new_model)
            mapped_models[(m['name'], model_source)] = new_model

    for model in dataset_structure["models"].values():
        for model_source, m in model.items():
            if model_source == ".":
                blank_model = mapped_models[(m["name"], model_source)]
            dedup_prop = Deduplicator('_{}')
            converted_props = {}
            for prop in m["properties"].values():
                new_prop = to_property_name(prop["name"])
                new_prop = dedup_prop(new_prop)
                extra = {}
                type_detector = prop["type_detector"]
                if type_detector.get_type() == "ref":
                    if prop['name'] in dataset_structure["models"].keys():
                        model_name = mapped_models[prop['name'], prop['extra']]
                        ref_model = f'{dataset_structure["dataset"]}/{model_name}'
                    else:
                        ref_model = f'{dataset_structure["dataset"]}/{blank_model}'
                    extra = {
                        'model': ref_model
                    }
                converted_props[new_prop] = {
                    'type': type_detector.get_type(),
                    'external': {
                        'name': prop["source"]
                    },
                    'description': '',
                    'required': type_detector.required,
                    'unique': type_detector.unique,
                    **extra
                }
            yield None, {
                'type': 'model',
                'name': f'{dataset_structure["dataset"]}/{mapped_models[(m["name"], model_source)]}',
                'external': {
                    'dataset': dataset_structure["dataset"],
                    'resource': dataset_structure["resource"],
                    'name': m["source"],
                },
                'description': '',
                'properties': converted_props
            }


class _MappedProperties(TypedDict):
    name: str
    source: str
    extra: str
    type_detector: TypeDetector


class _MappedModels(TypedDict):
    name: str
    source: str
    properties: Dict[str, _MappedProperties]


class _MappedDataset(TypedDict):
    dataset: str
    resource: str
    models: Dict[str, Dict[str, _MappedModels]]


class _MappingMeta(TypedDict):
    is_blank_node: bool
    blank_node_name: str
    blank_node_source: str
    seperator: str
    recursive_descent: str


class _MappingScope(TypedDict):
    parent_scope: str
    model_scope: str
    model_name: str
    property_name: str


def nested_prop_names(new_values: list, values: dict, root: str, seperator: str):
    for key, value in values.items():
        if isinstance(value, dict):
            nested_prop_names(new_values, value, f'{root}{seperator}{key}', seperator)
        elif isinstance(value, list):
            if not _is_list_of_dicts(value):
                new_values.append(f'{root}{seperator}{key}')
        else:
            new_values.append(f'{root}{seperator}{key}')


def check_missing_prop_required(dataset: _MappedDataset, values: dict, mapping_scope: _MappingScope, mapping_meta: _MappingMeta):
    if mapping_scope["model_scope"] == "":
        model_name = mapping_scope["model_name"] if mapping_scope["model_name"] != "" else list(dataset["models"].keys())[0]
        model_source = _create_name_with_prefix(
            mapping_scope["parent_scope"],
            mapping_meta["seperator"],
            model_name
        )
        if mapping_scope["model_name"] == "":
            model_source = "."
        key_values = []
        for k, v in values.items():
            new_val = _create_name_with_prefix(
                mapping_scope["model_scope"],
                mapping_meta["seperator"],
                k
            )
            if isinstance(v, dict):
                nested_prop_names(key_values, v, new_val, mapping_meta['seperator'])
            elif isinstance(v, list):
                if not _is_list_of_dicts(v):
                    key_values.append(new_val)
            else:
                key_values.append(new_val)
        if key_values:
            filtered_models = dataset["models"][model_name][model_source]["properties"].keys()
            subtracted = set(filtered_models) - set(key_values)
            for prop in subtracted:
                type_detector = dataset["models"][model_name][model_source]["properties"][prop]["type_detector"]
                if type_detector.required and type_detector.type != 'ref':
                    type_detector.required = False


def run_type_detectors(dataset: _MappedDataset, values: dict, mapping_scope: _MappingScope, mapping_meta: _MappingMeta):
    if isinstance(values, list):
        for item in values:
            run_type_detectors(dataset, item, mapping_scope, mapping_meta)
    elif isinstance(values, dict):
        check_missing_prop_required(dataset, values, mapping_scope, mapping_meta)
        for key, value in values.items():
            new_mapping_scope = _MappingScope(
                parent_scope=mapping_scope["parent_scope"],
                model_name=mapping_scope["model_name"],
                model_scope=mapping_scope["model_scope"],
                property_name=key
            )
            if isinstance(value, list):
                if _is_list_of_dicts(value):
                    new_mapping_scope["model_name"] = key
                    parent_scope = _create_name_with_prefix(
                        new_mapping_scope["parent_scope"],
                        mapping_meta["seperator"],
                        "" if mapping_scope["model_name"] == "" else f'{mapping_scope["model_name"]}[]'
                    )
                    parent_scope = _create_name_with_prefix(
                        parent_scope,
                        mapping_meta["seperator"],
                        new_mapping_scope["model_scope"]
                    )
                    new_mapping_scope["model_scope"] = ""
                    new_mapping_scope["parent_scope"] = parent_scope
                    for item in value:
                        run_type_detectors(dataset, item, new_mapping_scope, mapping_meta)
            elif isinstance(value, dict):
                new_mapping_scope["model_scope"] = _create_name_with_prefix(
                    new_mapping_scope["model_scope"],
                    mapping_meta["seperator"],
                    key
                )
                run_type_detectors(dataset, value, new_mapping_scope, mapping_meta)
            else:
                _detect_type(dataset, new_mapping_scope, mapping_meta, value)


def _detect_type(dataset: _MappedDataset, mapping_scope: _MappingScope, mapping_meta: _MappingMeta, value: Any):
    prop_name = _create_name_with_prefix(
        mapping_scope["model_scope"],
        mapping_meta["seperator"],
        mapping_scope["property_name"]
    )
    model_name = mapping_scope["model_name"]
    model_source = _create_name_with_prefix(
        mapping_scope["parent_scope"],
        mapping_meta["seperator"],
        mapping_scope["model_name"]
    )
    if model_name == "":
        model_name = mapping_meta["blank_node_name"]
        model_source = "."
    dataset["models"][model_name][model_source]["properties"][prop_name]["type_detector"].detect(value)


def _is_list_of_dicts(lst: List) -> bool:
    for item in lst:
        if not isinstance(item, dict):
            return False
    return True


def setup_model_type_detectors(dataset: _MappedDataset, values: Union[dict, list], mapping_scope: _MappingScope,
                               mapping_meta: _MappingMeta):
    if isinstance(values, list):
        for item in values:
            setup_model_type_detectors(dataset, item, mapping_scope, mapping_meta)
    elif isinstance(values, dict):
        for key, value in values.items():
            new_mapping_scope = _MappingScope(
                parent_scope=mapping_scope["parent_scope"],
                model_name=mapping_scope["model_name"],
                model_scope=mapping_scope["model_scope"],
                property_name=key
            )
            if isinstance(value, list):
                if _is_list_of_dicts(value):
                    new_mapping_scope["model_name"] = key
                    parent_scope = _create_name_with_prefix(
                        new_mapping_scope["parent_scope"],
                        mapping_meta["seperator"],
                        "" if mapping_scope["model_name"] == "" else f'{mapping_scope["model_name"]}[]'
                    )
                    parent_scope = _create_name_with_prefix(
                        parent_scope,
                        mapping_meta["seperator"],
                        new_mapping_scope["model_scope"]
                    )
                    new_mapping_scope["model_scope"] = ""
                    new_mapping_scope["parent_scope"] = parent_scope
                    for item in value:
                        setup_model_type_detectors(dataset, item, new_mapping_scope, mapping_meta)
                    old_mapping_scope = new_mapping_scope.copy()
                    old_mapping_scope["property_name"] = mapping_scope["property_name"]
                    if mapping_scope["property_name"] == "" and mapping_meta["is_blank_node"]:
                        old_mapping_scope["property_name"] = "parent"
                        set_type_detector(dataset, old_mapping_scope, mapping_meta, is_ref=True)
                    else:
                        set_type_detector(dataset, old_mapping_scope, mapping_meta, is_ref=True)
                else:
                    set_type_detector(dataset, new_mapping_scope, mapping_meta, is_array=True)
            elif isinstance(value, dict):
                new_mapping_scope["model_scope"] = _create_name_with_prefix(
                    new_mapping_scope["model_scope"],
                    mapping_meta["seperator"],
                    key
                )
                new_mapping_scope["property_name"] = mapping_scope["property_name"]
                setup_model_type_detectors(dataset, value, new_mapping_scope, mapping_meta)
            else:
                set_type_detector(dataset, new_mapping_scope, mapping_meta)


def _create_name_with_prefix(prefix: str, seperator: str, value: str) -> str:
    if value == "":
        return prefix
    return value if prefix == "" else f'{prefix}{seperator}{value}'


def create_type_detectors(dataset: _MappedDataset, values: Any, mapping_meta: _MappingMeta):
    mapping_scope = _MappingScope(
        parent_scope="",
        model_scope="",
        model_name="",
        property_name=""
    )
    setup_model_type_detectors(dataset, values, mapping_scope, mapping_meta)
    run_type_detectors(dataset, values, mapping_scope, mapping_meta)


def _is_blank_node(values: Union[list, dict]) -> bool:
    if isinstance(values, list):
        return True
    if isinstance(values, dict):
        for key, value in values.items():
            if not isinstance(value, list):
                return True
    return False


def set_type_detector(dataset: _MappedDataset, mapping_scope: _MappingScope, mapping_meta: _MappingMeta,
                      is_ref: bool = False, is_array: bool = False):
    if mapping_scope["property_name"] != '':
        model_name = mapping_scope["model_name"]
        model_source = _create_name_with_prefix(
            mapping_scope["parent_scope"],
            mapping_meta["seperator"],
            model_name
        )
        prop_name = _create_name_with_prefix(
            mapping_scope["model_scope"],
            mapping_meta["seperator"],
            mapping_scope["property_name"]
        )
        prop_source = prop_name
        if model_name == "":
            model_name = mapping_meta["blank_node_name"]
            model_source = "."
        extra = ""
        if is_ref:
            extra = ""
            split = mapping_scope["parent_scope"].split(mapping_meta["seperator"])
            count = 0
            for i in reversed(split):
                if i.endswith("[]"):
                    half = mapping_scope["parent_scope"].split(i)
                    extra = i[:-2] if half[0] == '' else f'{half[0]}{i[:-2]}'
                    break
                elif i != '':
                    count += 1
            prop_source = f"..{mapping_meta['recursive_descent'] * count}"

        if model_name in dataset["models"].keys():
            if model_source in dataset["models"][model_name].keys():
                if prop_name not in dataset["models"][model_name][model_source]["properties"].keys():
                    dataset["models"][model_name][model_source]["properties"][prop_name] = {
                        "name": prop_name,
                        "source": prop_source,
                        "type_detector": TypeDetector(),
                        'extra': extra
                    }
            else:
                dataset["models"][model_name][model_source] = {
                    "name": model_name,
                    "source": model_source,
                    "properties": {
                        prop_name: {
                            "name": prop_name,
                            "source": prop_source,
                            "type_detector": TypeDetector(),
                            'extra': extra
                        }
                    }
                }
        else:
            dataset["models"][model_name] = {
                model_source: {
                    "name": model_name,
                    "source": model_source,
                    "properties": {
                        prop_name: {
                            "name": prop_name,
                            "source": prop_source,
                            "type_detector": TypeDetector(),
                            'extra': extra
                        }
                    }
                }
            }

        type_detector = dataset["models"][model_name][model_source]["properties"][prop_name]["type_detector"]
        if is_array:
            type_detector.type = 'array'
            type_detector.unique = False
            type_detector.required = False
        if is_ref:
            type_detector.type = 'ref'
            type_detector.unique = False
            type_detector.required = False
