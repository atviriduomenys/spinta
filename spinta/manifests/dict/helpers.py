import pathlib
import json
from copy import deepcopy

import requests
import xmltodict
from typing import List, Dict, Union, TypedDict, Any, Tuple, Callable

from spinta.manifests.dict.components import DictFormat
from spinta.manifests.helpers import TypeDetector
from spinta.utils.itertools import first_dict_value, first_dict_key
from spinta.utils.naming import Deduplicator, to_model_name, to_property_name


def read_schema(manifest_type: DictFormat, path: str, dataset_name: str):
    if path.startswith(('http://', 'https://')):
        value = requests.get(path).text
    else:
        with pathlib.Path(path).open(encoding='utf-8-sig') as f:
            value = f.read()

    converted = {}
    mapping_meta = _MappingMeta(
        is_blank_node=False,
        blank_node_name="model1",
        blank_node_source=".",
        seperator="",
        recursive_descent="",
        model_source_prefix="",
        namespace_seperator=":",
        remove_array_suffix=False,
        check_namespace=False,
        namespace_prefixes={},
        detect_model=is_model
    )
    if manifest_type == DictFormat.JSON:
        converted = json.loads(value)
        mapping_meta["seperator"] = "."
        mapping_meta["recursive_descent"] = "."

    elif manifest_type in (DictFormat.XML, DictFormat.HTML):
        converted = xmltodict.parse(value, cdata_key='text()')
        mapping_meta["seperator"] = "/"
        mapping_meta["recursive_descent"] = "/.."
        mapping_meta["model_source_prefix"] = "/"
        mapping_meta["remove_array_suffix"] = True
        mapping_meta["check_namespace"] = True
        mapping_meta["namespace_prefixes"] = {
            "xmlns": ["xmlns", "@xmlns"]
        }

    namespaces = list(extract_namespaces(converted, mapping_meta))
    converted = _fix_for_blank_nodes(converted)
    prefixes = {}
    for i, (key, value) in enumerate(namespaces):
        prefixes[key] = {
            'type': 'prefix',
            'name': key,
            'uri': value,
            'eid': i
        }
    dataset_structure: _MappedDataset = {
        "dataset": dataset_name if dataset_name else "dataset",
        "resource": "resource",
        "models": {}
    }
    mapping_meta["is_blank_node"] = is_blank_node(converted)
    create_type_detectors(dataset_structure, converted, mapping_meta)

    yield None, {
        'type': 'dataset',
        'name': dataset_structure["dataset"],
        'prefixes': prefixes,
        'resources': {
            dataset_structure["resource"]: {
                'type': manifest_type.value,
                'external': path,
            },
        },
        'given_name': dataset_name
    }

    dedup_model = Deduplicator('{}')
    blank_model = ""
    mapped_models: Dict[Tuple, str] = {}

    for model in dataset_structure["models"].values():
        for model_source, m in model.items():
            new_model = to_model_name(_name_without_namespace(m["name"], mapping_meta, prefixes))
            new_model = dedup_model(new_model)
            mapped_models[(m['name'], model_source)] = new_model

    for model in dataset_structure["models"].values():
        for model_source, m in model.items():
            if model_source == mapping_meta["blank_node_source"]:
                blank_model = mapped_models[(m["name"], model_source)]
            dedup_prop = Deduplicator('_{}')
            converted_props = {}
            for prop in m["properties"].values():
                new_prop = to_property_name(_name_without_namespace(prop["name"], mapping_meta, prefixes))
                new_prop = dedup_prop(new_prop)
                extra = {}
                type_detector = prop["type_detector"]
                prop_type = type_detector.get_type()
                if prop_type == "ref":
                    if prop['name'] in dataset_structure["models"].keys():
                        model_name = _name_without_namespace(mapped_models[prop['name'], prop['extra']], mapping_meta,
                                                             prefixes)
                        ref_model = f'{dataset_structure["dataset"]}/{model_name}'
                    else:
                        ref_model = f'{dataset_structure["dataset"]}/{blank_model}'
                    extra = {
                        'model': ref_model
                    }
                converted_props[new_prop] = {
                    'type': prop_type,
                    'external': {
                        'name': prop["source"]
                    },
                    'description': '',
                    'required': type_detector.required,
                    'unique': type_detector.unique,
                    **extra
                }
                if type_detector.array:
                    copied = deepcopy(converted_props[new_prop])
                    converted_props[new_prop]['type'] = 'array'
                    converted_props[new_prop]['explicitly_given'] = False
                    copied['given_name'] = f'{new_prop}[]'
                    converted_props[new_prop]['items'] = copied

            fixed_external_source = m["source"]
            if mapping_meta["remove_array_suffix"]:
                fixed_external_source = fixed_external_source.replace("[]", "")
            if not fixed_external_source.startswith("."):
                fixed_external_source = f'{mapping_meta["model_source_prefix"]}{fixed_external_source}'
            yield None, {
                'type': 'model',
                'name': f'{dataset_structure["dataset"]}/{mapped_models[(m["name"], model_source)]}',
                'external': {
                    'dataset': dataset_structure["dataset"],
                    'resource': dataset_structure["resource"],
                    'name': fixed_external_source,
                },
                'description': '',
                'properties': converted_props
            }


def is_single_item_dict(value: dict) -> bool:
    return len(value) == 1


def _fix_for_blank_nodes(values: Any):
    return_values = values
    if isinstance(values, dict) and is_single_item_dict(values):
        result = {}
        temp_dict = values
        while is_single_item_dict(temp_dict):
            first_key = first_dict_key(temp_dict)
            temp_dict = temp_dict[first_key]

            if not isinstance(temp_dict, dict):
                return return_values

            result[first_key] = temp_dict if is_single_item_dict(temp_dict) else [temp_dict]

        return_values = result
    return return_values


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
    remove_array_suffix: bool
    model_source_prefix: str
    check_namespace: bool
    namespace_prefixes: dict
    namespace_seperator: str
    detect_model: Callable


class _MappingScope(TypedDict):
    parent_scope: str
    model_scope: str
    model_name: str
    property_name: str


def _name_without_namespace(name: str, mapping_meta: _MappingMeta, prefixes: dict):
    if mapping_meta["namespace_seperator"] in name:
        for prefix in prefixes.keys():
            namespace = f'{prefix}{mapping_meta["namespace_seperator"]}'
            if namespace in name:
                return name.replace(namespace, '')
    return name


def is_model(data):
    if isinstance(data, list) and is_list_of_dicts(data):
        return True
    return False


def _find_parent_key(data, string):
    for key, values in data.items():
        for value in values:
            if string.startswith(value):
                return key
    return None


def extract_namespaces(data: Any, mapping_meta: _MappingMeta):
    if mapping_meta["check_namespace"]:
        if isinstance(data, dict):
            keys_to_remove = {}
            for key, value in data.items():
                parent = _find_parent_key(mapping_meta["namespace_prefixes"], key)
                if parent is not None:
                    if key not in keys_to_remove.keys():
                        return_key = key.split(mapping_meta["namespace_seperator"])
                        if len(return_key) == 1:
                            return_key = [parent]
                        keys_to_remove[key] = (return_key[-1], value)
            for key, value in keys_to_remove.items():
                del data[key]
                yield value
            for value in data.values():
                yield from extract_namespaces(value, mapping_meta)
        elif isinstance(data, list):
            for item in data:
                yield from extract_namespaces(item, mapping_meta)


def nested_prop_names(new_values: list, values: dict, root: str, seperator: str):
    for key, value in values.items():
        if isinstance(value, dict):
            nested_prop_names(new_values, value, f'{root}{seperator}{key}', seperator)
        elif isinstance(value, list):
            if not is_list_of_dicts(value):
                new_values.append(f'{root}{seperator}{key}')
        else:
            new_values.append(f'{root}{seperator}{key}')


def check_missing_prop_required(dataset: _MappedDataset, values: dict, mapping_scope: _MappingScope,
                                mapping_meta: _MappingMeta):
    if mapping_scope["model_scope"] == "":
        model_name = mapping_scope["model_name"] if mapping_scope["model_name"] != "" else first_dict_key(
            dataset["models"])
        model_source = _create_name_with_prefix(
            mapping_scope["parent_scope"],
            mapping_meta["seperator"],
            model_name
        )
        if mapping_scope["model_name"] == "" and mapping_meta["is_blank_node"]:
            model_name = mapping_meta["blank_node_name"]
            model_source = mapping_meta["blank_node_source"]
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
                if not is_list_of_dicts(v):
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
            if mapping_meta["detect_model"](value):
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
                run_type_detectors(dataset, value, new_mapping_scope, mapping_meta)
            else:
                if isinstance(value, dict):
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
        model_source = mapping_meta["blank_node_source"]
    dataset["models"][model_name][model_source]["properties"][prop_name]["type_detector"].detect(value)


def is_list_of_dicts(lst: List) -> bool:
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
            if mapping_meta["detect_model"](value):
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

                setup_model_type_detectors(dataset, value, new_mapping_scope, mapping_meta)

                old_mapping_scope = new_mapping_scope.copy()
                old_mapping_scope["property_name"] = mapping_scope["property_name"]

                if old_mapping_scope["property_name"] == "" and mapping_meta["is_blank_node"]:
                    old_mapping_scope["property_name"] = "parent"
                    set_type_detector(dataset, old_mapping_scope, mapping_meta, is_ref=True)
                elif old_mapping_scope["property_name"] != "":
                    set_type_detector(dataset, old_mapping_scope, mapping_meta, is_ref=True)
            else:
                if isinstance(value, list):
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


def is_blank_node(values: Union[list, dict]) -> bool:
    if isinstance(values, list):
        return True
    if isinstance(values, dict):
        temp_dict = values
        first_value = first_dict_value(temp_dict)
        if is_single_item_dict(temp_dict) and isinstance(first_value, dict):
            while is_single_item_dict(temp_dict) and isinstance(first_value, dict):
                temp_dict = first_value
                first_value = first_dict_value(temp_dict)
                if not is_single_item_dict(temp_dict):
                    return True
                if isinstance(first_value, list):
                    return False
            return True
        else:
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
        if model_name == "" and mapping_meta["is_blank_node"]:
            model_name = mapping_meta["blank_node_name"]
            model_source = mapping_meta["blank_node_source"]
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
            type_detector.array = True
            type_detector.unique = False
            type_detector.required = False
        if is_ref:
            type_detector.type = 'ref'
            type_detector.unique = False
            type_detector.required = False
