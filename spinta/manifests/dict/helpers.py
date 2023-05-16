import pathlib
import json
from typing import List, Dict, Union, TypedDict

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
    seperator = ""
    if manifest_type == DictFormat.JSON:
        seperator = "."
        converted = json.loads(value)
    dataset_structure: MappedDataset = {
        "dataset": "dataset",
        "resource": "resource",
        "models": {}
    }
    starting_source = "."
    starting_model = "model1"
    if isinstance(converted, dict):
        if len(converted.keys()) == 1:
            key = list(converted.keys())[0]
            if isinstance(converted[key], list):
                starting_source = key
                starting_model = key
    setup_model_type_detectors(dataset_structure, converted, starting_model, starting_source, "", seperator)
    run_type_detectors(dataset_structure, converted, starting_model, "", seperator)

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
    for model in dataset_structure["models"].values():
        new_model = to_model_name(model["name"])
        new_model = dedup_model(new_model)
        if model["source"] == ".":
            blank_model = new_model
        dedup_prop = Deduplicator('_{}')
        converted_props = {}
        for prop in model["properties"].values():
            new_prop = to_property_name(prop["name"])
            new_prop = dedup_prop(new_prop)
            extra = {}
            type_detector = prop["type_detector"]
            if type_detector.get_type() == "ref":
                if prop["source"] == "..":
                    ref_model = f'{dataset_structure["dataset"]}/{blank_model}'
                else:
                    ref_model = f'{dataset_structure["dataset"]}/{to_model_name(prop["source"])}'
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
            'name': f'{dataset_structure["dataset"]}/{new_model}',
            'external': {
                'dataset': dataset_structure["dataset"],
                'resource': dataset_structure["resource"],
                'name': model["source"],
            },
            'description': '',
            'properties': converted_props
        }


class MappedProperties(TypedDict):
    name: str
    source: str
    type_detector: TypeDetector


class MappedModels(TypedDict):
    name: str
    source: str
    properties: Dict[str, MappedProperties]


class MappedDataset(TypedDict):
    dataset: str
    resource: str
    models: Dict[str, MappedModels]


def nested_prop_names(new_values: list, values: dict, root: str, seperator: str):
    for key, value in values.items():
        if isinstance(value, dict):
            nested_prop_names(new_values, values, f'{root}{seperator}{key}', seperator)
        else:
            new_values.append(f'{root}{seperator}{key}')


def check_missing_prop_required(dataset: MappedDataset, values: dict, model: str, prefix: str, seperator: str):
    if model != '':
        key_values = []
        for k, v in values.items():
            if isinstance(v, dict):
                new_val = k if prefix == '' else f'{prefix}{seperator}{k}'
                nested_prop_names(key_values, v, new_val, seperator)
            else:
                new_val = k if prefix == '' else f'{prefix}{seperator}{k}'
                key_values.append(new_val)
        filtered_models = [filtered for filtered in dataset["models"][model]["properties"].keys() if f'{prefix}{seperator}' in filtered]
        subtracted = set(filtered_models) - set(key_values)
        for prop in subtracted:
            type_detector = dataset["models"][model]["properties"][prop]["type_detector"]
            if type_detector.required and type_detector.type != 'ref':
                type_detector.required = False


def run_type_detectors(dataset: MappedDataset, values: dict, model: str, prefix: str, seperator: str):
    if isinstance(values, list):
        for item in values:
            run_type_detectors(dataset, item, model, prefix, seperator)
    else:
        check_missing_prop_required(dataset, values, model, prefix, seperator)
        for k, v in values.items():
            if isinstance(v, list):
                if _is_list_of_dicts(v):
                    for item in v:
                        run_type_detectors(dataset, item, k, prefix, seperator)
            elif isinstance(v, dict):
                new_prefix = k if prefix == "" else f'{prefix}{seperator}{k}'

                run_type_detectors(dataset, v, model, new_prefix, seperator)
            else:
                new_prop = k if prefix == "" else f'{prefix}{seperator}{k}'
                dataset["models"][model]["properties"][new_prop]["type_detector"].detect(v)


def _is_list_of_dicts(lst: List) -> bool:
    for item in lst:
        if not isinstance(item, dict):
            return False
    return True


def setup_model_type_detectors(dataset: MappedDataset, values: Union[dict, list], model: str, source: str, prefix: str, seperator: str):
    if isinstance(values, list):
        for item in values:
            setup_model_type_detectors(dataset, item, model, source, prefix, seperator)
    else:
        for k, v in values.items():
            if isinstance(v, list):
                if _is_list_of_dicts(v):
                    for item in v:
                        setup_model_type_detectors(dataset, item, k, k, prefix, seperator)
                        if k != model:
                            prop_name = model
                            prop_source = prop_name
                            if source == ".":
                                prop_name = "parent"
                                prop_source = ".."

                            set_type_detector(dataset, k, prop_name, k, prop_source, prefix, seperator, is_ref=True)
                else:
                    set_type_detector(dataset, model, k, source, k, prefix, seperator, is_array=True)
            elif isinstance(v, dict):
                new_prefix = k if prefix == "" else f'{prefix}{seperator}{k}'
                setup_model_type_detectors(dataset, v, model, source, new_prefix, seperator)
            else:
                set_type_detector(dataset, model, k, source, k, prefix, seperator)


def set_type_detector(dataset: MappedDataset, model: str, prop: str, model_source: str, prop_source: str, prefix: str, seperator: str, is_ref: bool = False, is_array: bool = False):
    if prop != '':
        new_prop = prop if prefix == "" else f'{prefix}{seperator}{prop}'
        if prop == prop_source:
            prop_source = new_prop

        if model in dataset["models"].keys():
            if new_prop not in dataset["models"][model]["properties"].keys():
                dataset["models"][model]["properties"][new_prop] = {
                    "name": new_prop,
                    "source": prop_source,
                    "type_detector": TypeDetector()
                }
        else:
            dataset["models"][model] = {
                "name": model,
                "source": model_source,
                "properties": {
                    new_prop: {
                        "name": new_prop,
                        "source": prop_source,
                        "type_detector": TypeDetector()
                    }
                }
            }
        type_detector = dataset["models"][model]["properties"][new_prop]["type_detector"]
        if is_array:
            type_detector.type = 'array'
            type_detector.unique = False
            type_detector.required = False
        if is_ref:
            type_detector.type = 'ref'
            type_detector.unique = False
            type_detector.required = False




