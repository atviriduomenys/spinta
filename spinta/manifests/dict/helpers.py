import contextlib
import logging

import pathlib
import json
from abc import ABC, abstractmethod
from copy import deepcopy

import requests
from typing import Any, Iterator, IO

from spinta import HTTP_URL_PREFIXES
from spinta.manifests.components import ManifestSchema
from spinta.manifests.dict.components import (
    DictFormat,
    MappedDataset,
    MappingMeta,
    MappingScope,
    MappedModels,
    MappedProperties,
)
from spinta.manifests.helpers import TypeDetector
from spinta.utils.itertools import first_dict_value, first_dict_key
from spinta.utils.naming import Deduplicator, to_model_name, to_property_name

from lxml import etree


logger = logging.getLogger(__name__)

# lxml iterparse events:
EVENT_START_NS = "start-ns"  # Fired when XML namespace is found.
EVENT_START = "start"  # Fired when XML element is found. Only element and its attributes available.
EVENT_END = "end"  # Fired when XML element closing tag is found. Element content is available.

RIGHT_CURLY_BRACE = "}"


class DictSchemaReader(ABC):
    path: str
    mapping_meta: MappingMeta
    dataset_structure: MappedDataset

    def __init__(self, path: str, mapping_meta: MappingMeta, dataset_structure: MappedDataset) -> None:
        self.path = path
        self.mapping_meta = mapping_meta
        self.dataset_structure = dataset_structure

    @contextlib.contextmanager
    def read_path(self) -> Iterator[IO[bytes]]:
        if self.path.startswith(HTTP_URL_PREFIXES):
            response = requests.get(self.path, stream=True)
            response.raise_for_status()
            try:
                yield response.raw
            finally:
                response.close()
        else:
            with pathlib.Path(self.path).open("rb") as file:
                yield file

    @abstractmethod
    def read_schema(self) -> None: ...


class JSONSchemaReader(DictSchemaReader):
    path: str
    mapping_meta: MappingMeta
    dataset_structure: MappedDataset

    def read_schema(self) -> None:
        with self.read_path() as stream:
            json_data = json.loads(stream.read().decode("utf-8"))
            self.dataset_structure, _ = parse_data(self.mapping_meta, json_data, self.dataset_structure)


class XMLIterSchemaReader(DictSchemaReader):
    """Reads XML data and converts it into mapped data by collecting unique structure using streaming"""

    path: str
    mapping_meta: MappingMeta
    dataset_structure: MappedDataset

    __structural_data: dict
    namespaces: list

    def __init__(
        self,
        path: str,
        mapping_meta: MappingMeta,
        dataset_structure: MappedDataset,
    ) -> None:
        super().__init__(path, mapping_meta, dataset_structure)

        self.namespaces = []
        self.__structural_data = {}
        self.__seen_tags = [set()]

    def read_schema(self) -> None:
        with self.read_path() as stream:
            context = etree.iterparse(stream, events=(EVENT_START, EVENT_END, EVENT_START_NS), recover=True)
            path = []

            for event, element in context:
                if event == EVENT_START_NS:
                    prefix, uri = element
                    prefix = "xmlns" if prefix == "" else prefix
                    if (prefix, uri) not in self.namespaces:
                        self.namespaces.append((prefix, uri))
                    continue

                # Get tag with prefix instead of tag with namespace.
                tag = self._get_element_tag_with_prefix(element)

                if event == EVENT_START:
                    is_repeat_in_parent = tag in self.__seen_tags[-1]
                    self.__seen_tags[-1].add(tag)
                    self.__seen_tags.append(set())

                    path.append(tag)
                    self._merge_start(path, element, is_repeat_in_parent)
                elif event == EVENT_END:
                    self.__seen_tags.pop()
                    self._merge_end(path, element)
                    path.pop()
                    # Memory cleanup
                    element.clear()
                    while element.getprevious() is not None:
                        del element.getparent()[0]

        # Process the collected structural data once at the end
        self.parse_structure()

    @staticmethod
    def _strip_namespace(tag: str) -> str:
        return tag.split(RIGHT_CURLY_BRACE)[-1] if RIGHT_CURLY_BRACE in tag else tag

    def _get_element_tag_with_prefix(self, element: etree._Element) -> str:
        """Replace element namespace with prefix"""
        tag = self._strip_namespace(element.tag)
        return f"{element.prefix}:{tag}" if element.prefix else tag

    def _get_element_attribute_with_with_prefix(self, attribute: str, nsmap: dict[str, str]) -> str:
        """If attribute has a namespace - replace namespace with prefix.
        Examples:
            "{https://example.com/xsi}code" -> "xsi:code", if namespace exists in nsmap
            "{https://example.com/xsi}code" -> "code", if namespace does not exist in nsmap
            "code" -> "code"
        """
        if RIGHT_CURLY_BRACE not in attribute:
            return f"@{attribute}"

        attribute_namespace = (attribute.split(RIGHT_CURLY_BRACE)[0]).strip("{}")
        attribute_name = self._strip_namespace(attribute)

        return next(
            (f"@{prefix}:{attribute_name}" for prefix, ns in nsmap.items() if attribute_namespace == ns),
            f"@{attribute_name}",
        )

    @staticmethod
    def _get_inner_value(data: dict[str, list | dict], tag: str) -> dict:
        return data[tag][0] if isinstance(data[tag], list) else data[tag]

    def _merge_start(self, path: list[str], element: etree._Element, is_repeat_in_parent: bool) -> None:
        """Merges element and attributes to self.__structural_data"""
        current = self.__structural_data
        for i, tag in enumerate(path[:-1]):
            parent = current
            current = self._get_inner_value(current, tag)

            # If we previously thought this was a leaf (None or str), but now it has a child,
            # we must convert it to a dictionary.
            if not isinstance(current, dict):
                if isinstance(parent[tag], list):
                    parent[tag][0] = {"text()": current} if current is not None else {}
                    current = parent[tag][0]
                else:
                    parent[tag] = {"text()": current} if current is not None else {}
                    current = parent[tag]

        tag = path[-1]
        if tag not in current:
            # Tag does not exist. Add tag and element attributes to python structure
            if element.attrib:
                current[tag] = {
                    self._get_element_attribute_with_with_prefix(k, element.nsmap): v for k, v in element.attrib.items()
                }
            else:
                current[tag] = None
        else:
            # Repetition detected, promote to list for array inference
            if is_repeat_in_parent and not isinstance(current[tag], list):
                current[tag] = [current[tag]]

            target = self._get_inner_value(current, tag)

            if element.attrib:
                if not isinstance(target, dict):
                    # Convert simple value to dict to accommodate attributes
                    target_value = {"text()": target} if target is not None else {}
                    if isinstance(current[tag], list):
                        current[tag][0] = target_value
                    else:
                        current[tag] = target_value
                    target = target_value

                for key, value in element.attrib.items():
                    target[self._get_element_attribute_with_with_prefix(key, element.nsmap)] = value

    def _merge_end(self, path: list[str], element: etree._Element) -> None:
        """Merges element text to self.__structural_data"""
        text = element.text.strip() if element.text else None
        if not text:
            return

        current = self.__structural_data
        for tag in path[:-1]:
            current = self._get_inner_value(current, tag)

        tag = path[-1]
        if isinstance(current[tag], list):
            target = current[tag][0]
            if isinstance(target, dict):
                target["text()"] = text
            else:
                current[tag][0] = text
        else:
            target = current[tag]
            if isinstance(target, dict):
                target["text()"] = text
            else:
                current[tag] = text

    def parse_structure(self) -> None:
        """
        Process the consolidated structural data and save processed result into self.dataset_structure
        """
        self.dataset_structure, namespaces = parse_data(
            self.mapping_meta,
            self.__structural_data,
            self.dataset_structure,
        )

        # Add unique namespaces to self.namespaces and keep ordering
        for namespace in namespaces:
            if namespace not in self.namespaces:
                self.namespaces.append(namespace)


def read_schema(manifest_type: DictFormat, path: str, dataset_name: str) -> Iterator[ManifestSchema]:
    mapping_meta = MappingMeta.get_for(manifest_type)
    dataset_structure = MappedDataset(
        dataset=dataset_name if dataset_name else "dataset",
        given_dataset_name=dataset_name,
        resource="resource",
        resource_type=f"dask/{manifest_type.value}",
        resource_path=path,
        models={},
    )

    if manifest_type == DictFormat.JSON:
        json_reader = JSONSchemaReader(path, mapping_meta, dataset_structure)
        json_reader.read_schema()
        yield from convert_to_manifest(mapping_meta, [], dataset_structure)

    elif manifest_type in (DictFormat.XML, DictFormat.HTML):
        xml_reader = XMLIterSchemaReader(path, mapping_meta, dataset_structure)
        xml_reader.read_schema()
        yield from convert_to_manifest(mapping_meta, xml_reader.namespaces, xml_reader.dataset_structure)


def parse_data(
    mapping_meta: MappingMeta, data: dict, dataset_structure: MappedDataset
) -> tuple[MappedDataset, list[Any]]:
    namespaces = list(extract_namespaces(data, mapping_meta))

    converted = _fix_for_blank_nodes(data)

    mapping_meta.is_blank_node = is_blank_node(converted)

    create_type_detectors(dataset_structure, converted, mapping_meta)

    return dataset_structure, namespaces


def convert_to_manifest(
    mapping_meta: MappingMeta,
    namespaces: list[Any],
    dataset_structure: MappedDataset,
) -> Iterator[ManifestSchema]:
    prefixes = {}
    for i, (key, value) in enumerate(namespaces):
        prefixes[key] = {"type": "prefix", "name": key, "uri": value, "eid": i}

    yield (
        None,
        {
            "type": "dataset",
            "name": dataset_structure.dataset,
            "prefixes": prefixes,
            "resources": {
                dataset_structure.resource: {
                    "type": dataset_structure.resource_type,
                    "external": dataset_structure.resource_path,
                },
            },
            "given_name": dataset_structure.given_dataset_name,
        },
    )

    dedup_model = Deduplicator("{}")
    blank_model = ""
    mapped_models: dict[tuple, str] = {}

    for model in dataset_structure.models.values():
        for model_source, m in model.items():
            new_model = to_model_name(_name_without_namespace(m.name, mapping_meta, prefixes))
            new_model = dedup_model(new_model)
            mapped_models[(m.name, model_source)] = new_model

    for model in dataset_structure.models.values():
        for model_source, m in model.items():
            if model_source == mapping_meta.blank_node_source:
                blank_model = mapped_models[(m.name, model_source)]
            dedup_prop = Deduplicator("_{}")
            converted_props = {}
            for prop in m.properties.values():
                new_prop = to_property_name(_name_without_namespace(prop.name, mapping_meta, prefixes))
                new_prop = dedup_prop(new_prop)
                extra = {}
                type_detector = prop.type_detector
                prop_type = type_detector.get_type()
                if prop_type == "ref":
                    if prop.name in dataset_structure.models.keys():
                        model_name = _name_without_namespace(
                            mapped_models[prop.name, prop.extra], mapping_meta, prefixes
                        )
                        ref_model = f"{dataset_structure.dataset}/{model_name}"
                    else:
                        ref_model = f"{dataset_structure.dataset}/{blank_model}"
                    extra = {"model": ref_model}
                converted_props[new_prop] = {
                    "type": prop_type,
                    "external": {"name": prop.source},
                    "description": "",
                    "required": type_detector.required,
                    "unique": type_detector.unique,
                    **extra,
                }
                if type_detector.array:
                    copied = deepcopy(converted_props[new_prop])
                    converted_props[new_prop]["type"] = "array"
                    converted_props[new_prop]["explicitly_given"] = False
                    copied["given_name"] = f"{new_prop}[]"
                    converted_props[new_prop]["items"] = copied

            fixed_external_source = m.source
            if mapping_meta.remove_array_suffix:
                fixed_external_source = fixed_external_source.replace("[]", "")
            if not fixed_external_source.startswith("."):
                fixed_external_source = f"{mapping_meta.model_source_prefix}{fixed_external_source}"
            yield (
                None,
                {
                    "type": "model",
                    "name": f"{dataset_structure.dataset}/{mapped_models[(m.name, model_source)]}",
                    "external": {
                        "dataset": dataset_structure.dataset,
                        "resource": dataset_structure.resource,
                        "name": fixed_external_source,
                    },
                    "description": "",
                    "properties": converted_props,
                },
            )


def is_single_item_dict(value: dict) -> bool:
    return len(value) == 1


def _fix_for_blank_nodes(values: Any) -> Any:
    if not (isinstance(values, dict) and is_single_item_dict(values)):
        return values

    return_values = values
    temp_dict = values
    result = {}

    while is_single_item_dict(temp_dict):
        first_key = first_dict_key(temp_dict)
        temp_dict = temp_dict[first_key]

        if not isinstance(temp_dict, dict):
            return return_values

        result[first_key] = temp_dict if is_single_item_dict(temp_dict) else [temp_dict]

    return_values = result
    return return_values


def _name_without_namespace(name: str, mapping_meta: MappingMeta, prefixes: dict) -> str:
    if mapping_meta.namespace_seperator not in name:
        return name

    for prefix in prefixes.keys():
        if (namespace := f"{prefix}{mapping_meta.namespace_seperator}") in name:
            return name.replace(namespace, "")

    return name


def is_model(data: Any) -> bool:
    return isinstance(data, list) and is_list_of_dicts(data)


def _find_parent_key(data: dict[str, list[str]], string: str) -> str | None:
    for key, values in data.items():
        for value in values:
            if string.startswith(value):
                return key
    return None


def extract_namespaces(data: Any, mapping_meta: MappingMeta) -> Iterator[Any]:
    if not mapping_meta.check_namespace:
        return

    if isinstance(data, dict):
        keys_to_remove = {}
        for key, value in data.items():
            parent = _find_parent_key(mapping_meta.namespace_prefixes, key)
            if parent is not None and key not in keys_to_remove.keys():
                return_key = key.split(mapping_meta.namespace_seperator)
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


def nested_prop_names(new_values: list, values: dict, root: str, seperator: str) -> None:
    for key, value in values.items():
        if isinstance(value, dict):
            nested_prop_names(new_values, value, f"{root}{seperator}{key}", seperator)
        elif isinstance(value, list):
            if not is_list_of_dicts(value):
                new_values.append(f"{root}{seperator}{key}")
        else:
            new_values.append(f"{root}{seperator}{key}")


def check_missing_prop_required(
    dataset: MappedDataset, values: dict, mapping_scope: MappingScope, mapping_meta: MappingMeta
) -> None:
    if mapping_scope.model_scope != "":
        return

    model_name = mapping_scope.model_name if mapping_scope.model_name != "" else first_dict_key(dataset.models)
    model_source = _create_name_with_prefix(mapping_scope.parent_scope, mapping_meta.seperator, model_name)
    if mapping_scope.model_name == "" and mapping_meta.is_blank_node:
        model_name = mapping_meta.blank_node_name
        model_source = mapping_meta.blank_node_source

    key_values = []
    for key, value in values.items():
        new_value = _create_name_with_prefix(mapping_scope.model_scope, mapping_meta.seperator, key)
        if isinstance(value, dict):
            nested_prop_names(key_values, value, new_value, mapping_meta.seperator)
        elif isinstance(value, list):
            if not is_list_of_dicts(value):
                key_values.append(new_value)
        else:
            key_values.append(new_value)

    if not key_values:
        return

    filtered_models = dataset.models[model_name][model_source].properties.keys()
    subtracted = set(filtered_models) - set(key_values)
    for prop in subtracted:
        type_detector = dataset.models[model_name][model_source].properties[prop].type_detector
        if type_detector.required and type_detector.type != "ref":
            type_detector.required = False


def run_type_detectors(
    dataset: MappedDataset, values: dict, mapping_scope: MappingScope, mapping_meta: MappingMeta
) -> None:
    if isinstance(values, list):
        for item in values:
            run_type_detectors(dataset, item, mapping_scope, mapping_meta)

    elif isinstance(values, dict):
        check_missing_prop_required(dataset, values, mapping_scope, mapping_meta)
        for key, value in values.items():
            if is_model(value):
                parent_scope = _create_name_with_prefix(
                    prefix=_create_name_with_prefix(
                        prefix=mapping_scope.parent_scope,
                        seperator=mapping_meta.seperator,
                        value="" if mapping_scope.model_name == "" else f"{mapping_scope.model_name}[]",
                    ),
                    seperator=mapping_meta.seperator,
                    value=mapping_scope.model_scope,
                )
                new_mapping_scope = MappingScope(
                    parent_scope=parent_scope,
                    model_name=key,
                    model_scope="",
                    property_name=key,
                )
                run_type_detectors(dataset, value, new_mapping_scope, mapping_meta)
            else:
                new_mapping_scope = MappingScope(
                    parent_scope=mapping_scope.parent_scope,
                    model_name=mapping_scope.model_name,
                    model_scope=mapping_scope.model_scope,
                    property_name=key,
                )
                if isinstance(value, dict):
                    new_mapping_scope.model_scope = _create_name_with_prefix(
                        prefix=new_mapping_scope.model_scope,
                        seperator=mapping_meta.seperator,
                        value=key,
                    )
                    run_type_detectors(dataset, value, new_mapping_scope, mapping_meta)
                else:
                    _detect_type(dataset, new_mapping_scope, mapping_meta, value)


def _detect_type(dataset: MappedDataset, mapping_scope: MappingScope, mapping_meta: MappingMeta, value: Any) -> None:
    prop_name = _create_name_with_prefix(mapping_scope.model_scope, mapping_meta.seperator, mapping_scope.property_name)
    model_name = mapping_scope.model_name
    model_source = _create_name_with_prefix(
        mapping_scope.parent_scope, mapping_meta.seperator, mapping_scope.model_name
    )
    if model_name == "":
        model_name = mapping_meta.blank_node_name
        model_source = mapping_meta.blank_node_source
    dataset.models[model_name][model_source].properties[prop_name].type_detector.detect(value)


def is_list_of_dicts(data: list) -> bool:
    return all(isinstance(item, dict) for item in data)


def setup_model_type_detectors(
    dataset: MappedDataset, values: dict | list, mapping_scope: MappingScope, mapping_meta: MappingMeta
) -> None:
    if isinstance(values, list):
        for item in values:
            setup_model_type_detectors(dataset, item, mapping_scope, mapping_meta)

    elif isinstance(values, dict):
        for key, value in values.items():
            if is_model(value):
                parent_scope = _create_name_with_prefix(
                    prefix=_create_name_with_prefix(
                        prefix=mapping_scope.parent_scope,
                        seperator=mapping_meta.seperator,
                        value="" if mapping_scope.model_name == "" else f"{mapping_scope.model_name}[]",
                    ),
                    seperator=mapping_meta.seperator,
                    value=mapping_scope.model_scope,
                )

                new_mapping_scope = MappingScope(
                    parent_scope=parent_scope,
                    model_name=key,
                    model_scope="",
                    property_name=key,
                )
                setup_model_type_detectors(dataset, value, new_mapping_scope, mapping_meta)

                old_mapping_scope = deepcopy(new_mapping_scope)
                old_mapping_scope.property_name = mapping_scope.property_name

                if old_mapping_scope.property_name == "" and mapping_meta.is_blank_node:
                    old_mapping_scope.property_name = "parent"
                    set_type_detector(dataset, old_mapping_scope, mapping_meta, is_ref=True)
                elif old_mapping_scope.property_name != "":
                    set_type_detector(dataset, old_mapping_scope, mapping_meta, is_ref=True)

            else:
                new_mapping_scope = MappingScope(
                    parent_scope=mapping_scope.parent_scope,
                    model_name=mapping_scope.model_name,
                    model_scope=mapping_scope.model_scope,
                    property_name=key,
                )
                if isinstance(value, list):
                    set_type_detector(dataset, new_mapping_scope, mapping_meta, is_array=True)
                elif isinstance(value, dict):
                    new_mapping_scope.model_scope = _create_name_with_prefix(
                        new_mapping_scope.model_scope, mapping_meta.seperator, key
                    )
                    new_mapping_scope.property_name = mapping_scope.property_name
                    setup_model_type_detectors(dataset, value, new_mapping_scope, mapping_meta)
                else:
                    set_type_detector(dataset, new_mapping_scope, mapping_meta)


def _create_name_with_prefix(prefix: str, seperator: str, value: str) -> str:
    if value == "":
        return prefix
    if prefix == "":
        return value

    return f"{prefix}{seperator}{value}"


def create_type_detectors(dataset: MappedDataset, values: Any, mapping_meta: MappingMeta) -> None:
    mapping_scope = MappingScope(parent_scope="", model_scope="", model_name="", property_name="")
    setup_model_type_detectors(dataset, values, mapping_scope, mapping_meta)
    run_type_detectors(dataset, values, mapping_scope, mapping_meta)


def is_blank_node(values: list | dict) -> bool:
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


def set_type_detector(
    dataset: MappedDataset,
    mapping_scope: MappingScope,
    mapping_meta: MappingMeta,
    is_ref: bool = False,
    is_array: bool = False,
) -> None:
    if mapping_scope.property_name == "":
        return

    model_name = mapping_scope.model_name
    model_source = _create_name_with_prefix(mapping_scope.parent_scope, mapping_meta.seperator, model_name)
    prop_name = _create_name_with_prefix(mapping_scope.model_scope, mapping_meta.seperator, mapping_scope.property_name)
    prop_source = prop_name
    if model_name == "" and mapping_meta.is_blank_node:
        model_name = mapping_meta.blank_node_name
        model_source = mapping_meta.blank_node_source
    extra = ""
    if is_ref:
        split = mapping_scope.parent_scope.split(mapping_meta.seperator)
        count = 0
        for i in reversed(split):
            if i.endswith("[]"):
                half = mapping_scope.parent_scope.split(i)
                extra = i[:-2] if half[0] == "" else f"{half[0]}{i[:-2]}"
                break
            elif i != "":
                count += 1
        prop_source = f"..{mapping_meta.recursive_descent * count}"

    if model_name in dataset.models.keys():
        if model_source in dataset.models[model_name].keys():
            if prop_name not in dataset.models[model_name][model_source].properties.keys():
                dataset.models[model_name][model_source].properties[prop_name] = MappedProperties(
                    name=prop_name,
                    source=prop_source,
                    type_detector=TypeDetector(),
                    extra=extra,
                )
        else:
            dataset.models[model_name][model_source] = MappedModels(
                name=model_name,
                source=model_source,
                properties={
                    prop_name: MappedProperties(
                        name=prop_name,
                        source=prop_source,
                        type_detector=TypeDetector(),
                        extra=extra,
                    )
                },
            )
    else:
        dataset.models[model_name] = {
            model_source: MappedModels(
                name=model_name,
                source=model_source,
                properties={
                    prop_name: MappedProperties(
                        name=prop_name,
                        source=prop_source,
                        type_detector=TypeDetector(),
                        extra=extra,
                    )
                },
            )
        }

    type_detector = dataset.models[model_name][model_source].properties[prop_name].type_detector
    if is_array:
        type_detector.array = True
        type_detector.unique = False
        type_detector.required = False
    if is_ref:
        type_detector.type = "ref"
        type_detector.unique = False
        type_detector.required = False
    # Do not set unique or required for lxml parsed manifests
    if mapping_meta.manifest_type in (DictFormat.XML, DictFormat.HTML):
        type_detector.unique = False
        type_detector.required = False
