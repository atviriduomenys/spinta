from __future__ import annotations

from collections.abc import Iterable, Mapping
from copy import deepcopy
import logging
import os
from tempfile import TemporaryDirectory
from typing import Any
from urllib.request import urlopen
import xml.etree.ElementTree as xml_etree

import lxml.etree as etree
from lxml.etree import QName, _Element

from spinta.manifests.wsdl.ambiguity import raise_ambiguous_wsdl_reference, raise_duplicate_wsdl_qname_conflict
from spinta.manifests.wsdl.qname import WsdlQName, wsdl_qname_key
from spinta.manifests.wsdl.xsd.datatypes import normalize_embedded_field_type
from spinta.manifests.wsdl.xsd.schema_sources import TOLERATED_SCHEMA_ERRORS, load_schema_root as _load_schema_root, normalize_schema_source, resolve_schema_reference
from spinta.manifests.xsd.helpers import XSDModel, XSDProperty, XSDReader
from spinta.utils.naming import Deduplicator
from spinta.utils.naming import to_property_name

XSD_NS = "http://www.w3.org/2001/XMLSchema"


logger = logging.getLogger(__name__)


def load_schema_root(source: str) -> _Element:
    return _load_schema_root(source, urlopen_handler=urlopen)


def compose_schema_tree(
    root: _Element,
    source: str,
    visited: set[str] | None = None,
    *,
    tolerate_errors: bool = False,
    diagnostics: list[Exception] | None = None,
) -> _Element:
    normalized_source = normalize_schema_source(source)
    if visited is None:
        visited = set()
    if normalized_source in visited:
        return etree.Element(root.tag, nsmap=root.nsmap)

    visited = visited | {normalized_source}
    composed_root = deepcopy(root)
    referenced_children: list[_Element] = []

    for child in list(composed_root):
        if isinstance(child, etree._Comment):
            continue

        local_name = QName(child).localname
        if local_name not in {"include", "import"}:
            continue

        schema_location = child.attrib.get("schemaLocation")
        if not schema_location:
            logger.warning("tag %s without schemaLocation is not supported yet", local_name)
            composed_root.remove(child)
            continue

        referenced_source = resolve_schema_reference(source, schema_location)
        try:
            referenced_root = compose_schema_tree(
                load_schema_root(referenced_source),
                referenced_source,
                visited,
                tolerate_errors=tolerate_errors,
                diagnostics=diagnostics,
            )
        except TOLERATED_SCHEMA_ERRORS as error:
            if not tolerate_errors:
                raise
            if diagnostics is not None:
                diagnostics.append(error)
            composed_root.remove(child)
            continue
        for referenced_child in referenced_root.getchildren():
            if isinstance(referenced_child, etree._Comment):
                continue
            referenced_children.append(deepcopy(referenced_child))
        composed_root.remove(child)

    for referenced_child in referenced_children:
        composed_root.append(referenced_child)

    return composed_root


def local_name(name: str | None) -> str:
    if not name:
        return ""
    return name.split(":")[-1]


def flatten_nested_fields(
    prefix: str,
    nested: Iterable[dict[str, Any]],
    *,
    required: bool,
    is_array_field: bool,
) -> list[dict[str, Any]]:
    fields = []
    for field in nested:
        fields.append({
            **field,
            "required": required and field.get("required", False),
            "source": f"{prefix}/{field.get('source') or field['name']}",
            **({"array": True} if is_array_field else {}),
        })
    return fields


def collect_embedded_schema_types(
    schemas,
    *,
    xsd_ns: str = XSD_NS,
    datatype_overrides: Mapping[str, str] | None = None,
    flatten_nested_handler=None,
    dataset_name: str = "",
    schema_source_path: str | None = None,
    schema_diagnostics: list[Exception] | None = None,
    tolerate_schema_errors: bool = False,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, dict[str, Any]]]:
    del xsd_ns

    schema_list = list(schemas)
    if not schema_list:
        return {}, {}

    element_types: dict[str, list[dict[str, Any]]] = {}
    raw_schema_models: dict[str, dict[str, Any]] = {}
    visited_referenced_sources: set[str] = set()
    with TemporaryDirectory() as tmpdir:
        for index, schema in enumerate(schema_list):
            diagnostics_before = len(schema_diagnostics or [])
            try:
                root = _schema_to_lxml_root(schema)
                _merge_embedded_element_types(
                    element_types,
                    _process_embedded_schema_root(
                        root,
                        source=schema_source_path,
                        tmpdir=tmpdir,
                        file_stem=f"embedded_{index}",
                        dataset_name=dataset_name,
                        datatype_overrides=datatype_overrides,
                        flatten_nested_handler=flatten_nested_handler,
                        schema_diagnostics=schema_diagnostics,
                        tolerate_schema_errors=tolerate_schema_errors,
                        raw_schema_models=raw_schema_models,
                    ),
                    path=schema_source_path,
                )
                if schema_source_path:
                    for referenced_index, (referenced_root, referenced_source) in enumerate(
                        _iter_referenced_schema_roots(
                            root,
                            schema_source_path,
                            diagnostics=schema_diagnostics,
                            tolerate_errors=tolerate_schema_errors,
                            visited=visited_referenced_sources,
                        )
                    ):
                        _merge_embedded_element_types(
                            element_types,
                            _process_embedded_schema_root(
                                referenced_root,
                                source=referenced_source,
                                tmpdir=tmpdir,
                                file_stem=f"embedded_{index}_referenced_{referenced_index}",
                                dataset_name=dataset_name,
                                datatype_overrides=datatype_overrides,
                                flatten_nested_handler=flatten_nested_handler,
                                schema_diagnostics=schema_diagnostics,
                                tolerate_schema_errors=tolerate_schema_errors,
                                raw_schema_models=raw_schema_models,
                            ),
                            path=referenced_source,
                        )
            except KeyError:
                if not tolerate_schema_errors or len(schema_diagnostics or []) == diagnostics_before:
                    raise
                continue

    return element_types, raw_schema_models


def flatten_embedded_nested_fields(
    prefix: str,
    nested: list[dict[str, Any]],
    required: bool,
    is_array: bool,
) -> list[dict[str, Any]]:
    fields = []
    for field in flatten_nested_fields(
        prefix,
        nested,
        required=required,
        is_array_field=is_array,
    ):
        fields.append({**field, "name": to_property_name(f"{prefix}_{field['name']}")})
    return fields


def _schema_to_bytes(schema) -> bytes:
    if isinstance(schema, etree._Element):
        return etree.tostring(schema, encoding="utf-8")
    return xml_etree.tostring(schema, encoding="utf-8")


def _schema_to_lxml_root(schema) -> etree._Element:
    if isinstance(schema, etree._Element):
        return etree.fromstring(etree.tostring(schema, encoding="utf-8"))
    return etree.fromstring(_schema_to_bytes(schema))


def _process_embedded_schema_root(
    root: etree._Element,
    *,
    source: str | None,
    tmpdir: str,
    file_stem: str,
    dataset_name: str,
    datatype_overrides: Mapping[str, str] | None,
    flatten_nested_handler,
    schema_diagnostics: list[Exception] | None,
    tolerate_schema_errors: bool,
    raw_schema_models: dict[str, dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    target_namespace = root.attrib.get("targetNamespace")
    allowed_element_names = _schema_top_level_element_names(
        root,
        target_namespace=target_namespace,
        path=source,
    )
    composed_root = root
    if source:
        composed_root = compose_schema_tree(
            root,
            source,
            tolerate_errors=tolerate_schema_errors,
            diagnostics=schema_diagnostics,
        )

    path = os.path.join(tmpdir, f"{file_stem}.xsd")
    with open(path, "wb") as file:
        file.write(etree.tostring(composed_root, encoding="utf-8"))

    reader = XSDReader(path, dataset_name, assign_nested_partial_source=False)
    reader.start()
    raw_schema_models.update(_reader_to_raw_schema_models(reader))

    element_types: dict[str, list[dict[str, Any]]] = {}
    for element_name in allowed_element_names:
        filtered_root = _filter_schema_for_top_level_element(root, composed_root, element_name)
        filtered_path = os.path.join(tmpdir, f"{file_stem}_{element_name}.xsd")
        with open(filtered_path, "wb") as file:
            file.write(etree.tostring(filtered_root, encoding="utf-8"))

        filtered_reader = XSDReader(filtered_path, dataset_name, assign_nested_partial_source=False)
        filtered_reader.start()
        element_types.update(
            _reader_to_embedded_element_types(
                filtered_reader,
                source_path=source,
                target_namespace=target_namespace,
                allowed_element_names={element_name},
                datatype_overrides=datatype_overrides,
                flatten_nested_handler=flatten_nested_handler,
            )
        )

    return element_types


def _schema_top_level_element_names(
    root: etree._Element,
    *,
    target_namespace: str | None,
    path: str | None,
) -> set[str]:
    names: set[str] = set()
    for child in root:
        if isinstance(child, etree._Comment):
            continue
        if QName(child).localname != "element":
            continue
        name = child.attrib.get("name")
        if name:
            if name in names:
                raise_duplicate_wsdl_qname_conflict(
                    path=_wsdl_ambiguity_path(path),
                    qname=_expanded_wsdl_qname(name, target_namespace),
                    scope="wsdl:types/xs:element",
                )
            names.add(name)
    return names


def _merge_embedded_element_types(
    current: dict[str, list[dict[str, Any]]],
    incoming: dict[str, list[dict[str, Any]]],
    *,
    path: str | None,
) -> None:
    for qname, fields in incoming.items():
        if qname in current:
            raise_duplicate_wsdl_qname_conflict(
                path=_wsdl_ambiguity_path(path),
                qname=qname,
                scope="wsdl:types/xs:element",
            )
        current[qname] = fields


def _wsdl_ambiguity_path(path: str | None) -> str:
    if not path:
        return "<embedded schema>"
    return normalize_schema_source(path)


def _filter_schema_for_top_level_element(
    original_root: etree._Element,
    composed_root: etree._Element,
    element_name: str,
) -> etree._Element:
    filtered_root = etree.Element(composed_root.tag, nsmap=composed_root.nsmap)
    for key, value in composed_root.attrib.items():
        filtered_root.set(key, value)

    direct_element = None
    for child in original_root:
        if isinstance(child, etree._Comment):
            continue
        if QName(child).localname != "element":
            continue
        if child.attrib.get("name") == element_name:
            direct_element = deepcopy(child)
            break

    for child in composed_root:
        if isinstance(child, etree._Comment):
            continue
        if QName(child).localname == "element" and child.attrib.get("name") == element_name:
            continue
        filtered_root.append(deepcopy(child))

    if direct_element is not None:
        filtered_root.append(direct_element)

    return filtered_root


def _iter_referenced_schema_roots(
    root: etree._Element,
    source: str,
    *,
    diagnostics: list[Exception] | None,
    tolerate_errors: bool,
    visited: set[str] | None = None,
) -> Iterable[tuple[etree._Element, str]]:
    if visited is None:
        visited = set()

    for child in root:
        if isinstance(child, etree._Comment):
            continue

        local_name = QName(child).localname
        if local_name not in {"include", "import"}:
            continue

        schema_location = child.attrib.get("schemaLocation")
        if not schema_location:
            continue

        referenced_source = resolve_schema_reference(source, schema_location)
        normalized_source = normalize_schema_source(referenced_source)
        if normalized_source in visited:
            continue

        try:
            referenced_root = load_schema_root(referenced_source)
        except TOLERATED_SCHEMA_ERRORS as error:
            if not tolerate_errors:
                raise
            if diagnostics is not None:
                diagnostics.append(error)
            continue

        visited.add(normalized_source)
        yield referenced_root, referenced_source
        yield from _iter_referenced_schema_roots(
            referenced_root,
            referenced_source,
            diagnostics=diagnostics,
            tolerate_errors=tolerate_errors,
            visited=visited,
        )


def _expanded_wsdl_qname(local_name: str, namespace: str | None) -> str:
    return wsdl_qname_key(
        WsdlQName(raw=local_name, local_name=local_name, namespace=namespace),
        fallback=local_name,
    )


def _reader_to_embedded_element_types(
    reader: XSDReader,
    *,
    source_path: str | None,
    target_namespace: str | None,
    allowed_element_names: set[str],
    datatype_overrides: Mapping[str, str] | None,
    flatten_nested_handler,
) -> dict[str, list[dict[str, Any]]]:
    element_types: dict[str, list[dict[str, Any]]] = {}
    entry_models: dict[str, list[XSDModel]] = {}

    for model in reader.models:
        if model.is_entry_model and model.source and model.source.startswith("/"):
            local_name = model.source[1:]
            if local_name not in allowed_element_names:
                continue
            entry_models.setdefault(local_name, []).append(model)

    for local_name, models in entry_models.items():
        if len(models) > 1:
            raise_ambiguous_wsdl_reference(
                path=_wsdl_ambiguity_path(source_path),
                qname=_expanded_wsdl_qname(local_name, target_namespace),
                scope="wsdl:types/xs:element",
                error="QName resolution matched multiple embedded schema model candidates.",
            )
        element_types[_expanded_wsdl_qname(local_name, target_namespace)] = _flatten_model_fields(
                models[0],
                datatype_overrides=datatype_overrides,
                flatten_nested_handler=flatten_nested_handler,
                visited_models=set(),
        )

    resource_model = getattr(reader, "resource_model", None)
    if resource_model is not None:
        for prop in resource_model.properties.values():
            if prop.type.name in {"ref", "backref", "ref_backref"}:
                continue
            if prop.xsd_name not in allowed_element_names:
                continue
            element_types.setdefault(
                _expanded_wsdl_qname(prop.xsd_name, target_namespace),
                [_property_to_field(prop, datatype_overrides=datatype_overrides)],
            )

    return element_types


def _reader_to_raw_schema_models(reader: XSDReader) -> dict[str, dict[str, Any]]:
    raw_schema_models: dict[str, dict[str, Any]] = {}
    deduplicator = Deduplicator("{}")

    for model in reader.models:
        schema = _normalize_raw_schema_model(model.get_data())
        schema_name = deduplicator(_schema_model_basename(schema["name"]))
        schema["name"] = schema_name
        raw_schema_models[schema_name] = schema

    return raw_schema_models


def _normalize_raw_schema_model(schema: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(schema)

    external = dict(normalized.get("external", {}))
    normalized_external: dict[str, Any] = {}
    if external.get("name"):
        normalized_external["name"] = external["name"]
    if "prepare" in external:
        normalized_external["prepare"] = external["prepare"]
    if normalized_external:
        normalized["external"] = normalized_external
    elif "external" in normalized:
        normalized.pop("external")

    properties: dict[str, dict[str, Any]] = {}
    for prop_name, prop_data in normalized.get("properties", {}).items():
        properties[prop_name] = _normalize_raw_schema_property(prop_data)
    normalized["properties"] = properties

    return normalized


def _normalize_raw_schema_property(prop_data: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(prop_data)

    if "model" in normalized:
        normalized["model"] = _schema_model_basename(normalized["model"])

    if normalized.get("type") == "array" and isinstance(normalized.get("items"), dict):
        normalized["items"] = _normalize_raw_schema_property(normalized["items"])

    return normalized


def _schema_model_basename(name: str) -> str:
    return name.split("/")[-1]


def _flatten_model_fields(
    model: XSDModel,
    *,
    datatype_overrides: Mapping[str, str] | None,
    flatten_nested_handler,
    visited_models: set[int],
) -> list[dict[str, Any]]:
    model_id = id(model)
    if model_id in visited_models:
        return []

    fields: list[dict[str, Any]] = []
    visited_models = visited_models | {model_id}
    for prop in model.properties.values():
        fields.extend(
            _flatten_property_fields(
                prop,
                datatype_overrides=datatype_overrides,
                flatten_nested_handler=flatten_nested_handler,
                visited_models=visited_models,
            )
        )
    return fields


def _flatten_property_fields(
    prop: XSDProperty,
    *,
    datatype_overrides: Mapping[str, str] | None,
    flatten_nested_handler,
    visited_models: set[int],
) -> list[dict[str, Any]]:
    if prop.type.name in {"ref", "backref", "ref_backref"} and prop.ref_model is not None:
        nested = _flatten_model_fields(
            prop.ref_model,
            datatype_overrides=datatype_overrides,
            flatten_nested_handler=flatten_nested_handler,
            visited_models=visited_models,
        )
        if not nested:
            return []

        flatten = flatten_nested_handler or _flatten_nested_fields
        return flatten(prop.xsd_name, nested, bool(prop.required), bool(prop.is_array))

    return [_property_to_field(prop, datatype_overrides=datatype_overrides)]


def _property_to_field(
    prop: XSDProperty,
    *,
    datatype_overrides: Mapping[str, str] | None,
) -> dict[str, Any]:
    field = {
        "name": prop.xsd_name,
        "type": normalize_embedded_field_type(prop.type.name, datatype_overrides),
        "required": bool(prop.required),
        "source": _normalize_embedded_field_source(prop.source),
    }
    if prop.is_array:
        field["array"] = True
    return field


def _normalize_embedded_field_source(source: str | None) -> str:
    if not source:
        return ""
    if source == "text()":
        return source
    if source.endswith("/text()"):
        return source[:-7]
    return source


def _flatten_nested_fields(
    prefix: str,
    nested: list[dict[str, Any]],
    required: bool,
    is_array: bool,
) -> list[dict[str, Any]]:
    return flatten_nested_fields(
        prefix,
        nested,
        required=required,
        is_array_field=is_array,
    )