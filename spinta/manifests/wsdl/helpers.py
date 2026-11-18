from __future__ import annotations

import logging
import os
from collections.abc import Iterator
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qs, urlparse

import requests
from lxml import etree
from lxml.etree import QName, _Element

from spinta.core.ufuncs import Expr
from spinta.exceptions import WsdlReadError
from spinta.manifests.xsd.helpers import DATATYPES_MAPPING, XSDModel, XSDProperty, XSDReader
from spinta.utils.naming import Deduplicator, to_code_name, to_dataset_name, to_model_name

log = logging.getLogger(__name__)

WSDL_11_NS = "http://schemas.xmlsoap.org/wsdl/"
WSDL_20_NS = "http://www.w3.org/ns/wsdl"
XSD_NS = "http://www.w3.org/2001/XMLSchema"
SOAP_11_NS = "http://schemas.xmlsoap.org/wsdl/soap/"
SOAP_12_NS = "http://schemas.xmlsoap.org/wsdl/soap12/"
WSDL_20_SOAP_TYPE = "http://www.w3.org/ns/wsdl/soap"

WSDL_ROOTS = {
    (WSDL_11_NS, "definitions"),
    (WSDL_20_NS, "description"),
}

REQUEST_TIMEOUT = 60


def _is_remote(path: str) -> bool:
    return urlparse(path).scheme in ("http", "https")


def _xml_parser() -> etree.XMLParser:
    # WSDL may come from untrusted sources, so external entities and network access are disabled
    return etree.XMLParser(resolve_entities=False, no_network=True, remove_comments=True)


def _root_name(path: str) -> tuple[str, str] | None:
    try:
        for _, element in etree.iterparse(path, events=("start",), resolve_entities=False, no_network=True):
            name = QName(element)
            return name.namespace, name.localname
    except (OSError, etree.XMLSyntaxError):
        return None
    return None


def is_wsdl_path(path: str) -> bool:
    if _is_remote(path):
        parsed = urlparse(path)
        query = {key.lower() for key in parse_qs(parsed.query, keep_blank_values=True)}
        return "wsdl" in query or parsed.path.lower().endswith(".wsdl")

    lower_path = path.lower()
    if lower_path.endswith(".wsdl"):
        return True
    # plain XML files are WSDL only if their root element says so
    if lower_path.endswith(".xml") and os.path.isfile(path):
        return _root_name(path) in WSDL_ROOTS
    return False


def _read_wsdl_root(path: str) -> _Element:
    try:
        if _is_remote(path):
            response = requests.get(path, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            content = response.content
        else:
            with open(path, "rb") as file:
                content = file.read()
        root = etree.fromstring(content, _xml_parser())
    except (OSError, requests.RequestException, etree.XMLSyntaxError) as e:
        raise WsdlReadError(path=path, error=str(e))

    name = QName(root)
    if (name.namespace, name.localname) not in WSDL_ROOTS:
        raise WsdlReadError(
            path=path,
            error=f"root element {root.tag!r} is neither WSDL 1.1 'definitions' nor WSDL 2.0 'description'",
        )
    return root


def _local_name(qname: str | None) -> str | None:
    if not qname or qname.startswith("#"):
        # WSDL 2.0 uses "#any", "#none" and "#other" instead of element names
        return None
    return qname.split(":")[-1]


@dataclass
class _Part:
    name: str | None
    element: str | None = None
    type: str | None = None


@dataclass
class _Operation:
    service: str
    port: str
    port_type: str
    name: str
    input: list[_Part] = field(default_factory=list)
    output: list[_Part] = field(default_factory=list)

    @property
    def source(self) -> str:
        # format expected by the `soap` backend: service.port.portType.operation
        return f"{self.service}.{self.port}.{self.port_type}.{self.name}"


def _is_soap_11_binding(binding: _Element) -> bool:
    for child in binding:
        name = QName(child)
        if name.localname == "binding" and name.namespace in (SOAP_11_NS, SOAP_12_NS):
            return True
    return False


def _wsdl_11_operations(root: _Element) -> Iterator[_Operation]:
    ns = {"w": WSDL_11_NS}
    messages = {
        message.get("name"): [
            _Part(part.get("name"), _local_name(part.get("element")), _local_name(part.get("type")))
            for part in message.findall("w:part", ns)
        ]
        for message in root.findall("w:message", ns)
    }
    port_types = {
        port_type.get("name"): {operation.get("name"): operation for operation in port_type.findall("w:operation", ns)}
        for port_type in root.findall("w:portType", ns)
    }
    bindings = {binding.get("name"): binding for binding in root.findall("w:binding", ns)}

    def message_parts(operation: _Element, direction: str) -> list[_Part]:
        message = operation.find(f"w:{direction}", ns)
        if message is None:
            return []
        return messages.get(_local_name(message.get("message")), [])

    for service in root.findall("w:service", ns):
        for port in service.findall("w:port", ns):
            binding = bindings.get(_local_name(port.get("binding")))
            if binding is None or not _is_soap_11_binding(binding):
                log.warning(f"Skipping WSDL port {port.get('name')!r}, it does not have a SOAP binding.")
                continue
            port_type = _local_name(binding.get("type"))
            operations = port_types.get(port_type, {})
            for binding_operation in binding.findall("w:operation", ns):
                operation = operations.get(binding_operation.get("name"))
                if operation is None:
                    continue
                yield _Operation(
                    service=service.get("name"),
                    port=port.get("name"),
                    port_type=port_type,
                    name=operation.get("name"),
                    input=message_parts(operation, "input"),
                    output=message_parts(operation, "output"),
                )


def _wsdl_20_operations(root: _Element) -> Iterator[_Operation]:
    ns = {"w": WSDL_20_NS}
    interfaces = {
        interface.get("name"): interface.findall("w:operation", ns) for interface in root.findall("w:interface", ns)
    }
    bindings = {binding.get("name"): binding for binding in root.findall("w:binding", ns)}

    def element_parts(operation: _Element, direction: str) -> list[_Part]:
        parts = []
        for message in operation.findall(f"w:{direction}", ns):
            if element := _local_name(message.get("element")):
                parts.append(_Part(name=None, element=element))
        return parts

    for service in root.findall("w:service", ns):
        for endpoint in service.findall("w:endpoint", ns):
            binding = bindings.get(_local_name(endpoint.get("binding")))
            if binding is None or binding.get("type") != WSDL_20_SOAP_TYPE:
                log.warning(f"Skipping WSDL endpoint {endpoint.get('name')!r}, it does not have a SOAP binding.")
                continue
            interface = _local_name(binding.get("interface") or service.get("interface"))
            for operation in interfaces.get(interface, []):
                yield _Operation(
                    service=service.get("name"),
                    port=endpoint.get("name"),
                    port_type=interface,
                    name=operation.get("name"),
                    input=element_parts(operation, "input"),
                    output=element_parts(operation, "output"),
                )


def _iter_operations(root: _Element) -> Iterator[_Operation]:
    operations = _wsdl_11_operations(root) if QName(root).namespace == WSDL_11_NS else _wsdl_20_operations(root)
    seen = set()
    for operation in operations:
        # the same operation is often exposed through several ports, e.g. SOAP 1.1 and SOAP 1.2
        key = (operation.service, operation.port_type, operation.name)
        if key not in seen:
            seen.add(key)
            yield operation


def _merge_schemas(root: _Element) -> _Element | None:
    """Joins all schemas from WSDL `types` into one, because they often reference each other's types."""
    schemas = root.findall(f"{{{QName(root).namespace}}}types/{{{XSD_NS}}}schema")
    if not schemas:
        return None

    nsmap = {}
    for element in (root, *schemas):
        for prefix, uri in element.nsmap.items():
            if prefix is not None:
                nsmap.setdefault(prefix, uri)

    merged = etree.Element(f"{{{XSD_NS}}}schema", nsmap=nsmap)
    for schema in schemas:
        for child in schema:
            if QName(child).localname in ("import", "include"):
                if child.get("schemaLocation"):
                    log.warning(f"External schema {child.get('schemaLocation')!r} is not supported, skipping it.")
                continue
            merged.append(deepcopy(child))
    return merged


class _WsdlSchemaReader(XSDReader):
    def __init__(self, schema: _Element, dataset_name: str):
        super().__init__(path="", dataset_name=dataset_name)
        self._schema = schema
        self._dataset_name = dataset_name

    def _extract_root(self) -> None:
        self.root = self._schema

    def _get_dataset_name(self) -> str:
        return self._dataset_name


def _soap_source(source: str | None) -> str:
    # XSD reader produces XPath-like sources, SOAP backend reads keys of the response dict
    source = source or ""
    return source.removesuffix("/text()").removeprefix("@")


def _is_link(prop: XSDProperty) -> bool:
    return prop.type.name in ("ref", "backref") and prop.ref_model is not None and bool(prop.source)


def _linked_models(model: XSDModel) -> Iterator[XSDModel]:
    for prop in model.properties.values():
        if _is_link(prop):
            yield prop.ref_model


def _input_fields(
    model: XSDModel, prefix: str = "", visited: frozenset = frozenset()
) -> Iterator[tuple[str, XSDProperty]]:
    """Leaf fields of a request element, with their paths in the SOAP request body."""
    for prop in model.properties.values():
        if not prop.source:
            continue
        source = f"{prefix}{_soap_source(prop.source)}"
        if _is_link(prop):
            if prop.ref_model not in visited:
                yield from _input_fields(prop.ref_model, f"{source}/", visited | {model})
        else:
            yield source, prop


def _fix_property_sources(properties: dict[str, dict[str, Any]], model_names: set[str]) -> dict[str, dict[str, Any]]:
    result = {}
    for name, prop in properties.items():
        target = prop.get("items", prop)
        # reverse refs to models which are not included (e.g. request models) are dropped
        if target.get("model") and target["model"] not in model_names:
            continue
        target["external"]["name"] = _soap_source(target["external"].get("name"))
        result[name] = prop
    return result


def _dataset_name(root: _Element, path: str, given_name: str | None) -> str:
    if given_name:
        return given_name
    if _is_remote(path):
        name = root.get("name") or urlparse(path).path
    else:
        name = os.path.splitext(os.path.basename(path))[0]
    return to_dataset_name(name) or "wsdl"


def _part_type(part: _Part) -> str | None:
    if part.type and part.type in DATATYPES_MAPPING:
        return DATATYPES_MAPPING[part.type].split(";")[0]
    return None


def read_schema(path: str, dataset_name: str | None = None) -> Iterator[tuple[None, dict[str, Any]]]:
    root = _read_wsdl_root(path)
    dataset_name = _dataset_name(root, path, dataset_name)

    reader = None
    if (schema := _merge_schemas(root)) is not None:
        reader = _WsdlSchemaReader(schema, dataset_name)
        reader.start()
    element_models: dict[str, list[XSDModel]] = {}
    for model in reader.models if reader else []:
        if model.source.startswith("/"):
            element_models.setdefault(model.source[1:], []).append(model)
    model_names = reader.deduplicate_model_name if reader else Deduplicator()

    resource_names = Deduplicator()
    wsdl_resource = resource_names("wsdl")
    resources: dict[str, dict[str, Any]] = {wsdl_resource: {"type": "wsdl", "external": path}}
    # id(model) -> (model, resource name of the first operation which uses it)
    emitted: dict[int, tuple[XSDModel, str]] = {}
    extra_models: list[dict[str, Any]] = []
    params_by_model: dict[int, list[tuple[str, XSDProperty | _Part]]] = {}

    for operation in _iter_operations(root):
        resource = resource_names(to_code_name(operation.name))

        params: dict[str, dict[str, Any]] = {}
        param_fields: list[tuple[str, XSDProperty | _Part]] = []
        param_names = Deduplicator()
        for part in operation.input:
            if part.element:
                fields = [
                    field_ for model in element_models.get(part.element, [])[:1] for field_ in _input_fields(model)
                ]
            elif _part_type(part):
                fields = [(part.name, part)]
            else:
                log.warning(f"Skipping input part {part.name!r} of WSDL operation {operation.name!r}.")
                fields = []
            for source, field_ in fields:
                name = param_names(to_code_name(source.split("/")[-1]))
                params[name] = {"name": name, "source": [source], "prepare": [Expr("input")], "type": "param"}
                param_fields.append((name, field_))

        resources[resource] = {
            "type": "soap",
            "external": operation.source,
            "prepare": Expr("wsdl", Expr("bind", wsdl_resource)),
            "title": operation.name,
            "params": params,
        }

        response_models = [model for part in operation.output for model in element_models.get(part.element, [])]
        if not response_models:
            # no schema for the response, so the model only has request parameters
            extra_models.append(
                {
                    "type": "model",
                    "name": f"{dataset_name}/{model_names(to_model_name(operation.name))}",
                    "external": {"dataset": dataset_name, "resource": resource, "name": "/"},
                    "properties": _param_properties(param_fields, set()),
                }
            )
            continue

        stack = list(response_models)
        while stack:
            model = stack.pop()
            if id(model) in emitted:
                continue
            emitted[id(model)] = (model, resource)
            stack.extend(_linked_models(model))
        for model in response_models:
            model.source = "/"
            params_by_model[id(model)] = param_fields

    yield (
        None,
        {
            "type": "dataset",
            "name": dataset_name,
            "title": root.get("name") or "",
            "resources": resources,
        },
    )

    names = {model.name for model, _ in emitted.values()}
    models = list(extra_models)
    for model, resource in emitted.values():
        data = model.get_data()
        data["external"]["resource"] = resource
        data["properties"] = _fix_property_sources(data["properties"], names)
        if id(model) in params_by_model:
            data["properties"].update(_param_properties(params_by_model[id(model)], set(data["properties"])))
        models.append(data)

    resource_order = {name: i for i, name in enumerate(resources)}
    # models are grouped by resource, otherwise tabular output repeats resource rows
    for data in sorted(models, key=lambda data: (resource_order[data["external"]["resource"]], data["name"])):
        yield None, data


def _param_properties(param_fields: list[tuple[str, XSDProperty | _Part]], existing: set[str]) -> dict[str, Any]:
    """Request parameters are also added to the model, so that they are visible in the returned data."""
    names = Deduplicator()
    for name in existing:
        names(name)
    properties = {}
    for param, field_ in param_fields:
        if isinstance(field_, _Part):
            type_ = _part_type(field_)
        else:
            type_ = field_.type.name
        properties[names(param)] = {
            "type": type_,
            "external": {"name": "", "prepare": Expr("param", Expr("bind", param))},
        }
    return properties
