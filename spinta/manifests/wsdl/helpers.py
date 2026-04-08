from __future__ import annotations

from dataclasses import dataclass
import pathlib
from typing import Any, Callable, Iterable, Iterator, Optional, cast
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlparse
import urllib.request
import xml.etree.ElementTree as ET

from spinta import commands
from spinta import exceptions
from spinta.core.ufuncs import Bind, Expr
from spinta.manifests.components import Manifest
from spinta.manifests.components import ManifestSchema
from spinta.manifests.wsdl.xml_helpers import tag_namespace
from spinta.utils.naming import Deduplicator, to_code_name, to_dataset_name, to_model_name, to_property_name
from spinta.utils.schema import NA

from .qname import WsdlQName, build_wsdl_namespace_map, resolve_wsdl_qname, resolve_wsdl_qname_in_scope, wsdl_qname_key, wsdl_qname_local_name
from .document.helpers import (
    BindingInfo,
    MessageInfo,
    ServicePortInfo,
    WsdlNamespaceContext,
    collect_bindings,
    collect_messages,
    collect_services,
    collect_wsdl_2_0_bindings,
    collect_wsdl_2_0_services,
    message_info,
)
from .soap.helpers import resolve_ports_for_port_type
from spinta.manifests.wsdl.xsd.datatypes import get_wsdl_datatype_overrides
from .xsd.helpers import (
    collect_embedded_schema_types,
    flatten_embedded_nested_fields,
    local_name,
)

WSDL_FILE_PREFIX = "wsdl+file://"
WSDL_HTTP_PREFIXES = ("wsdl+http://", "wsdl+https://")
WSDL_PREFIX = "wsdl:"
WSDL_QUERY_MARKER = "wsdl"
WSDL_1_1_NS = "http://schemas.xmlsoap.org/wsdl/"
WSDL_2_0_NS = "http://www.w3.org/ns/wsdl"
XSD_NS = "http://www.w3.org/2001/XMLSchema"
WSDL_VERSIONS = {
    WSDL_1_1_NS: "1.1",
    WSDL_2_0_NS: "2.0",
}

RAW_SCHEMA_NAMESPACE = "schema"


class _XmlNamespaceScopeRegistry:
    def __init__(self) -> None:
        self._scopes_by_root_id: dict[int, dict[int, dict[str, str]]] = {}

    def remember(self, root: ET.Element, scopes: dict[int, dict[str, str]]) -> None:
        self._scopes_by_root_id[id(root)] = scopes

    def scopes_for(self, root: ET.Element) -> dict[int, dict[str, str]]:
        return self._scopes_by_root_id.get(id(root), {})


_xml_namespace_scope_registry = _XmlNamespaceScopeRegistry()


@dataclass
class WsdlOperationMessage:
    suffix: str
    message: MessageInfo | None


@dataclass
class WsdlOperationLink:
    operation_name: str
    operation_qname: WsdlQName | None
    interface_name: str
    interface_qname: WsdlQName | None
    service_name: str
    port_name: str
    resource_key: str
    resource: dict[str, Any]
    messages: list[WsdlOperationMessage]


@dataclass
class WsdlReadContext:
    version: str
    normalized_path: str
    definitions_name: str
    dataset: str
    dataset_name: str | None
    target_namespace: str
    namespace_map: dict[str, str]
    resource_name: str
    prefixes: dict[str, str]
    schemas: list[ET.Element]
    element_types: dict[str, list[dict[str, Any]]]
    raw_schema_models: dict[str, dict[str, Any]]
    schema_diagnostics: list[Exception]
    messages: dict[str, MessageInfo]
    bindings: dict[str, BindingInfo]
    services: dict[str, dict[str, ServicePortInfo]]
    resources: dict[str, dict[str, Any]]
    operation_links: list[WsdlOperationLink]


@dataclass
class WsdlPreparedReadState:
    definitions_name: str
    dataset: str
    target_namespace: str
    namespace_map: dict[str, str]
    namespace_context: WsdlNamespaceContext
    resource_name: str
    schemas: list[ET.Element]
    element_types: dict[str, list[dict[str, Any]]]
    raw_schema_models: dict[str, dict[str, Any]]
    schema_diagnostics: list[Exception]
    resources: dict[str, dict[str, Any]]


@dataclass
class WsdlCollectedArtifacts:
    messages: dict[str, MessageInfo]
    bindings: dict[str, BindingInfo]
    services: dict[str, dict[str, ServicePortInfo]]
    operation_links: list[WsdlOperationLink]


def is_wsdl_path(path: str) -> bool:
    if not path:
        return False

    if path.startswith(WSDL_FILE_PREFIX) or path.startswith(WSDL_HTTP_PREFIXES) or path.startswith(WSDL_PREFIX):
        return True

    if _is_remote_url(path):
        parsed = urlparse(path)
        query_keys = {key.lower() for key, _ in parse_qsl(parsed.query, keep_blank_values=True)}
        return path.endswith(".wsdl") or WSDL_QUERY_MARKER in query_keys or parsed.query.lower() == WSDL_QUERY_MARKER

    lower_path = path.lower()
    return lower_path.endswith(".wsdl") or lower_path.endswith("?wsdl")


def normalize_wsdl_path(path: str) -> str:
    if path.startswith(WSDL_FILE_PREFIX):
        return path[len(WSDL_FILE_PREFIX):]
    if path.startswith("wsdl+http://"):
        return "http://" + path[len("wsdl+http://"):]
    if path.startswith("wsdl+https://"):
        return "https://" + path[len("wsdl+https://"):]
    if path.startswith(WSDL_PREFIX):
        return path[len(WSDL_PREFIX):]
    if _is_remote_url(path):
        return path
    if path.lower().endswith("?wsdl"):
        return path[:-5]
    return path


def read_wsdl_document_version(path: str) -> str:
    normalized_path = normalize_wsdl_path(path)
    root, prefixes = _read_xml(normalized_path)
    return detect_wsdl_document_version(root, prefixes, path=normalized_path)


def read_schema(
    context,
    manifest: Manifest,
    path: str,
    dataset_name: str | None = None,
) -> Iterator[ManifestSchema]:
    """Yield WSDL manifest schema dictionaries for generic manifest loading.

    The WSDL reader follows the same contract as other manifest readers in the
    codebase: it yields ``(eid, schema_dict)`` items and leaves runtime node
    materialization to ``load_manifest_nodes``.
    """
    normalized_path = normalize_wsdl_path(path)
    root, prefixes = _read_xml(normalized_path)

    version = detect_wsdl_document_version(root, prefixes, path=normalized_path)
    if version in {"1.1", "2.0"}:
        wsdl_context = _extract_wsdl_read_context(
            context,
            manifest,
            normalized_path,
            dataset_name,
            root,
            prefixes,
            version=version,
        )
        commands.store_schema_diagnostics(context, manifest, path, wsdl_context.schema_diagnostics)
        yield from _iter_wsdl_manifest_schemas(wsdl_context)
        return

    raise exceptions.UnsupportedWsdlVersion(
        path=normalized_path,
        namespace=tag_namespace(root.tag) or "",
    )


def detect_wsdl_document_version(
    root: ET.Element,
    prefixes: dict[str, str],
    *,
    path: str,
) -> str:
    namespace = tag_namespace(root.tag)
    if namespace in WSDL_VERSIONS:
        return WSDL_VERSIONS[namespace]

    namespace_candidates = set(prefixes.values())
    if WSDL_1_1_NS in namespace_candidates:
        return WSDL_VERSIONS[WSDL_1_1_NS]
    if WSDL_2_0_NS in namespace_candidates:
        return WSDL_VERSIONS[WSDL_2_0_NS]

    raise exceptions.UnsupportedWsdlVersion(
        path=path,
        namespace=namespace or "",
    )


def _extract_wsdl_read_context(
    context,
    manifest: Manifest,
    normalized_path: str,
    dataset_name: str | None,
    root: ET.Element,
    prefixes: dict[str, str],
    *,
    version: str,
) -> WsdlReadContext:
    del context
    prepared = _prepare_wsdl_read_state(
        manifest,
        normalized_path,
        dataset_name,
        root,
        prefixes,
    )
    collected = _collect_wsdl_artifacts(
        root,
        version=version,
        prepared=prepared,
    )

    return WsdlReadContext(
        version=version,
        normalized_path=normalized_path,
        definitions_name=prepared.definitions_name,
        dataset=prepared.dataset,
        dataset_name=dataset_name,
        target_namespace=prepared.target_namespace,
        namespace_map=prepared.namespace_map,
        resource_name=prepared.resource_name,
        prefixes=prefixes,
        schemas=prepared.schemas,
        element_types=prepared.element_types,
        raw_schema_models=prepared.raw_schema_models,
        schema_diagnostics=prepared.schema_diagnostics,
        messages=collected.messages,
        bindings=collected.bindings,
        services=collected.services,
        resources=prepared.resources,
        operation_links=collected.operation_links,
    )


def _prepare_wsdl_read_state(
    manifest: Manifest,
    normalized_path: str,
    dataset_name: str | None,
    root: ET.Element,
    prefixes: dict[str, str],
) -> WsdlPreparedReadState:
    definitions_name = root.get("name") or pathlib.Path(urlparse(normalized_path).path).stem or "service"
    dataset = dataset_name or f"services/{to_dataset_name(definitions_name)}"
    target_namespace = root.get("targetNamespace") or ""
    namespace_map = build_wsdl_namespace_map(prefixes)
    namespace_context = WsdlNamespaceContext(
        path=normalized_path,
        target_namespace=target_namespace,
        namespace_map=namespace_map,
        namespace_scopes=_namespace_scopes_for_root(root),
    )
    resource_name = "contract"
    schemas = root.findall(f".//{{{XSD_NS}}}schema")
    element_types, raw_schema_models, schema_diagnostics = _collect_xsd_types(
        manifest,
        schemas,
        dataset_name=dataset_name,
        schema_source_path=normalized_path,
    )
    return WsdlPreparedReadState(
        definitions_name=definitions_name,
        dataset=dataset,
        target_namespace=target_namespace,
        namespace_map=namespace_map,
        namespace_context=namespace_context,
        resource_name=resource_name,
        schemas=schemas,
        element_types=element_types,
        raw_schema_models=raw_schema_models,
        schema_diagnostics=schema_diagnostics,
        resources=_build_wsdl_root_resource(normalized_path, resource_name, definitions_name),
    )


def _build_wsdl_root_resource(
    normalized_path: str,
    resource_name: str,
    definitions_name: str,
) -> dict[str, dict[str, Any]]:
    return {
        resource_name: {
            "type": "wsdl",
            "external": normalized_path,
            "title": definitions_name,
        }
    }


def _collect_wsdl_artifacts(
    root: ET.Element,
    *,
    version: str,
    prepared: WsdlPreparedReadState,
) -> WsdlCollectedArtifacts:
    bindings_collector: Callable[..., dict[str, BindingInfo]]
    services_collector: Callable[..., dict[str, dict[str, ServicePortInfo]]]
    operation_links_collector: Callable[..., list[WsdlOperationLink]]
    collector_kwargs: dict[str, Any]
    operation_link_kwargs: dict[str, Any]

    if version == "1.1":
        messages = collect_messages(
            root,
            wsdl_ns=WSDL_1_1_NS,
            element_types=prepared.element_types,
            namespace_context=prepared.namespace_context,
        )
        bindings_collector = collect_bindings
        services_collector = collect_services
        operation_links_collector = _collect_wsdl_1_1_operation_links
        collector_kwargs = {
            "wsdl_ns": WSDL_1_1_NS,
            "namespace_context": prepared.namespace_context,
        }
        operation_link_kwargs = {"messages": messages}
    elif version == "2.0":
        messages = {}
        bindings_collector = collect_wsdl_2_0_bindings
        services_collector = collect_wsdl_2_0_services
        operation_links_collector = _collect_wsdl_2_0_operation_links
        collector_kwargs = {"namespace_context": prepared.namespace_context}
        operation_link_kwargs = {"element_types": prepared.element_types}
    else:
        raise exceptions.UnsupportedWsdlVersion(
            path=prepared.namespace_context.path,
            namespace=version,
        )

    bindings = bindings_collector(root, **collector_kwargs)
    services = services_collector(root, **collector_kwargs)
    operation_links = operation_links_collector(
        root,
        resource_name=prepared.resource_name,
        bindings=bindings,
        services=services,
        resources=prepared.resources,
        namespace_context=prepared.namespace_context,
        **operation_link_kwargs,
    )
    return WsdlCollectedArtifacts(
        messages=messages,
        bindings=bindings,
        services=services,
        operation_links=operation_links,
    )


def _build_wsdl_dataset_manifest_schema(wsdl_context: WsdlReadContext) -> dict[str, Any]:
    return {
        "type": "dataset",
        "name": wsdl_context.dataset,
        "title": wsdl_context.definitions_name,
        "prefixes": {
            key: {"type": "prefix", "name": key, "uri": value, "eid": index}
            for index, (key, value) in enumerate(wsdl_context.prefixes.items())
        },
        "resources": wsdl_context.resources,
        "given_name": wsdl_context.dataset_name,
    }


def _build_operation_message_manifest_schema(
    wsdl_context: WsdlReadContext,
    operation_link: WsdlOperationLink,
    operation_message: WsdlOperationMessage,
    *,
    model_name: str,
) -> dict[str, Any]:
    message = cast(MessageInfo, operation_message.message)
    message_local_name = wsdl_qname_local_name(
        message.element_qname,
        fallback=wsdl_qname_local_name(
            message.type_qname,
            fallback=operation_message.suffix.lower(),
        ),
    )
    return {
        "type": "model",
        "name": f"{wsdl_context.dataset}/{model_name}",
        "title": f"{operation_link.operation_name} {operation_message.suffix}",
        "external": {
            "dataset": wsdl_context.dataset,
            "resource": operation_link.resource_key,
            "name": message_local_name,
        },
        "properties": _fields_to_properties(message.fields or []),
    }


def _iter_operation_message_manifest_schemas(
    wsdl_context: WsdlReadContext,
    *,
    model_deduplicator: Deduplicator,
) -> Iterator[ManifestSchema]:
    for operation_link in wsdl_context.operation_links:
        for operation_message in operation_link.messages:
            if operation_message.message is None:
                continue

            model_name = model_deduplicator(to_model_name(f"{operation_link.operation_name}{operation_message.suffix}"))
            yield None, _build_operation_message_manifest_schema(
                wsdl_context,
                operation_link,
                operation_message,
                model_name=model_name,
            )


def _resolve_wsdl_service_binding(
    services: dict[str, dict[str, ServicePortInfo]],
    bindings: dict[str, BindingInfo],
    namespace_context: WsdlNamespaceContext,
    *,
    service_name: str,
    port_name: str,
) -> tuple[ServicePortInfo | None, BindingInfo | None]:
    service_qname = namespace_context.resolve(service_name)
    port_qname = namespace_context.resolve(port_name)
    port_info = services.get(wsdl_qname_key(service_qname, fallback=service_name), {}).get(
        wsdl_qname_key(port_qname, fallback=port_name)
    )
    binding_info = bindings.get(wsdl_qname_key(port_info.binding_qname)) if port_info else None
    return port_info, binding_info


def _append_wsdl_operation_link(
    operation_links: list[WsdlOperationLink],
    resources: dict[str, dict[str, Any]],
    resource_deduplicator: Deduplicator,
    *,
    resource_name: str,
    service_name: str,
    port_name: str,
    interface_name: str,
    operation_name: str,
    operation_qname: WsdlQName | None,
    interface_qname: WsdlQName | None,
    binding_info: BindingInfo | None,
    port_info: ServicePortInfo | None,
    messages: list[WsdlOperationMessage],
) -> None:
    resource_key = resource_deduplicator(to_code_name(f"{port_name}_{operation_name}"))
    resource = _build_operation_resource(
        resource_name=resource_name,
        binding_info=binding_info,
        port_info=port_info,
        operation_name=operation_name,
        soap_source=".".join([service_name, port_name, interface_name, operation_name]),
    )
    resources[resource_key] = resource
    operation_links.append(
        WsdlOperationLink(
            operation_name=operation_name,
            operation_qname=operation_qname,
            interface_name=interface_name,
            interface_qname=interface_qname,
            service_name=service_name,
            port_name=port_name,
            resource_key=resource_key,
            resource=resource,
            messages=messages,
        )
    )


def _iter_wsdl_manifest_schemas(wsdl_context: WsdlReadContext) -> Iterator[ManifestSchema]:
    yield None, _build_wsdl_dataset_manifest_schema(wsdl_context)

    yield from _iter_raw_schema_manifest_schemas(wsdl_context)

    model_deduplicator = Deduplicator("{}")
    yield from _iter_operation_message_manifest_schemas(
        wsdl_context,
        model_deduplicator=model_deduplicator,
    )


def _iter_raw_schema_manifest_schemas(wsdl_context: WsdlReadContext) -> Iterator[ManifestSchema]:
    for schema_name, raw_schema_model in wsdl_context.raw_schema_models.items():
        yield None, _build_raw_schema_manifest_schema(wsdl_context, schema_name, raw_schema_model)


def _build_raw_schema_manifest_schema(
    wsdl_context: WsdlReadContext,
    schema_name: str,
    raw_schema_model: dict[str, Any],
) -> dict[str, Any]:
    external = dict(raw_schema_model.get("external", {}))
    external["dataset"] = wsdl_context.dataset
    external["resource"] = wsdl_context.resource_name

    model_schema = {
        "type": "model",
        "name": _qualify_raw_schema_model_name(wsdl_context.dataset, schema_name),
        "given_name": schema_name,
        "external": external,
        "properties": _qualify_raw_schema_properties(
            raw_schema_model.get("properties", {}),
            dataset=wsdl_context.dataset,
        ),
    }
    model_schema.update({
        key: value
        for key, value in raw_schema_model.items()
        if key not in {"type", "name", "external", "properties"}
    })
    return model_schema


def _qualify_raw_schema_properties(
    properties: dict[str, dict[str, Any]],
    *,
    dataset: str,
) -> dict[str, dict[str, Any]]:
    return {
        prop_name: _qualify_raw_schema_property(prop_schema, dataset=dataset)
        for prop_name, prop_schema in properties.items()
    }


def _qualify_raw_schema_property(
    prop_schema: dict[str, Any],
    *,
    dataset: str,
) -> dict[str, Any]:
    qualified = dict(prop_schema)

    if "model" in qualified:
        qualified["model"] = _qualify_raw_schema_model_name(dataset, qualified["model"])

    if qualified.get("type") == "array" and isinstance(qualified.get("items"), dict):
        qualified["items"] = _qualify_raw_schema_property(qualified["items"], dataset=dataset)

    return qualified


def _qualify_raw_schema_model_name(dataset: str, model_name: str) -> str:
    return f"{dataset}/{RAW_SCHEMA_NAMESPACE}/{model_name}"


def _collect_wsdl_1_1_operation_links(
    root: ET.Element,
    *,
    resource_name: str,
    messages: dict[str, MessageInfo],
    bindings: dict[str, BindingInfo],
    services: dict[str, dict[str, ServicePortInfo]],
    resources: dict[str, dict[str, Any]],
    namespace_context: WsdlNamespaceContext,
) -> list[WsdlOperationLink]:
    def resolve_operation(operation: ET.Element) -> tuple[str, WsdlQName | None, list[WsdlOperationMessage]]:
        operation_name = _local_name(operation.get("name"))
        operation_qname = namespace_context.resolve_in_scope(operation.get("name"), node=operation)
        input_message = message_info(
            operation.find(f"{{{WSDL_1_1_NS}}}input"),
            messages,
            namespace_context=namespace_context,
        )
        output_message = message_info(
            operation.find(f"{{{WSDL_1_1_NS}}}output"),
            messages,
            namespace_context=namespace_context,
        )
        return operation_name, operation_qname, [
            WsdlOperationMessage("Request", input_message),
            WsdlOperationMessage("Response", output_message),
        ]

    return _collect_wsdl_operation_links_for_interfaces(
        root.findall(f".//{{{WSDL_1_1_NS}}}portType"),
        operation_tag=f"{{{WSDL_1_1_NS}}}operation",
        resource_name=resource_name,
        bindings=bindings,
        services=services,
        resources=resources,
        namespace_context=namespace_context,
        resolve_operation=resolve_operation,
    )


def _collect_wsdl_operation_links_for_interfaces(
    interfaces: Iterable[ET.Element],
    *,
    operation_tag: str,
    resource_name: str,
    bindings: dict[str, BindingInfo],
    services: dict[str, dict[str, ServicePortInfo]],
    resources: dict[str, dict[str, Any]],
    namespace_context: WsdlNamespaceContext,
    resolve_operation: Callable[[ET.Element], tuple[str, WsdlQName | None, list[WsdlOperationMessage]]],
) -> list[WsdlOperationLink]:
    operation_links: list[WsdlOperationLink] = []
    resource_deduplicator = Deduplicator("_{}")

    for interface in interfaces:
        interface_name = _local_name(interface.get("name"))
        interface_qname = namespace_context.resolve_in_scope(interface.get("name"), node=interface)
        for operation in interface.findall(operation_tag):
            operation_name, operation_qname, operation_messages = resolve_operation(operation)
            for service_name, port_name in resolve_ports_for_port_type(services, bindings, interface_qname, interface_name):
                port_info, binding_info = _resolve_wsdl_service_binding(
                    services,
                    bindings,
                    namespace_context,
                    service_name=service_name,
                    port_name=port_name,
                )
                _append_wsdl_operation_link(
                    operation_links,
                    resources,
                    resource_deduplicator,
                    resource_name=resource_name,
                    service_name=service_name,
                    port_name=port_name,
                    interface_name=interface_name,
                    operation_name=operation_name,
                    operation_qname=operation_qname,
                    interface_qname=interface_qname,
                    binding_info=binding_info,
                    port_info=port_info,
                    messages=operation_messages,
                )

    return operation_links


def _collect_wsdl_2_0_operation_links(
    root: ET.Element,
    *,
    resource_name: str,
    element_types: dict[str, list[dict[str, Any]]],
    bindings: dict[str, BindingInfo],
    services: dict[str, dict[str, ServicePortInfo]],
    resources: dict[str, dict[str, Any]],
    namespace_context: WsdlNamespaceContext,
) -> list[WsdlOperationLink]:
    def resolve_operation(operation: ET.Element) -> tuple[str, WsdlQName | None, list[WsdlOperationMessage]]:
        operation_name = _local_name(operation.get("name") or operation.get("ref"))
        operation_qname = namespace_context.resolve_in_scope(
            operation.get("name") or operation.get("ref"),
            node=operation,
        )
        input_message = _message_info_from_element_or_type(
            operation.find(f"{{{WSDL_2_0_NS}}}input"),
            element_types,
            namespace_context=namespace_context,
        )
        output_message = _message_info_from_element_or_type(
            operation.find(f"{{{WSDL_2_0_NS}}}output"),
            element_types,
            namespace_context=namespace_context,
        )
        return operation_name, operation_qname, [
            WsdlOperationMessage("Request", input_message),
            WsdlOperationMessage("Response", output_message),
        ]

    return _collect_wsdl_operation_links_for_interfaces(
        root.findall(f".//{{{WSDL_2_0_NS}}}interface"),
        operation_tag=f"{{{WSDL_2_0_NS}}}operation",
        resource_name=resource_name,
        bindings=bindings,
        services=services,
        resources=resources,
        namespace_context=namespace_context,
        resolve_operation=resolve_operation,
    )


def _build_operation_resource(
    *,
    resource_name: str,
    binding_info: BindingInfo | None,
    port_info: ServicePortInfo | None,
    operation_name: str,
    soap_source: str,
) -> dict[str, Any]:
    resource = {
        "type": "soap",
        "external": soap_source,
        "prepare": Expr("wsdl", Bind(resource_name)),
        "title": operation_name,
    }
    metadata_params = _soap_metadata_to_resource_params(
        _merge_metadata(
            binding_info.params if binding_info else None,
            port_info.params if port_info else None,
            binding_info.operation_params.get(operation_name) if binding_info else None,
        )
    )
    if metadata_params:
        resource["params"] = metadata_params
    return resource


def _read_xml(path: str) -> tuple[ET.Element, dict[str, str]]:
    prefixes: dict[str, str] = {}
    element_scopes: dict[int, dict[str, str]] = {}
    xml_data = _read_path(path)
    try:
        pending_namespaces: list[tuple[str, str]] = []
        scope_stack: list[dict[str, str]] = []
        root: ET.Element | None = None
        for event, node in ET.iterparse(_iter_bytes(xml_data), events=("start", "end", "start-ns")):
            if event == "start-ns":
                prefix, uri = node
                prefixes[str(prefix or "xmlns")] = str(uri)
                pending_namespaces.append((str(prefix or ""), str(uri)))
                continue

            if event == "start":
                scope = dict(scope_stack[-1]) if scope_stack else {}
                for prefix, uri in pending_namespaces:
                    scope[prefix] = uri
                pending_namespaces.clear()
                element_scopes[id(node)] = scope
                scope_stack.append(scope)
                if root is None:
                    root = node
                continue

            if scope_stack:
                scope_stack.pop()
    except ET.ParseError as error:
        raise exceptions.MalformedWsdlFile(path=path, error=str(error)) from error
    if root is None:
        raise exceptions.MalformedWsdlFile(path=path, error="Empty XML document.")
    _xml_namespace_scope_registry.remember(root, element_scopes)
    return root, prefixes


def _namespace_scopes_for_root(root: ET.Element) -> dict[int, dict[str, str]]:
    return _xml_namespace_scope_registry.scopes_for(root)


def _iter_bytes(data: bytes):
    from io import BytesIO

    return BytesIO(data)


def _read_path(path: str) -> bytes:
    if _is_remote_url(path):
        try:
            with urllib.request.urlopen(path) as response:
                return response.read()
        except HTTPError as error:
            if error.code in {401, 403}:
                raise exceptions.RemoteWsdlAuthenticationError(
                    path=path,
                    status=error.code,
                    error=error.reason,
                ) from error
            if error.code >= 500:
                raise exceptions.RemoteWsdlServerError(
                    path=path,
                    status=error.code,
                    error=error.reason,
                ) from error
            raise exceptions.RemoteWsdlResourceUnavailable(
                path=path,
                error=f"HTTP status: {error.code}. {error.reason}",
            ) from error
        except URLError as error:
            raise exceptions.RemoteWsdlResourceUnavailable(
                path=path,
                error=str(error.reason),
            ) from error
    return pathlib.Path(path).read_bytes()


def _is_remote_url(path: str) -> bool:
    return path.startswith(("http://", "https://"))


def _local_name(name: Optional[str]) -> str:
    return local_name(name)


def _collect_xsd_types(
    manifest: Manifest,
    schemas: Iterable[ET.Element],
    *,
    dataset_name: str | None,
    schema_source_path: str,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, dict[str, Any]], list[Exception]]:
    schema_diagnostics: list[Exception] = []
    del manifest
    element_types, raw_schema_models = collect_embedded_schema_types(
        schemas,
        dataset_name=dataset_name or "",
        schema_source_path=schema_source_path,
        xsd_ns=XSD_NS,
        datatype_overrides=get_wsdl_datatype_overrides(),
        flatten_nested_handler=flatten_embedded_nested_fields,
        schema_diagnostics=schema_diagnostics,
        tolerate_schema_errors=True,
    )
    return element_types, raw_schema_models, schema_diagnostics


def _merge_metadata(*parts: dict[str, str] | None) -> dict[str, str]:
    result: dict[str, str] = {}
    for part in parts:
        if part:
            result.update({key: value for key, value in part.items() if value})
    return result


def _soap_metadata_to_resource_params(metadata: dict[str, str]) -> dict[str, dict[str, Any]]:
    params: dict[str, dict[str, Any]] = {}
    for name, value in metadata.items():
        params[name] = {
            "type": "param",
            "name": name,
            "source": [value],
            "prepare": [NA],
            "title": "",
            "description": "",
        }
    return params


def _message_info_from_element_or_type(
    node: Optional[ET.Element],
    element_types: dict[str, list[dict[str, Any]]],
    *,
    namespace_context: WsdlNamespaceContext,
) -> Optional[MessageInfo]:
    if node is None:
        return None

    element_qname = namespace_context.resolve_in_scope(node.get("element"), node=node)
    type_qname = namespace_context.resolve_in_scope(node.get("type"), node=node)
    if element_qname:
        return MessageInfo(
            name=element_qname.local_name,
            qname=element_qname,
            element_qname=element_qname,
            fields=element_types.get(element_qname.expanded_name, []),
        )
    if type_qname:
        return MessageInfo(
            name=type_qname.local_name,
            qname=type_qname,
            type_qname=type_qname,
            fields=[],
        )
    return None


def _fields_to_properties(
    fields: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    properties = {}
    deduplicator = Deduplicator("_{}")
    for field in fields:
        name = deduplicator(to_property_name(field["name"]))
        prop = {
            "type": field["type"],
            "external": {"name": field.get("source") or field["name"]},
            "required": field.get("required", False),
            "description": "",
        }
        if field.get("array"):
            item = dict(prop)
            item["given_name"] = f"{name}[]"
            prop["type"] = "array"
            prop["explicitly_given"] = False
            prop["items"] = item
        properties[name] = prop
    return properties


def name_or_default(element: ET.Element) -> str:
    return _local_name(element.get("name")) or "value"