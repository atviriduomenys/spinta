from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Mapping, Optional
import xml.etree.ElementTree as ET

from spinta.manifests.wsdl.xml_helpers import get_attribute_by_local_name, local_part, tag_namespace
from spinta.manifests.wsdl.ambiguity import ensure_unique_wsdl_name

from ..qname import WsdlQName, resolve_wsdl_qname, resolve_wsdl_qname_in_scope, wsdl_qname_key
from ..xsd.helpers import local_name


WSDL_2_0_NS = "http://www.w3.org/ns/wsdl"


@dataclass(frozen=True)
class WsdlNamespaceContext:
    path: str
    target_namespace: str
    namespace_map: Mapping[str, str]
    namespace_scopes: Mapping[int, Mapping[str, str]]

    def resolve(self, name: str | None) -> WsdlQName | None:
        return resolve_wsdl_qname(
            name,
            namespace_map=self.namespace_map,
            default_namespace=self.target_namespace,
        )

    def resolve_in_scope(self, name: str | None, *, node: ET.Element) -> WsdlQName | None:
        return resolve_wsdl_qname_in_scope(
            name,
            node=node,
            namespace_scopes=self.namespace_scopes,
            namespace_map=self.namespace_map,
            default_namespace=self.target_namespace,
        )


@dataclass
class MessageInfo:
    name: str
    qname: WsdlQName | None = None
    element_qname: WsdlQName | None = None
    type_qname: WsdlQName | None = None
    fields: Optional[list[dict[str, Any]]] = None


@dataclass
class BindingInfo:
    qname: WsdlQName | None
    type_qname: WsdlQName | None
    params: dict[str, str]
    operation_params: dict[str, dict[str, str]]


@dataclass
class ServicePortInfo:
    name: str
    qname: WsdlQName | None
    binding_qname: WsdlQName | None
    params: dict[str, str]


def collect_messages(
    root: ET.Element,
    *,
    wsdl_ns: str,
    element_types: dict[str, list[dict[str, Any]]],
    namespace_context: WsdlNamespaceContext,
) -> dict[str, MessageInfo]:
    result: dict[str, MessageInfo] = {}
    for message in root.findall(f".//{{{wsdl_ns}}}message"):
        message_name = local_name(message.get("name"))
        message_qname = namespace_context.resolve_in_scope(message.get("name"), node=message)
        info = MessageInfo(name=message_name, qname=message_qname, fields=[])
        for part in message.findall(f"{{{wsdl_ns}}}part"):
            element_qname = namespace_context.resolve_in_scope(part.get("element"), node=part)
            type_qname = namespace_context.resolve_in_scope(part.get("type"), node=part)
            if element_qname:
                info.element_qname = element_qname
                info.fields = element_types.get(element_qname.expanded_name, [])
            elif type_qname:
                info.type_qname = type_qname
        message_key = wsdl_qname_key(message_qname, fallback=message_name)
        ensure_unique_wsdl_name(
            result,
            message_qname,
            key=message_key,
            scope="wsdl:message",
            path=namespace_context.path,
        )
        result[message_key] = info
    return result


def message_info(
    node: Optional[ET.Element],
    messages: dict[str, MessageInfo],
    *,
    namespace_context: WsdlNamespaceContext,
) -> Optional[MessageInfo]:
    if node is None:
        return None
    reference_qname = namespace_context.resolve_in_scope(node.get("message"), node=node)
    if reference_qname is None:
        return None
    return messages.get(reference_qname.expanded_name)


def collect_bindings(
    root: ET.Element,
    *,
    wsdl_ns: str,
    namespace_context: WsdlNamespaceContext,
) -> dict[str, BindingInfo]:
    bindings: dict[str, BindingInfo] = {}
    for binding in root.findall(f".//{{{wsdl_ns}}}binding"):
        operation_params: dict[str, dict[str, str]] = {}
        for operation in binding.findall(f"{{{wsdl_ns}}}operation"):
            operation_name = local_name(operation.get("name") or operation.get("ref"))
            params: dict[str, str] = {}
            for child in operation:
                if _is_soap_extension(child, "operation"):
                    soap_action = get_attribute_by_local_name(child, "soapAction") or get_attribute_by_local_name(child, "action")
                    if soap_action:
                        params["soapAction"] = soap_action
            if params:
                operation_params[operation_name] = params

        binding_name = local_name(binding.get("name"))
        binding_qname = namespace_context.resolve_in_scope(binding.get("name"), node=binding)
        type_qname = namespace_context.resolve_in_scope(
            binding.get("type") or binding.get("interface"),
            node=binding,
        )
        binding_key = wsdl_qname_key(binding_qname, fallback=binding_name)
        ensure_unique_wsdl_name(
            bindings,
            binding_qname,
            key=binding_key,
            scope="wsdl:binding",
            path=namespace_context.path,
        )
        bindings[binding_key] = BindingInfo(
            qname=binding_qname,
            type_qname=type_qname,
            params=_collect_soap_binding_params(binding),
            operation_params=operation_params,
        )
    return bindings


def collect_wsdl_2_0_bindings(
    root: ET.Element,
    *,
    namespace_context: WsdlNamespaceContext,
) -> dict[str, BindingInfo]:
    bindings: dict[str, BindingInfo] = {}
    for binding in root.findall(f".//{{{WSDL_2_0_NS}}}binding"):
        binding_name = local_name(binding.get("name"))
        binding_qname = namespace_context.resolve_in_scope(binding.get("name"), node=binding)
        interface_qname = namespace_context.resolve_in_scope(binding.get("interface"), node=binding)
        binding_key = wsdl_qname_key(binding_qname, fallback=binding_name)
        ensure_unique_wsdl_name(
            bindings,
            binding_qname,
            key=binding_key,
            scope="wsdl:binding",
            path=namespace_context.path,
        )
        bindings[binding_key] = BindingInfo(
            qname=binding_qname,
            type_qname=interface_qname,
            params=_collect_soap_binding_params(binding),
            operation_params={},
        )
    return bindings


def collect_services(
    root: ET.Element,
    *,
    wsdl_ns: str,
    namespace_context: WsdlNamespaceContext,
) -> dict[str, dict[str, ServicePortInfo]]:
    services: dict[str, dict[str, ServicePortInfo]] = defaultdict(dict)
    for service in root.findall(f".//{{{wsdl_ns}}}service"):
        service_name = local_name(service.get("name"))
        service_qname = namespace_context.resolve_in_scope(service.get("name"), node=service)
        service_key = wsdl_qname_key(service_qname, fallback=service_name)
        ensure_unique_wsdl_name(
            services,
            service_qname,
            key=service_key,
            scope="wsdl:service",
            path=namespace_context.path,
        )
        for port in service.findall(f"{{{wsdl_ns}}}port"):
            params: dict[str, str] = {}
            for child in port:
                if _is_soap_extension(child, "address"):
                    address = get_attribute_by_local_name(child, "location") or get_attribute_by_local_name(child, "address")
                    if address:
                        params["address"] = address

            port_name = local_name(port.get("name"))
            port_qname = namespace_context.resolve_in_scope(port.get("name"), node=port)
            binding_qname = namespace_context.resolve_in_scope(port.get("binding"), node=port)
            services[service_key][wsdl_qname_key(port_qname, fallback=port_name)] = ServicePortInfo(
                name=port_name,
                qname=port_qname,
                binding_qname=binding_qname,
                params=params,
            )
    return services


def collect_wsdl_2_0_services(
    root: ET.Element,
    *,
    namespace_context: WsdlNamespaceContext,
) -> dict[str, dict[str, ServicePortInfo]]:
    services: dict[str, dict[str, ServicePortInfo]] = {}
    for service in root.findall(f".//{{{WSDL_2_0_NS}}}service"):
        service_name = local_name(service.get("name"))
        service_qname = namespace_context.resolve_in_scope(service.get("name"), node=service)
        service_key = wsdl_qname_key(service_qname, fallback=service_name)
        ensure_unique_wsdl_name(
            services,
            service_qname,
            key=service_key,
            scope="wsdl:service",
            path=namespace_context.path,
        )
        endpoints: dict[str, ServicePortInfo] = {}
        for endpoint in service.findall(f"{{{WSDL_2_0_NS}}}endpoint"):
            params: dict[str, str] = {}
            address = get_attribute_by_local_name(endpoint, "address")
            if address:
                params["address"] = address
            endpoint_name = local_name(endpoint.get("name"))
            endpoint_qname = namespace_context.resolve_in_scope(endpoint.get("name"), node=endpoint)
            binding_qname = namespace_context.resolve_in_scope(endpoint.get("binding"), node=endpoint)
            endpoints[wsdl_qname_key(endpoint_qname, fallback=endpoint_name)] = ServicePortInfo(
                name=endpoint_name,
                qname=endpoint_qname,
                binding_qname=binding_qname,
                params=params,
            )
        services[service_key] = endpoints
    return services


def _collect_soap_binding_params(binding: ET.Element) -> dict[str, str]:
    params: dict[str, str] = {}
    for child in binding:
        if _is_soap_extension(child, "binding"):
            for name in ("style", "transport", "protocol"):
                value = get_attribute_by_local_name(child, name)
                if value:
                    params[name] = value
    return params


def _is_soap_extension(node: ET.Element, local: str) -> bool:
    namespace = tag_namespace(node.tag)
    return local_part(node.tag) == local and namespace in {
        "http://schemas.xmlsoap.org/wsdl/soap/",
        "http://www.w3.org/ns/wsdl/soap",
    }