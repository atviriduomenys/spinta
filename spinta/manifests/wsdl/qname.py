from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping
import xml.etree.ElementTree as ET


@dataclass(frozen=True)
class WsdlQName:
    raw: str
    local_name: str
    namespace: str | None = None
    prefix: str | None = None

    @property
    def expanded_name(self) -> str:
        if self.namespace:
            return f"{{{self.namespace}}}{self.local_name}"
        return self.local_name


def build_wsdl_namespace_map(prefixes: Mapping[str, str]) -> dict[str, str]:
    namespace_map: dict[str, str] = {}
    for prefix, uri in prefixes.items():
        if prefix == "xmlns":
            namespace_map[""] = uri
        else:
            namespace_map[prefix] = uri
    return namespace_map


def resolve_wsdl_qname(
    value: str | None,
    *,
    namespace_map: Mapping[str, str] | None = None,
    default_namespace: str | None = None,
) -> WsdlQName | None:
    if not value:
        return None

    if value.startswith("{") and "}" in value:
        namespace, local_name = value[1:].split("}", 1)
        return WsdlQName(raw=value, local_name=local_name, namespace=namespace)

    if ":" in value:
        prefix, local_name = value.split(":", 1)
        namespace = None if namespace_map is None else namespace_map.get(prefix)
        return WsdlQName(
            raw=value,
            local_name=local_name,
            namespace=namespace,
            prefix=prefix,
        )

    namespace = default_namespace
    if namespace is None and namespace_map is not None:
        namespace = namespace_map.get("")

    return WsdlQName(raw=value, local_name=value, namespace=namespace)


def resolve_wsdl_qname_in_scope(
    value: str | None,
    *,
    node: ET.Element | None,
    namespace_scopes: Mapping[int, Mapping[str, str]] | None = None,
    namespace_map: Mapping[str, str] | None = None,
    default_namespace: str | None = None,
) -> WsdlQName | None:
    scoped_namespace_map = namespace_map
    if node is not None and namespace_scopes is not None:
        scoped_namespace_map = namespace_scopes.get(id(node), namespace_map)

    return resolve_wsdl_qname(
        value,
        namespace_map=scoped_namespace_map,
        default_namespace=default_namespace,
    )


def wsdl_qname_key(qname: WsdlQName | None, fallback: str = "") -> str:
    if qname is None:
        return fallback
    return qname.expanded_name


def wsdl_qname_local_name(qname: WsdlQName | None, fallback: str = "") -> str:
    if qname is None:
        return fallback
    return qname.local_name


def wsdl_local_name(value: str) -> str:
    if value.startswith("{") and "}" in value:
        return value[1:].split("}", 1)[1]
    return value.split(":", 1)[-1]
