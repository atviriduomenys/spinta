from __future__ import annotations

from collections.abc import Iterable

from ..document.helpers import BindingInfo, ServicePortInfo
from ..qname import WsdlQName, wsdl_local_name, wsdl_qname_key


def resolve_ports_for_port_type(
    services: dict[str, dict[str, ServicePortInfo]],
    bindings: dict[str, BindingInfo],
    port_type_qname: WsdlQName | None,
    port_type_name: str,
) -> Iterable[tuple[str, str]]:
    matches = []
    port_type_key = wsdl_qname_key(port_type_qname, fallback=port_type_name)
    for service_name, ports in services.items():
        for port_name, port_info in ports.items():
            binding_key = wsdl_qname_key(port_info.binding_qname)
            binding = bindings.get(binding_key)
            binding_type_key = wsdl_qname_key(binding.type_qname if binding else None)
            if binding and binding_type_key == port_type_key:
                matches.append((wsdl_local_name(service_name), port_info.name or wsdl_local_name(port_name)))
    if not matches:
        matches.append(("service", "port"))
    return matches