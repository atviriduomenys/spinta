from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType


_WSDL_DATATYPE_OVERRIDES = {
    "anyURI": "url",
}

WSDL_DATATYPE_OVERRIDES: Mapping[str, str] = MappingProxyType(_WSDL_DATATYPE_OVERRIDES)


def get_wsdl_datatype_overrides() -> Mapping[str, str]:
    return WSDL_DATATYPE_OVERRIDES


def normalize_embedded_field_type(type_name: str, overrides: Mapping[str, str] | None) -> str:
    if overrides and type_name == "uri" and overrides.get("anyURI"):
        return overrides["anyURI"]
    return type_name
