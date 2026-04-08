from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType


DATATYPES_MAPPING = {
    "string": "string",
    "boolean": "boolean",
    "decimal": "number",
    "float": "number",
    "double": "number",
    "duration": "string",
    "dateTime": "datetime",
    "time": "time",
    "date": "date",
    "gYearMonth": "date;enum;M",
    "gYear": "date;enum;Y",
    "gMonthDay": "string",
    "gDay": "string",
    "gMonth": "string",
    "hexBinary": "string",
    "base64Binary": "binary;prepare;base64",
    "anyURI": "uri",
    "QName": "string",
    "NOTATION": "string",
    "normalizedString": "string",
    "token": "string",
    "language": "string",
    "NMTOKEN": "string",
    "NMTOKENS": "string",
    "Name": "string",
    "NCName": "string",
    "ID": "string",
    "IDREF": "string",
    "IDREFS": "string",
    "ENTITY": "string",
    "ENTITIES": "string",
    "integer": "integer",
    "nonPositiveInteger": "integer",
    "negativeInteger": "integer",
    "long": "integer",
    "int": "integer",
    "short": "integer",
    "byte": "integer",
    "nonNegativeInteger": "integer",
    "unsignedLong": "integer",
    "unsignedInt": "integer",
    "unsignedShort": "integer",
    "unsignedByte": "integer",
    "positiveInteger": "integer",
    "yearMonthDuration": "integer",
    "dayTimeDuration": "integer",
    "dateTimeStamp": "datetime",
    "": "string",
}


_WSDL_DATATYPE_OVERRIDES = {
    "anyURI": "url",
}

WSDL_DATATYPE_OVERRIDES: Mapping[str, str] = MappingProxyType(_WSDL_DATATYPE_OVERRIDES)


def get_wsdl_datatype_overrides() -> Mapping[str, str]:
    return WSDL_DATATYPE_OVERRIDES


def map_xsd_datatype(
    type_name: str | None,
    *,
    custom_types: Mapping[str, str] | None = None,
    overrides: Mapping[str, str] | None = None,
    default: str = "string",
) -> str:
    name = _local_name(type_name)
    if custom_types and name in custom_types:
        return custom_types[name]

    mapped = DATATYPES_MAPPING.get(name, default)
    if ";" in mapped:
        mapped = mapped.split(";", 1)[0]

    if overrides and name in overrides:
        return overrides[name]

    return mapped


def map_wsdl_xsd_datatype(
    type_name: str | None,
    *,
    custom_types: Mapping[str, str] | None = None,
    default: str = "string",
) -> str:
    return map_xsd_datatype(
        type_name,
        custom_types=custom_types,
        overrides=get_wsdl_datatype_overrides(),
        default=default,
    )


def normalize_embedded_field_type(type_name: str, overrides: Mapping[str, str] | None) -> str:
    if overrides and type_name == "uri" and overrides.get("anyURI"):
        return overrides["anyURI"]
    return type_name


def _local_name(name: str | None) -> str:
    if not name:
        return ""
    return name.split(":")[-1]