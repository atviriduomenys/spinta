from __future__ import annotations

import xml.etree.ElementTree as ET

from .xsd.helpers import local_name


def get_attribute_by_local_name(node: ET.Element, name: str) -> str | None:
    if name in node.attrib:
        return node.attrib[name]
    for attr_name, value in node.attrib.items():
        if local_part(attr_name) == name:
            return value
    return None


def local_part(name: str | None) -> str:
    if not name:
        return ""
    if name.startswith("{"):
        return name[1:].split("}", 1)[1]
    return local_name(name)


def tag_namespace(tag: str) -> str | None:
    if tag.startswith("{"):
        return tag[1:].split("}", 1)[0]
    return None