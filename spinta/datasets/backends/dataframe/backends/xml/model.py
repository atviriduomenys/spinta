"""Domain entities and value objects for ftree."""

from __future__ import annotations

from dataclasses import dataclass
import enum

from collections.abc import Callable
from typing import Any, Optional, Tuple

from spinta.components import Component


class DataModel(Component):
    """Data model component"""
    schema = {
        "data": {"type": "dict", "required": True}
    }
    data: dict[str, Any]

    def __dict__(self):
        return self.data


class MetaItem():
    pass


class ModelHeader(MetaItem):
    pass


class ModelRef(MetaItem):
    pass


@dataclass
class ModelItem:
    """Represents an item in a models after normalization."""

    path: Tuple[str, ...]
    property: str
    type: str | MetaItem
    ref: str
    source: Optional[str] = None
    value: str | Callable[[Any], Any] | None = None
    access: Optional[enum.IntEnum] = None
    maturity: Optional[enum.IntEnum] = None


__all__ = [
    "DataModel",
    "ModelHeader",
    "ModelRef",
]
