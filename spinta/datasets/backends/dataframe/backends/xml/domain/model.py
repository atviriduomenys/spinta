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


class TransformationModel():
    pass


@dataclass
class ModelItem:
    """Represents a row in a manifest after adapter normalization."""

    path: Tuple[str, ...]
    property: str
    type: str | MetaItem
    ref: str
    source: Optional[str] = None
    value: str | Callable[[Any], Any] | None = None
    access: Optional[enum.IntEnum] = None
    maturity: Optional[enum.IntEnum] = None


@dataclass
class Model(Component):
    """Domain representation of model data."""
    manifest: TransformationModel
    data: dict[str, object]

    def __init__(self, manifest: TransformationModel, data: dict[str, object]) -> None:
        self.manifest = manifest
        self.data = data

    def __call__(self, data, manifest) -> Model:
        return Model(data=data, manifest=manifest)


__all__ = [
    "Model",
    "DataModel",
    "ModelHeader",
    "ModelRef",
    "TransformationModel",
]
