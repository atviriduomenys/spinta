from __future__ import annotations

from enum import Enum
from typing import Any
from typing import Optional
from typing import Type

from spinta import exceptions


def get_enum_by_name(enum, value):
    for item in enum:
        if item.name == value:
            return item
    raise Exception(f"Unknown value {value!r}.")


def enum_by_name(
    component: 'spinta.components.Component',
    param: Optional[str],  # component param
    enum: Type[Enum],
    name: Any,
) -> Optional[Enum]:
    if name is None or name == '':
        return None
    for item in enum:
        if item.name == name:
            return item
    raise exceptions.InvalidValue(component, param=param, given=name)


def enum_by_value(
    component: 'spinta.components.Component',
    param: Optional[str],  # component param
    enum: Type[Enum],
    value: Any,
) -> Optional[Enum]:
    if value is None or value == '':
        return None
    for item in enum:
        if item.value == value:
            return item
    raise exceptions.InvalidValue(component, param=param, given=value)


