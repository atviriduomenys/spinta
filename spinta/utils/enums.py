from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Any, TypeVar

from spinta import exceptions

if TYPE_CHECKING:
    from spinta.components import Component, Context

E = TypeVar("E", bound=Enum)


def get_enum_by_name(enum: type[E], value: str) -> E:
    for item in enum:
        if item.name == value:
            return item
    raise Exception(f"Unknown value {value!r}.")


def get_enum_by_value(enum: type[E], value: Any) -> E:
    for item in enum:
        if item.value == value:
            return item
    raise Exception(f"Unknown value {value!r}.")


def enum_by_name(
    component: Component,
    param: str | None,  # component param
    enum: type[E],
    name: Any,
) -> E | None:
    if name is None or name == "":
        return None
    for item in enum:
        if item.name == name:
            return item
    raise exceptions.InvalidValue(component, param=param, given=name)


def enum_by_value(
    context: Context,
    component: Component,
    param: str | None,  # component param
    enum: type[E],
    value: Any,
) -> E | None:
    if value is None or value == "":
        return None
    for item in enum:
        if item.value == value:
            return item

    error = exceptions.InvalidValue(component, param=param, given=value)

    manager = context.get("error_manager")
    manager.handle_error(error)
