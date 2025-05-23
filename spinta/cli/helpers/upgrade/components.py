from __future__ import annotations

import enum
from collections.abc import Callable

from typing_extensions import ParamSpec

from spinta.components import Context

P = ParamSpec("P")
UpgradeFuncType = Callable[P.kwargs]
UpgradeCheckFuncType = Callable[P.kwargs, bool]


class UpgradeComponent:
    def __init__(
        self,
        upgrade: UpgradeFuncType,
        check: UpgradeCheckFuncType | None = None,
        required: list[str] | None = None
    ):
        self.__upgrade_func = upgrade
        self.__check_func = check
        self.required = required

    def upgrade(self, context: Context, **kwargs):
        self.__upgrade_func(context, **kwargs)

    def check(self, context: Context, **kwargs) -> bool:
        if self.__check_func is None:
            return True

        return self.__check_func(context, **kwargs)


@enum.unique
class Script(enum.Enum):
    CLIENTS = "clients"
    REDIRECT = "redirect"
    DEDUPLICATE = "deduplicate"


class ScriptStatus(enum.Enum):
    PASSED = "PASSED"
    REQUIRED = "REQUIRED"
    FORCED = "FORCED"
    SKIPPED = "SKIPPED"
