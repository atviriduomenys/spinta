from collections.abc import Callable

from typing import Optional, Any
from typing_extensions import Concatenate, ParamSpec

from spinta.components import Context

P = ParamSpec("P")
UpgradeFuncType = Callable[Concatenate[Context, P], Any]
UpgradeCheckFuncType = Callable[Concatenate[Context, P], bool]


class UpgradeComponent:
    def __init__(
        self,
        upgrade: UpgradeFuncType,
        check: Optional[UpgradeCheckFuncType] = None
    ):
        self.__upgrade_func = upgrade
        self.__check_func = check

    def upgrade(self, context: Context, **kwargs):
        self.__upgrade_func(context, **kwargs)

    def check(self, context: Context, **kwargs) -> bool:
        if self.__check_func is None:
            return True

        return self.__check_func(context, **kwargs)


UPGRADE_CLIENTS_SCRIPT = "clients"
