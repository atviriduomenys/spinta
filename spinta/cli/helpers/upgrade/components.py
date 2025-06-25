from __future__ import annotations

import enum
from collections.abc import Callable

try:
    from typing import Concatenate, ParamSpec
except ImportError:
    from typing_extensions import Concatenate, ParamSpec

from spinta.components import Context

P = ParamSpec('P')

UpgradeFuncType = Callable[Concatenate[Context, P], None]
UpgradeCheckFuncType = Callable[Concatenate[Context, P], bool]


class UpgradeComponent:
    """
        Represents an upgrade component with optional preconditions and target constraints.

        Targets and tags are mainly used to be able to filter specific upgrades. Main use case would be to set target as
        specific object, like sqlalchemy keymap and tag as migration, so then you could filter all database migration
        scripts.

        Attributes:
            name (str): The name of the upgrade component.
            required (list[str] | None): List of required component names that must be present before applying this upgrade.
            targets (set[str]): Set of target identifiers this upgrade applies to.
            tags (set[str]): Set of tags associated with this upgrade component.

        Args:
            name (str): The name of the upgrade component.
            upgrade (callable): A callable that performs the upgrade logic.
            check (callable | None, optional): A callable that checks whether the upgrade should be applied. Defaults to None.
            required (list[str] | None, optional): A list of required component names. Defaults to None.
            targets (set[str] | None, optional): A set of applicable target identifiers. Defaults to None.
            tags (set[str] | None, optional): A set of tags for categorizing the component. Defaults to None.

        Methods:
            upgrade(context: Context, **kwargs):
                Executes the upgrade using the provided context and keyword arguments.
            check(context: Context, **kwargs) -> bool:
                Checks if the upgrade should be applied.
                Returns True if no check function is provided.
        """

    def __init__(
        self,
        name: str,
        upgrade: UpgradeFuncType,
        check: UpgradeCheckFuncType | None = None,
        required: list[str] | None = None,
        targets: set[str] | None = None,
        tags: set[str] | None = None,
    ):
        self.name = name
        self.__upgrade_func = upgrade
        self.__check_func = check
        self.required = required
        self.targets = targets or set()
        self.tags = tags or set()

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
    CHANGELOG = "changelog"

    # Sqlalchemy keymap migrations
    SQL_KEYMAP_INITIAL = "sqlalchemy_keymap_001_initial"
    SQL_KEYMAP_REDIRECT = "sqlalchemy_keymap_002_redirect_support"


@enum.unique
class ScriptTarget(enum.Enum):
    SQLALCHEMY_KEYMAP = "sqlalchemy_keymap"
    FS = "file_system"
    AUTH = "auth"
    BACKEND = "backend"


@enum.unique
class ScriptTag(enum.Enum):
    BUG_FIX = "bug_fix"
    MIGRATION = "migration"
    DB_MIGRATION = "db_migration"


class ScriptStatus(enum.Enum):
    PASSED = "PASSED"
    REQUIRED = "REQUIRED"
    FORCED = "FORCED"
    SKIPPED = "SKIPPED"
