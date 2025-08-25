from __future__ import annotations

import enum
from collections.abc import Callable

try:
    from typing import Concatenate, ParamSpec
except ImportError:
    from typing_extensions import Concatenate, ParamSpec

from spinta.components import Context

P = ParamSpec("P")

ScriptFuncType = Callable[Concatenate[Context, P], None]
ScriptCheckFuncType = Callable[Concatenate[Context, P], bool]


class _ScriptMeta(type):
    _type_registry: dict[str, type] = {}

    def __new__(cls, name, bases, attrs):
        new_cls = super().__new__(cls, name, bases, attrs)
        script_type = attrs.get("script_type")
        if script_type:
            if script_type in cls._type_registry:
                raise ValueError(f"Duplicate script type: {script_type}")
            cls._type_registry[script_type] = new_cls
        return new_cls


class ScriptBase(metaclass=_ScriptMeta):
    """
    Represents an script with optional preconditions and target constraints.

    Targets and tags are mainly used to be able to filter specific scripts. Main use case would be to set target as
    specific object, like sqlalchemy keymap and tag as migration, so then you could filter all database migration
    scripts.

    Attributes:
        name (str): The name of the script.
        required (list[str, tuple[str, str]] | None): List of required script names, or tuples of a script type with
        its name that must be present before applying this script.
        targets (set[str]): Set of target identifiers this script applies to.
        tags (set[str]): Set of tags associated with this script.

    Args:
        name (str): The name of the script.
        run (callable): A callable that performs the script logic.
        check (callable | None, optional): A callable that checks whether the script should be applied. Defaults to None.
        required (list[str | tuple[str, str]] | None, optional): A list of required script names, or tuples of a script
        type with its name. Defaults to None.
        targets (set[str] | None, optional): A set of applicable target identifiers. Defaults to None.
        tags (set[str] | None, optional): A set of tags for categorizing the script. Defaults to None.

    Methods:
        run(context: Context, **kwargs):
            Runs the script using the provided context and keyword arguments.
        check(context: Context, **kwargs) -> bool:
            Checks if the script should be applied.
            Returns True if no check function is provided.
    """

    script_type: str = None
    name: str
    run: ScriptFuncType
    check: ScriptCheckFuncType
    required: list[str | tuple[str, str]]
    targets: set[str]
    tags: set[str]

    def __init__(
        self,
        name: str,
        run: ScriptFuncType,
        check: ScriptCheckFuncType | None = None,
        required: list[str | tuple[str, str]] | None = None,
        targets: set[str] | None = None,
        tags: set[str] | None = None,
    ):
        self.name = name
        self.__run_func = run
        self.__check_func = check
        self.required = required
        self.targets = targets or set()
        self.tags = tags or set()

    def run(self, context: Context, **kwargs):
        self.__run_func(context, **kwargs)

    def check(self, context: Context, **kwargs) -> bool:
        if self.__check_func is None:
            return True

        return self.__check_func(context, **kwargs)

    @classmethod
    def get_registered_types(cls) -> dict[str, type]:
        return cls._type_registry


class ScriptStatus(enum.Enum):
    PASSED = "PASSED"
    REQUIRED = "REQUIRED"
    FORCED = "FORCED"
    SKIPPED = "SKIPPED"


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
