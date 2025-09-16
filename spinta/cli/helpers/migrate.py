from __future__ import annotations

import dataclasses
from typing import List, Callable


@dataclasses.dataclass
class MigrationConfig:
    plan: bool
    autocommit: bool
    rename_src: str | dict
    datasets: List[str] = dataclasses.field(default=None)
    migration_extension: Callable = dataclasses.field(default=None)
    raise_error: bool = dataclasses.field(default=False)


@dataclasses.dataclass
class MigrationContext:
    config: MigrationConfig
