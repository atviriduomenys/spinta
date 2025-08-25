from __future__ import annotations

import enum

from spinta.cli.helpers.script.components import ScriptBase

ADMIN_SCRIPT_TYPE = "admin"


class AdminScript(ScriptBase):
    script_type = ADMIN_SCRIPT_TYPE


@enum.unique
class Script(enum.Enum):
    DEDUPLICATE = "deduplicate"
    CHANGELOG = "changelog"
