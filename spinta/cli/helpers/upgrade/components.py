from __future__ import annotations

import enum

from spinta.cli.helpers.script.components import ScriptBase

UPGRADE_SCRIPT_TYPE: str = "upgrade"


class UpgradeScript(ScriptBase):
    script_type: str = UPGRADE_SCRIPT_TYPE


@enum.unique
class Script(enum.Enum):
    CLIENTS = "clients"
    REDIRECT = "redirect"

    # Sqlalchemy keymap migrations
    SQL_KEYMAP_INITIAL = "sqlalchemy_keymap_001_initial"
    SQL_KEYMAP_REDIRECT = "sqlalchemy_keymap_002_redirect_support"
    SQL_KEYMAP_MODIFIED = "sqlalchemy_keymap_003_add_modified_time"
