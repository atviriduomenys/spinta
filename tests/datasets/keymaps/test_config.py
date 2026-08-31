import os
import tempfile

from spinta.cli.helpers.store import attach_keymaps
from spinta.core.config import RawConfig
from spinta.testing.manifest import prepare_manifest


def test_multiple_keymap_config(rc: RawConfig):
    with tempfile.TemporaryDirectory() as tmpdir:
        default_path = f"sqlite+spinta:///{os.path.join(tmpdir, 'default.sqlite')}"
        sqlite1_path = f"sqlite+spinta:///{os.path.join(tmpdir, 'sqlite1.sqlite')}"
        sqlite2_path = f"sqlite+spinta:///{os.path.join(tmpdir, 'sqlite2.sqlite')}"
        forked = rc.fork(
            {
                "keymaps": {
                    "default": {"type": "sqlalchemy", "dsn": default_path},
                    "sqlite1": {"type": "sqlalchemy", "dsn": sqlite1_path},
                    "sqlite2": {"type": "sqlalchemy", "dsn": sqlite2_path},
                }
            }
        )

        context, _ = prepare_manifest(forked, None, full_load=True)
        store = context.get("store")
        with context:
            attach_keymaps(context, store)
            default = context.get("keymap.default")
            assert default.name == "default"
            assert default.dsn == default_path
            sqlite1 = context.get("keymap.sqlite1")
            assert sqlite1.name == "sqlite1"
            assert sqlite1.dsn == sqlite1_path
            sqlite2 = context.get("keymap.sqlite2")
            assert sqlite2.name == "sqlite2"
            assert sqlite2.dsn == sqlite2_path
