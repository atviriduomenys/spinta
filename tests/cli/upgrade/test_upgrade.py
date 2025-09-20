import os
import pathlib
from collections import Counter

import pytest

from spinta.cli.helpers.upgrade.components import Script
from spinta.cli.helpers.script.components import ScriptStatus
from spinta.cli.helpers.script.helpers import script_check_status_message
from spinta.cli.helpers.upgrade.registry import upgrade_script_registry
from spinta.cli.helpers.upgrade.scripts.clients import client_migration_status_message, CLIENT_STATUS_SUCCESS
from spinta.exceptions import ScriptNotFound
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.client import create_old_client_file
from spinta.utils.config import get_clients_path, get_keymap_path


def test_upgrade_invalid_script_name(context, rc, cli: SpintaCliRunner):
    with pytest.raises(ScriptNotFound):
        result = cli.invoke(rc, ["upgrade", "UNAVAILABLE"], fail=False)
        raise result.exception


def test_upgrade_check_only(context, rc, cli: SpintaCliRunner, tmp_path: pathlib.Path):
    clients_path = get_clients_path(tmp_path)
    os.makedirs(clients_path, exist_ok=True)
    rc = rc.fork({"config_path": str(tmp_path), "default_auth_client": None})

    # Create already existing file, to imitate old structure
    create_old_client_file(
        clients_path, {"client_id": "TEST", "client_secret_hash": "secret", "scopes": ["uapi:/:getall"]}
    )
    items = os.listdir(clients_path)
    assert items == ["TEST.yml"]

    result = cli.invoke(rc, ["upgrade", Script.CLIENTS.value, "-c"])
    assert result.exit_code == 0
    assert script_check_status_message(Script.CLIENTS.value, ScriptStatus.REQUIRED) in result.stdout
    assert client_migration_status_message("TEST.yml", CLIENT_STATUS_SUCCESS) not in result.stdout
    assert "Created keymap with 1 users" not in result.stdout

    # Check if no folders were created
    items = os.listdir(clients_path)
    assert Counter(items) == Counter(["TEST.yml"])

    # Ensure keymap.yml does not exist since it should have only done check and no execution.
    keymap_path = get_keymap_path(clients_path)
    assert not keymap_path.exists()


def test_upgrade_all(context, rc, cli: SpintaCliRunner, tmp_path: pathlib.Path):
    result = cli.invoke(rc, ["upgrade", "-c"])
    assert result.exit_code == 0

    for script in upgrade_script_registry.get_all_names():
        assert script_check_status_message(script, ScriptStatus.PASSED) in result.stdout


def test_upgrade_multiple(context, rc, cli: SpintaCliRunner, tmp_path: pathlib.Path):
    result = cli.invoke(rc, ["upgrade", Script.SQL_KEYMAP_REDIRECT.value, Script.SQL_KEYMAP_INITIAL.value, "-c"])
    assert result.exit_code == 0

    assert script_check_status_message(Script.SQL_KEYMAP_INITIAL.value, ScriptStatus.PASSED) in result.stdout
    assert script_check_status_message(Script.SQL_KEYMAP_REDIRECT.value, ScriptStatus.PASSED) in result.stdout
