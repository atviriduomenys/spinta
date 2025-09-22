import os
import pathlib
from collections import Counter
import pytest

from spinta.auth import ensure_client_folders_exist, get_client_file_path
from spinta.cli.helpers.upgrade.components import Script
from spinta.cli.helpers.script.components import ScriptStatus
from spinta.cli.helpers.script.helpers import script_check_status_message
from spinta.cli.helpers.upgrade.scripts.clients import (
    client_migration_status_message,
    CLIENT_STATUS_SUCCESS,
    CLIENT_STATUS_FAILED_INVALID,
    CLIENT_STATUS_FAILED_MISSING_ID,
    CLIENT_STATUS_FAILED_MISSING_SECRET,
    CLIENT_STATUS_FAILED_MISSING_SCOPES,
    CLIENT_STATUS_SKIPPED_MIGRATED,
)
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.client import create_old_client_file, get_yaml_data
from spinta.utils.config import get_clients_path, get_keymap_path, get_id_path
from spinta.utils.types import is_str_uuid


@pytest.mark.parametrize("scopes", [["spinta_getall"], ["uapi:/:getall"]])
def test_upgrade_clients_detect_upgrade(
    context,
    rc,
    cli: SpintaCliRunner,
    tmp_path: pathlib.Path,
    scopes: list,
):
    clients_path = get_clients_path(tmp_path)
    os.makedirs(clients_path, exist_ok=True)
    rc = rc.fork({"config_path": str(tmp_path), "default_auth_client": None})

    # Create already existing file, to imitate old structure
    create_old_client_file(clients_path, {"client_id": "TEST", "client_secret_hash": "secret", "scopes": scopes})
    items = os.listdir(clients_path)
    assert items == ["TEST.yml"]

    result = cli.invoke(rc, ["upgrade", Script.CLIENTS.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.CLIENTS.value, ScriptStatus.REQUIRED) in result.stdout
    assert client_migration_status_message("TEST.yml", CLIENT_STATUS_SUCCESS) in result.stdout
    assert "Created keymap with 1 users" in result.stdout

    # Check is file still exists and if new folders got created
    items = os.listdir(clients_path)
    assert Counter(items) == Counter(["TEST.yml", "helpers", "id"])

    # Ensure keymap.yml exists and it only contains `TEST` client.
    keymap_path = get_keymap_path(clients_path)
    assert keymap_path.exists()
    keymap = get_yaml_data(keymap_path)

    assert list(keymap.keys()) == ["TEST"]
    test_id = keymap["TEST"]
    assert is_str_uuid(test_id)

    # Check migrated client data structure, it should be similar to old (with `client_name` added)
    client_file_path = get_client_file_path(clients_path, test_id)
    assert client_file_path.exists()

    client_data = get_yaml_data(client_file_path)
    assert client_data == {
        "client_id": test_id,
        "client_name": "TEST",
        "client_secret_hash": "secret",
        "scopes": scopes,
    }


@pytest.mark.parametrize(
    "scopes, bigger_scopes",
    [
        (["spinta_getall"], ["spinta_getall", "spinta_update"]),
        (["uapi:/:getall"], ["uapi:/:getall", "uapi:/:update"]),
    ],
)
def test_upgrade_clients_detect_upgrade_multiple(
    context, rc, cli: SpintaCliRunner, tmp_path: pathlib.Path, scopes: list, bigger_scopes: list
):
    clients_path = get_clients_path(tmp_path)
    os.makedirs(clients_path, exist_ok=True)
    rc = rc.fork({"config_path": str(tmp_path), "default_auth_client": None})

    # Create already existing file, to imitate old structure
    create_old_client_file(clients_path, {"client_id": "TEST", "client_secret_hash": "secret", "scopes": scopes})

    create_old_client_file(clients_path, {"client_id": "NEW", "client_secret_hash": "secret", "scopes": bigger_scopes})
    items = os.listdir(clients_path)
    assert Counter(items) == Counter(["TEST.yml", "NEW.yml"])

    result = cli.invoke(rc, ["upgrade", Script.CLIENTS.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.CLIENTS.value, ScriptStatus.REQUIRED) in result.stdout
    assert client_migration_status_message("NEW.yml", CLIENT_STATUS_SUCCESS) in result.stdout
    assert client_migration_status_message("TEST.yml", CLIENT_STATUS_SUCCESS) in result.stdout
    assert "Created keymap with 2 users" in result.stdout

    # Check is file still exists and if new folders got created
    items = os.listdir(clients_path)
    assert Counter(items) == Counter(["TEST.yml", "NEW.yml", "helpers", "id"])

    # Ensure keymap.yml exists and it only contains `TEST` client.
    keymap_path = get_keymap_path(clients_path)
    assert keymap_path.exists()
    keymap = get_yaml_data(keymap_path)

    assert Counter(list(keymap.keys())) == Counter(["TEST", "NEW"])
    test_id = keymap["TEST"]
    assert is_str_uuid(test_id)

    # Check `TEST` migrated client data structure, it should be similar to old (with `client_name` added)
    client_file_path = get_client_file_path(clients_path, test_id)
    assert client_file_path.exists()

    client_data = get_yaml_data(client_file_path)
    assert client_data == {
        "client_id": test_id,
        "client_name": "TEST",
        "client_secret_hash": "secret",
        "scopes": scopes,
    }

    new_id = keymap["NEW"]
    assert is_str_uuid(new_id)

    # Check `NEW` migrated client data structure, it should be similar to old (with `client_name` added)
    client_file_path = get_client_file_path(clients_path, new_id)
    assert client_file_path.exists()

    client_data = get_yaml_data(client_file_path)
    assert client_data == {
        "client_id": new_id,
        "client_name": "NEW",
        "client_secret_hash": "secret",
        "scopes": bigger_scopes,
    }


@pytest.mark.parametrize("scopes", [["spinta_getall"], ["uapi:/:getall"]])
def test_upgrade_clients_detect_upgrade_folders_already_exist(
    context,
    rc,
    cli: SpintaCliRunner,
    tmp_path: pathlib.Path,
    scopes: list,
):
    clients_path = get_clients_path(tmp_path)

    # Create emtpy folders and keymap.yml (imitate running empty project with _ensure_config_dir)
    ensure_client_folders_exist(clients_path)

    rc = rc.fork({"config_path": str(tmp_path), "default_auth_client": None})

    # Create already existing file, to imitate old structure
    create_old_client_file(clients_path, {"client_id": "TEST", "client_secret_hash": "secret", "scopes": scopes})
    items = os.listdir(clients_path)
    assert Counter(items) == Counter(["TEST.yml", "helpers", "id"])

    # Check if keymap exists and the data is empty
    keymap_path = get_keymap_path(clients_path)
    assert keymap_path.exists()

    keymap = get_yaml_data(keymap_path)
    assert keymap is None

    # Check if id folder exists and there are no subfolders
    id_path = get_id_path(clients_path)
    items = os.listdir(id_path)
    assert items == []

    # Run upgrade in normal mode
    result = cli.invoke(rc, ["upgrade", Script.CLIENTS.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.CLIENTS.value, ScriptStatus.REQUIRED) in result.stdout
    assert client_migration_status_message("TEST.yml", CLIENT_STATUS_SUCCESS) in result.stdout
    assert "Created keymap with 1 users" in result.stdout

    # Check if `TEST` got migrated
    keymap = get_yaml_data(keymap_path)
    assert list(keymap.keys()) == ["TEST"]
    test_id = keymap["TEST"]

    assert is_str_uuid(test_id)

    client_file_path = get_client_file_path(clients_path, test_id)
    assert client_file_path.exists()

    client_data = get_yaml_data(client_file_path)
    assert client_data == {
        "client_id": test_id,
        "client_name": "TEST",
        "client_secret_hash": "secret",
        "scopes": scopes,
    }


@pytest.mark.parametrize("scopes", [["spinta_getall"], ["uapi:/:getall"]])
def test_upgrade_clients_skip_upgrade(
    context,
    rc,
    cli: SpintaCliRunner,
    tmp_path: pathlib.Path,
    scopes: list,
):
    clients_path = get_clients_path(tmp_path)

    # Create emtpy folders and keymap.yml (imitate running empty project with _ensure_config_dir)
    ensure_client_folders_exist(clients_path)

    rc = rc.fork({"config_path": str(tmp_path), "default_auth_client": None})

    # Create and migrate files
    create_old_client_file(clients_path, {"client_id": "TEST", "client_secret_hash": "secret", "scopes": scopes})
    result = cli.invoke(rc, ["upgrade", Script.CLIENTS.value])
    assert script_check_status_message(Script.CLIENTS.value, ScriptStatus.REQUIRED) in result.stdout
    assert client_migration_status_message("TEST.yml", CLIENT_STATUS_SUCCESS) in result.stdout
    assert "Created keymap with 1 users" in result.stdout

    # Run again
    result = cli.invoke(rc, ["upgrade", Script.CLIENTS.value])
    assert script_check_status_message(Script.CLIENTS.value, ScriptStatus.PASSED) in result.stdout
    assert "Created keymap" not in result.stdout

    # Add new client
    create_old_client_file(clients_path, {"client_id": "NEW", "client_secret_hash": "secret", "scopes": scopes})
    result = cli.invoke(rc, ["upgrade", Script.CLIENTS.value])
    assert script_check_status_message(Script.CLIENTS.value, ScriptStatus.PASSED) in result.stdout
    assert "Created keymap" not in result.stdout

    keymap_path = get_keymap_path(clients_path)
    assert keymap_path.exists()

    keymap = get_yaml_data(keymap_path)
    assert list(keymap.keys()) == ["TEST"]


def test_upgrade_clients_invalid_client(context, rc, cli: SpintaCliRunner, tmp_path: pathlib.Path):
    clients_path = get_clients_path(tmp_path)
    os.makedirs(clients_path, exist_ok=True)
    rc = rc.fork({"config_path": str(tmp_path), "default_auth_client": None})

    # Create already existing file, to imitate old structure
    create_old_client_file(clients_path, {}, "TEST")
    items = os.listdir(clients_path)
    assert items == ["TEST.yml"]

    result = cli.invoke(rc, ["upgrade", Script.CLIENTS.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.CLIENTS.value, ScriptStatus.REQUIRED) in result.stdout
    assert client_migration_status_message("TEST.yml", CLIENT_STATUS_FAILED_INVALID) in result.stdout
    assert "Created keymap with 0 users" in result.stdout

    # Check is file still exists and if new folders got created
    items = os.listdir(clients_path)
    assert Counter(items) == Counter(["TEST.yml", "helpers", "id"])

    # Ensure keymap.yml exists and it only contains `TEST` client.
    keymap_path = get_keymap_path(clients_path)
    assert keymap_path.exists()
    keymap = get_yaml_data(keymap_path)

    assert list(keymap.keys()) == []

    id_path = get_id_path(clients_path)
    items = os.listdir(id_path)
    assert items == []


@pytest.mark.parametrize("scopes", [["spinta_getall"], ["uapi:/:getall"]])
def test_upgrade_clients_invalid_client_missing_id(
    context,
    rc,
    cli: SpintaCliRunner,
    tmp_path: pathlib.Path,
    scopes: list,
):
    clients_path = get_clients_path(tmp_path)
    os.makedirs(clients_path, exist_ok=True)
    rc = rc.fork({"config_path": str(tmp_path), "default_auth_client": None})

    # Create already existing file, to imitate old structure
    create_old_client_file(clients_path, {"client_secret_hash": "secret", "scopes": scopes}, "TEST")
    items = os.listdir(clients_path)
    assert items == ["TEST.yml"]

    result = cli.invoke(rc, ["upgrade", Script.CLIENTS.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.CLIENTS.value, ScriptStatus.REQUIRED) in result.stdout
    assert client_migration_status_message("TEST.yml", CLIENT_STATUS_FAILED_MISSING_ID) in result.stdout
    assert "Created keymap with 0 users" in result.stdout

    # Check is file still exists and if new folders got created
    items = os.listdir(clients_path)
    assert Counter(items) == Counter(["TEST.yml", "helpers", "id"])

    # Ensure keymap.yml exists and it only contains `TEST` client.
    keymap_path = get_keymap_path(clients_path)
    assert keymap_path.exists()
    keymap = get_yaml_data(keymap_path)

    assert list(keymap.keys()) == []

    id_path = get_id_path(clients_path)
    items = os.listdir(id_path)
    assert items == []


@pytest.mark.parametrize("scopes", [["spinta_getall"], ["uapi:/:getall"]])
def test_upgrade_clients_invalid_client_missing_secret(
    context,
    rc,
    cli: SpintaCliRunner,
    tmp_path: pathlib.Path,
    scopes: list,
):
    clients_path = get_clients_path(tmp_path)
    os.makedirs(clients_path, exist_ok=True)
    rc = rc.fork({"config_path": str(tmp_path), "default_auth_client": None})

    # Create already existing file, to imitate old structure
    create_old_client_file(clients_path, {"client_id": "TEST", "scopes": scopes}, "TEST")
    items = os.listdir(clients_path)
    assert items == ["TEST.yml"]

    result = cli.invoke(rc, ["upgrade", Script.CLIENTS.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.CLIENTS.value, ScriptStatus.REQUIRED) in result.stdout
    assert client_migration_status_message("TEST.yml", CLIENT_STATUS_FAILED_MISSING_SECRET) in result.stdout
    assert "Created keymap with 0 users" in result.stdout

    # Check is file still exists and if new folders got created
    items = os.listdir(clients_path)
    assert Counter(items) == Counter(["TEST.yml", "helpers", "id"])

    # Ensure keymap.yml exists and it only contains `TEST` client.
    keymap_path = get_keymap_path(clients_path)
    assert keymap_path.exists()
    keymap = get_yaml_data(keymap_path)

    assert list(keymap.keys()) == []

    id_path = get_id_path(clients_path)
    items = os.listdir(id_path)
    assert items == []


def test_upgrade_clients_invalid_client_missing_scope(context, rc, cli: SpintaCliRunner, tmp_path: pathlib.Path):
    clients_path = get_clients_path(tmp_path)
    os.makedirs(clients_path, exist_ok=True)
    rc = rc.fork({"config_path": str(tmp_path), "default_auth_client": None})

    # Create already existing file, to imitate old structure
    create_old_client_file(
        clients_path,
        {
            "client_id": "TEST",
            "client_secret_hash": "secret",
        },
        "TEST",
    )
    items = os.listdir(clients_path)
    assert items == ["TEST.yml"]

    result = cli.invoke(rc, ["upgrade", Script.CLIENTS.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.CLIENTS.value, ScriptStatus.REQUIRED) in result.stdout
    assert client_migration_status_message("TEST.yml", CLIENT_STATUS_FAILED_MISSING_SCOPES) in result.stdout
    assert "Created keymap with 0 users" in result.stdout

    # Check is file still exists and if new folders got created
    items = os.listdir(clients_path)
    assert Counter(items) == Counter(["TEST.yml", "helpers", "id"])

    # Ensure keymap.yml exists and it only contains `TEST` client.
    keymap_path = get_keymap_path(clients_path)
    assert keymap_path.exists()
    keymap = get_yaml_data(keymap_path)

    assert list(keymap.keys()) == []

    id_path = get_id_path(clients_path)
    items = os.listdir(id_path)
    assert items == []


@pytest.mark.parametrize(
    "scopes, bigger_scopes",
    [
        (["spinta_getall"], ["spinta_getall", "spinta_update"]),
        (["uapi:/:getall"], ["uapi:/:getall", "uapi:/:update"]),
    ],
)
def test_upgrade_clients_force_upgrade(
    context, rc, cli: SpintaCliRunner, tmp_path: pathlib.Path, scopes: list, bigger_scopes: list
):
    clients_path = get_clients_path(tmp_path)

    # Create emtpy folders and keymap.yml (imitate running empty project with _ensure_config_dir)
    ensure_client_folders_exist(clients_path)

    rc = rc.fork({"config_path": str(tmp_path), "default_auth_client": None})

    # Create and migrate files
    create_old_client_file(clients_path, {"client_id": "TEST", "client_secret_hash": "secret", "scopes": scopes})
    result = cli.invoke(rc, ["upgrade", Script.CLIENTS.value])
    assert script_check_status_message(Script.CLIENTS.value, ScriptStatus.REQUIRED) in result.stdout

    # Add new client
    create_old_client_file(clients_path, {"client_id": "NEW", "client_secret_hash": "secret", "scopes": bigger_scopes})
    result = cli.invoke(rc, ["upgrade", Script.CLIENTS.value])
    assert script_check_status_message(Script.CLIENTS.value, ScriptStatus.PASSED) in result.stdout

    keymap_path = get_keymap_path(clients_path)
    assert keymap_path.exists()

    keymap = get_yaml_data(keymap_path)
    assert list(keymap.keys()) == ["TEST"]

    # Force check
    result = cli.invoke(rc, ["upgrade", Script.CLIENTS.value, "-f"])
    assert script_check_status_message(Script.CLIENTS.value, ScriptStatus.FORCED) in result.stdout
    assert client_migration_status_message("NEW.yml", CLIENT_STATUS_SUCCESS) in result.stdout
    assert client_migration_status_message("TEST.yml", CLIENT_STATUS_SKIPPED_MIGRATED) in result.stdout
    assert "Created keymap with 2 users" in result.stdout

    keymap_path = get_keymap_path(clients_path)
    assert keymap_path.exists()

    keymap = get_yaml_data(keymap_path)
    assert Counter(list(keymap.keys())) == Counter(["TEST", "NEW"])

    new_id = keymap["NEW"]

    assert is_str_uuid(new_id)

    client_file_path = get_client_file_path(clients_path, new_id)
    assert client_file_path.exists()

    client_data = get_yaml_data(client_file_path)
    assert client_data == {
        "client_id": new_id,
        "client_name": "NEW",
        "client_secret_hash": "secret",
        "scopes": bigger_scopes,
    }


@pytest.mark.parametrize("scopes", [["spinta_getall", "spinta_update"], ["uapi:/:getall", "uapi:/:update"]])
def test_upgrade_clients_force_upgrade_destructive(
    context,
    rc,
    cli: SpintaCliRunner,
    tmp_path: pathlib.Path,
    scopes: list,
):
    clients_path = get_clients_path(tmp_path)

    # Create emtpy folders and keymap.yml (imitate running empty project with _ensure_config_dir)
    ensure_client_folders_exist(clients_path)

    rc = rc.fork({"config_path": str(tmp_path), "default_auth_client": None})

    # Create and migrate files
    create_old_client_file(
        clients_path, {"client_id": "TEST", "client_secret_hash": "secret", "scopes": ["uapi:/:getall"]}
    )
    result = cli.invoke(rc, ["upgrade", Script.CLIENTS.value])
    assert script_check_status_message(Script.CLIENTS.value, ScriptStatus.REQUIRED) in result.stdout

    keymap_path = get_keymap_path(clients_path)
    assert keymap_path.exists()

    keymap = get_yaml_data(keymap_path)
    assert Counter(list(keymap.keys())) == Counter(["TEST"])

    test_id = keymap["TEST"]
    assert is_str_uuid(test_id)

    # Update client scopes
    create_old_client_file(
        clients_path,
        {"client_id": "TEST", "client_secret_hash": "secret", "scopes": scopes},
    )
    result = cli.invoke(rc, ["upgrade", Script.CLIENTS.value])
    assert script_check_status_message(Script.CLIENTS.value, ScriptStatus.PASSED) in result.stdout

    result = cli.invoke(rc, ["upgrade", Script.CLIENTS.value, "-f"])
    assert script_check_status_message(Script.CLIENTS.value, ScriptStatus.FORCED) in result.stdout
    assert client_migration_status_message("TEST.yml", CLIENT_STATUS_SKIPPED_MIGRATED) in result.stdout

    # Force check
    result = cli.invoke(rc, ["upgrade", Script.CLIENTS.value, "-f", "-d"])
    assert script_check_status_message(Script.CLIENTS.value, ScriptStatus.FORCED) in result.stdout
    assert client_migration_status_message("TEST.yml", CLIENT_STATUS_SUCCESS) in result.stdout
    assert "DESTRUCTIVE MODE" in result.stdout

    keymap = get_yaml_data(keymap_path)
    assert Counter(list(keymap.keys())) == Counter(["TEST"])

    new_test_id = keymap["TEST"]
    assert is_str_uuid(new_test_id)
    assert test_id == new_test_id

    client_file_path = get_client_file_path(clients_path, new_test_id)
    assert client_file_path.exists()

    client_data = get_yaml_data(client_file_path)
    assert client_data == {
        "client_id": test_id,
        "client_name": "TEST",
        "client_secret_hash": "secret",
        "scopes": scopes,
    }
