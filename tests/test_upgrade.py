import os
import pathlib
import shutil

import pytest

from spinta.auth import yml
from spinta.cli.helpers.upgrade.components import Script
from spinta.core.config import RawConfig
from spinta.exceptions import ClientsMigrationRequired
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.client import create_old_client_file
from spinta.testing.utils import get_error_codes
from spinta.utils.config import get_clients_path, get_keymap_path, get_id_path, get_helpers_path
from tests.test_api import ensure_temp_context_and_app


@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_getall"],
        ["uapi:/:getall"]
    ]
)
def test_detect_upgrade_clients_only_yml(
    tmp_path: pathlib.Path,
    rc: RawConfig,
    scope: list,
):
    clients_path = get_clients_path(tmp_path)
    os.makedirs(clients_path, exist_ok=True)

    create_old_client_file(
        clients_path, {"client_id": "NEW", "client_secret_hash": "secret", "scopes": scope}
    )

    with pytest.raises(ClientsMigrationRequired):
        ensure_temp_context_and_app(rc, tmp_path)


@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_getall"],
        ["uapi:/:getall"]
    ]
)
def test_detect_upgrade_clients_no_keymap(
    tmp_path: pathlib.Path,
    cli: SpintaCliRunner,
    rc: RawConfig,
    scope: list
):
    rc = rc.fork({"config_path": tmp_path})

    clients_path = get_clients_path(tmp_path)
    os.makedirs(clients_path, exist_ok=True)

    create_old_client_file(
        clients_path, {"client_id": "NEW", "client_secret_hash": "secret", "scopes": scope}
    )

    cli.invoke(rc, ["upgrade", Script.CLIENTS.value])

    keymap_path = get_keymap_path(clients_path)
    assert keymap_path.exists()

    id_path = get_helpers_path(clients_path)
    assert id_path.exists()

    # Should run normally
    ensure_temp_context_and_app(rc, tmp_path)

    # Remove keymap file
    os.remove(keymap_path)
    with pytest.raises(ClientsMigrationRequired):
        ensure_temp_context_and_app(rc, tmp_path)


@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_getall"],
        ["uapi:/:getall"]
    ]
)
def test_detect_upgrade_clients_no_id(
    tmp_path: pathlib.Path,
    cli: SpintaCliRunner,
    rc: RawConfig,
    scope: list,
):
    rc = rc.fork({"config_path": tmp_path})

    clients_path = get_clients_path(tmp_path)
    os.makedirs(clients_path, exist_ok=True)

    create_old_client_file(
        clients_path, {"client_id": "NEW", "client_secret_hash": "secret", "scopes": scope}
    )

    cli.invoke(rc, ["upgrade", Script.CLIENTS.value])

    keymap_path = get_keymap_path(clients_path)
    assert keymap_path.exists()

    id_path = get_helpers_path(clients_path)
    assert id_path.exists()

    # Should run normally
    ensure_temp_context_and_app(rc, tmp_path)

    # Remove id folder
    shutil.rmtree(id_path)
    with pytest.raises(ClientsMigrationRequired):
        ensure_temp_context_and_app(rc, tmp_path)


@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_getall"],
        ["uapi:/:getall"]
    ]
)
def test_detect_upgrade_clients_empty_keymap(
    tmp_path: pathlib.Path,
    cli: SpintaCliRunner,
    rc: RawConfig,
    scope: list,
):
    rc = rc.fork({"config_path": tmp_path})

    clients_path = get_clients_path(tmp_path)
    os.makedirs(clients_path, exist_ok=True)

    create_old_client_file(
        clients_path, {"client_id": "NEW", "client_secret_hash": "secret", "scopes": scope}
    )

    cli.invoke(rc, ["upgrade", Script.CLIENTS.value])

    keymap_path = get_keymap_path(clients_path)
    assert keymap_path.exists()

    id_path = get_helpers_path(clients_path)
    assert id_path.exists()

    # Should run normally
    ensure_temp_context_and_app(rc, tmp_path)

    # Clear keymap data
    yml.dump({}, keymap_path)
    with pytest.raises(ClientsMigrationRequired):
        ensure_temp_context_and_app(rc, tmp_path)


@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_getall"],
        ["uapi:/:getall"]
    ]
)
def test_detect_upgrade_clients_empty_id(
    tmp_path: pathlib.Path,
    cli: SpintaCliRunner,
    rc: RawConfig,
    scope: list,
):
    rc = rc.fork({"config_path": tmp_path})

    clients_path = get_clients_path(tmp_path)
    os.makedirs(clients_path, exist_ok=True)

    create_old_client_file(
        clients_path, {"client_id": "NEW", "client_secret_hash": "secret", "scopes": scope}
    )

    cli.invoke(rc, ["upgrade", Script.CLIENTS.value])

    keymap_path = get_keymap_path(clients_path)
    assert keymap_path.exists()

    id_path = get_helpers_path(clients_path)
    assert id_path.exists()

    # Should run normally
    ensure_temp_context_and_app(rc, tmp_path)

    # Clear id subfolders
    shutil.rmtree(id_path)
    os.makedirs(id_path, exist_ok=True)
    with pytest.raises(ClientsMigrationRequired):
        ensure_temp_context_and_app(rc, tmp_path)


def test_detect_upgrade_clients_keymap_missing(
    tmp_path: pathlib.Path,
    rc: RawConfig,
):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    keymap_path = get_keymap_path(get_clients_path(tmp_path))
    assert keymap_path.exists()

    os.remove(keymap_path)
    assert not keymap_path.exists()

    resp = app.get("/:ns")
    assert resp.status_code == 500
    assert get_error_codes(resp.json()) == ["ClientsKeymapNotFound"]


def test_detect_upgrade_clients_id_missing(
    tmp_path: pathlib.Path,
    rc: RawConfig,
):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    id_path = get_id_path(get_clients_path(tmp_path))
    assert id_path.exists()

    shutil.rmtree(id_path)
    assert not id_path.exists()

    resp = app.get("/:ns")
    assert resp.status_code == 500
    assert get_error_codes(resp.json()) == ["ClientsIdFolderNotFound"]


def test_detect_upgrade_empty_skip(
    tmp_path: pathlib.Path,
    rc: RawConfig,
):
    context, app = ensure_temp_context_and_app(rc, tmp_path)

    resp = app.get("/:ns")
    assert resp.status_code == 200
