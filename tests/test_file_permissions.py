"""Test that sensitive files are created with secure permissions."""

import os
import uuid

from spinta.auth import (
    create_client_file,
    ensure_client_folders_exist,
    gen_auth_server_keys,
    get_helpers_path,
    get_id_path,
    get_keymap_path,
)
from spinta.client import add_client_credentials


def test_private_key_permissions(tmp_path):
    """Private key should be created with 600 permissions."""
    priv, pub = gen_auth_server_keys(tmp_path)

    # Check private key
    perms = oct(os.stat(priv).st_mode)[-3:]
    assert perms == "600", f"Private key has {perms}, expected 600"

    # Check public key
    perms = oct(os.stat(pub).st_mode)[-3:]
    assert perms == "644", f"Public key has {perms}, expected 644"

    # Check keys directory
    keys_dir = tmp_path / "keys"
    perms = oct(os.stat(keys_dir).st_mode)[-3:]
    assert perms == "700", f"Keys directory has {perms}, expected 700"


def test_client_file_permissions(tmp_path):
    """Client credential files should be created with 600 permissions."""
    clients_path = tmp_path / "clients"
    ensure_client_folders_exist(clients_path)

    # Create a client
    client_id = str(uuid.uuid4())
    client_file, data = create_client_file(
        clients_path,
        "test_client",
        client_id,
        scopes=["test:scope"],
    )

    # Check client file permissions
    perms = oct(os.stat(client_file).st_mode)[-3:]
    assert perms == "600", f"Client file has {perms}, expected 600"

    # Check keymap permissions
    keymap_path = get_keymap_path(clients_path)
    perms = oct(os.stat(keymap_path).st_mode)[-3:]
    assert perms == "600", f"Keymap file has {perms}, expected 600"


def test_client_directories_permissions(tmp_path):
    """Client directories should be created with 700 permissions."""
    clients_path = tmp_path / "clients"
    ensure_client_folders_exist(clients_path)

    # Check main clients directory
    perms = oct(os.stat(clients_path).st_mode)[-3:]
    assert perms == "700", f"Clients directory has {perms}, expected 700"

    # Check helpers directory
    helpers_path = get_helpers_path(clients_path)
    perms = oct(os.stat(helpers_path).st_mode)[-3:]
    assert perms == "700", f"Helpers directory has {perms}, expected 700"

    # Check id directory
    id_path = get_id_path(clients_path)
    perms = oct(os.stat(id_path).st_mode)[-3:]
    assert perms == "700", f"ID directory has {perms}, expected 700"


def test_client_id_subdirectories_permissions(tmp_path):
    """Client ID subdirectories should be created with 700 permissions."""
    clients_path = tmp_path / "clients"
    ensure_client_folders_exist(clients_path)

    # Create a client with a valid UUID
    client_id = str(uuid.uuid4())
    client_file, data = create_client_file(
        clients_path,
        "test_client",
        client_id,
    )

    # Check first level subdirectory (first 2 chars of UUID)
    id_path = get_id_path(clients_path)
    level1 = id_path / client_id[:2]
    perms = oct(os.stat(level1).st_mode)[-3:]
    assert perms == "700", f"Level 1 directory has {perms}, expected 700"

    # Check second level subdirectory (chars 2-4 of UUID)
    level2 = level1 / client_id[2:4]
    perms = oct(os.stat(level2).st_mode)[-3:]
    assert perms == "700", f"Level 2 directory has {perms}, expected 700"


def test_credentials_file_permissions(tmp_path):
    """Client credentials config file should be created with 600 permissions."""
    creds_file = tmp_path / "credentials.cfg"

    add_client_credentials(
        creds_file,
        "http://localhost:8000",
        client="test_client",
        secret="test_secret",
        scopes=["test:scope"],
    )

    # Check credentials file permissions
    perms = oct(os.stat(creds_file).st_mode)[-3:]
    assert perms == "600", f"Credentials file has {perms}, expected 600"


def test_update_client_keymap_permissions(tmp_path):
    """Updating keymap should preserve secure permissions."""
    clients_path = tmp_path / "clients"
    ensure_client_folders_exist(clients_path)

    # Create a client
    client_id = str(uuid.uuid4())
    client_file, data = create_client_file(
        clients_path,
        "test_client",
        client_id,
    )

    # Get keymap file
    keymap_path = get_keymap_path(clients_path)

    # Check keymap permissions after creation
    perms = oct(os.stat(keymap_path).st_mode)[-3:]
    assert perms == "600", f"Keymap file has {perms}, expected 600"

    # Create another client (updates keymap)
    client_id2 = str(uuid.uuid4())
    client_file2, data2 = create_client_file(
        clients_path,
        "test_client2",
        client_id2,
    )

    # Check keymap permissions are still secure after update
    perms = oct(os.stat(keymap_path).st_mode)[-3:]
    assert perms == "600", f"Updated keymap file has {perms}, expected 600"
