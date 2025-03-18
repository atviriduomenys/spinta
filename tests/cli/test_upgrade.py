import os
import pathlib
from collections import Counter

from spinta.auth import ensure_client_folders_exist, get_client_file_path
from spinta.cli.helpers.upgrade.clients import client_migration_status_message, CLIENT_STATUS_SUCCESS, \
    CLIENT_STATUS_FAILED_INVALID, CLIENT_STATUS_FAILED_MISSING_ID, CLIENT_STATUS_FAILED_MISSING_SECRET, \
    CLIENT_STATUS_FAILED_MISSING_SCOPES, CLIENT_STATUS_SKIPPED_MIGRATED
from spinta.cli.helpers.upgrade.components import UPGRADE_CLIENTS_SCRIPT
from spinta.cli.helpers.upgrade.helpers import script_check_status_message
from spinta.cli.helpers.upgrade.scripts import UPGRADE_CHECK_STATUS_REQUIRED, UPGRADE_CHECK_STATUS_PASSED, \
    UPGRADE_CHECK_STATUS_FORCED
from spinta.exceptions import UpgradeScriptNotFound
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
import pytest

from spinta.testing.client import create_old_client_file, get_yaml_data
from spinta.testing.manifest import load_manifest
from spinta.testing.tabular import create_tabular_manifest
from spinta.utils.config import get_clients_path, get_keymap_path, get_id_path
from spinta.utils.types import is_str_uuid


def test_upgrade_invalid_script_name(context,
    rc,
    cli: SpintaCliRunner
):
    with pytest.raises(UpgradeScriptNotFound):
        result = cli.invoke(rc, [
            'upgrade',
            '-r', 'UNAVAILABLE'
        ], fail=False)
        raise result.exception


def test_upgrade_clients_detect_upgrade(context,
    rc,
    cli: SpintaCliRunner,
    tmp_path: pathlib.Path
):
    clients_path = get_clients_path(tmp_path)
    os.makedirs(clients_path, exist_ok=True)
    rc = rc.fork({
        'config_path': str(tmp_path),
        'default_auth_client': None
    })

    # Create already existing file, to imitate old structure
    create_old_client_file(
        clients_path, {
            'client_id': 'TEST',
            'client_secret_hash': 'secret',
            'scopes': [
                'spinta_getall'
            ]
        }
    )
    items = os.listdir(clients_path)
    assert items == ['TEST.yml']

    result = cli.invoke(rc, [
        'upgrade',
        '-r', UPGRADE_CLIENTS_SCRIPT
    ])
    assert result.exit_code == 0
    assert script_check_status_message(UPGRADE_CLIENTS_SCRIPT, UPGRADE_CHECK_STATUS_REQUIRED) in result.stdout
    assert client_migration_status_message('TEST.yml', CLIENT_STATUS_SUCCESS) in result.stdout
    assert "Created keymap with 1 users" in result.stdout

    # Check is file still exists and if new folders got created
    items = os.listdir(clients_path)
    assert Counter(items) == Counter(['TEST.yml', 'helpers', 'id'])

    # Ensure keymap.yml exists and it only contains `TEST` client.
    keymap_path = get_keymap_path(clients_path)
    assert keymap_path.exists()
    keymap = get_yaml_data(keymap_path)

    assert list(keymap.keys()) == ['TEST']
    test_id = keymap['TEST']
    assert is_str_uuid(test_id)

    # Check migrated client data structure, it should be similar to old (with `client_name` added)
    client_file_path = get_client_file_path(clients_path, test_id)
    assert client_file_path.exists()

    client_data = get_yaml_data(client_file_path)
    assert client_data == {
        'client_id': test_id,
        'client_name': 'TEST',
        'client_secret_hash': 'secret',
        'scopes': [
            'spinta_getall'
        ]
    }


def test_upgrade_clients_detect_upgrade_multiple(context,
    rc,
    cli: SpintaCliRunner,
    tmp_path: pathlib.Path
):
    clients_path = get_clients_path(tmp_path)
    os.makedirs(clients_path, exist_ok=True)
    rc = rc.fork({
        'config_path': str(tmp_path),
        'default_auth_client': None
    })

    # Create already existing file, to imitate old structure
    create_old_client_file(
        clients_path, {
            'client_id': 'TEST',
            'client_secret_hash': 'secret',
            'scopes': [
                'spinta_getall'
            ]
        }
    )

    create_old_client_file(
        clients_path, {
            'client_id': 'NEW',
            'client_secret_hash': 'secret',
            'scopes': [
                'spinta_getall'
            ]
        }
    )
    items = os.listdir(clients_path)
    assert Counter(items) == Counter(['TEST.yml', 'NEW.yml'])

    result = cli.invoke(rc, [
        'upgrade',
        '-r', UPGRADE_CLIENTS_SCRIPT
    ])
    assert result.exit_code == 0
    assert script_check_status_message(UPGRADE_CLIENTS_SCRIPT, UPGRADE_CHECK_STATUS_REQUIRED) in result.stdout
    assert client_migration_status_message('NEW.yml', CLIENT_STATUS_SUCCESS) in result.stdout
    assert client_migration_status_message('TEST.yml', CLIENT_STATUS_SUCCESS) in result.stdout
    assert "Created keymap with 2 users" in result.stdout

    # Check is file still exists and if new folders got created
    items = os.listdir(clients_path)
    assert Counter(items) == Counter(['TEST.yml', 'NEW.yml', 'helpers', 'id'])

    # Ensure keymap.yml exists and it only contains `TEST` client.
    keymap_path = get_keymap_path(clients_path)
    assert keymap_path.exists()
    keymap = get_yaml_data(keymap_path)

    assert Counter(list(keymap.keys())) == Counter(['TEST', 'NEW'])
    test_id = keymap['TEST']
    assert is_str_uuid(test_id)

    # Check `TEST` migrated client data structure, it should be similar to old (with `client_name` added)
    client_file_path = get_client_file_path(clients_path, test_id)
    assert client_file_path.exists()

    client_data = get_yaml_data(client_file_path)
    assert client_data == {
        'client_id': test_id,
        'client_name': 'TEST',
        'client_secret_hash': 'secret',
        'scopes': [
            'spinta_getall'
        ]
    }

    new_id = keymap['NEW']
    assert is_str_uuid(new_id)

    # Check `NEW` migrated client data structure, it should be similar to old (with `client_name` added)
    client_file_path = get_client_file_path(clients_path, new_id)
    assert client_file_path.exists()

    client_data = get_yaml_data(client_file_path)
    assert client_data == {
        'client_id': new_id,
        'client_name': 'NEW',
        'client_secret_hash': 'secret',
        'scopes': [
            'spinta_getall'
        ]
    }


def test_upgrade_clients_detect_upgrade_folders_already_exist(context,
    rc,
    cli: SpintaCliRunner,
    tmp_path: pathlib.Path
):
    clients_path = get_clients_path(tmp_path)

    # Create emtpy folders and keymap.yml (imitate running empty project with _ensure_config_dir)
    ensure_client_folders_exist(clients_path)

    rc = rc.fork({
        'config_path': str(tmp_path),
        'default_auth_client': None
    })

    # Create already existing file, to imitate old structure
    create_old_client_file(
        clients_path, {
            'client_id': 'TEST',
            'client_secret_hash': 'secret',
            'scopes': [
                'spinta_getall'
            ]
        }
    )
    items = os.listdir(clients_path)
    assert Counter(items) == Counter(['TEST.yml', 'helpers', 'id'])

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
    result = cli.invoke(rc, [
        'upgrade',
        '-r', UPGRADE_CLIENTS_SCRIPT
    ])
    assert result.exit_code == 0
    assert script_check_status_message(UPGRADE_CLIENTS_SCRIPT, UPGRADE_CHECK_STATUS_REQUIRED) in result.stdout
    assert client_migration_status_message('TEST.yml', CLIENT_STATUS_SUCCESS) in result.stdout
    assert "Created keymap with 1 users" in result.stdout

    # Check if `TEST` got migrated
    keymap = get_yaml_data(keymap_path)
    assert list(keymap.keys()) == ['TEST']
    test_id = keymap['TEST']

    assert is_str_uuid(test_id)

    client_file_path = get_client_file_path(clients_path, test_id)
    assert client_file_path.exists()

    client_data = get_yaml_data(client_file_path)
    assert client_data == {
        'client_id': test_id,
        'client_name': 'TEST',
        'client_secret_hash': 'secret',
        'scopes': [
            'spinta_getall'
        ]
    }


def test_upgrade_clients_skip_upgrade(context,
    rc,
    cli: SpintaCliRunner,
    tmp_path: pathlib.Path
):
    clients_path = get_clients_path(tmp_path)

    # Create emtpy folders and keymap.yml (imitate running empty project with _ensure_config_dir)
    ensure_client_folders_exist(clients_path)

    rc = rc.fork({
        'config_path': str(tmp_path),
        'default_auth_client': None
    })

    # Create and migrate files
    create_old_client_file(
        clients_path, {
            'client_id': 'TEST',
            'client_secret_hash': 'secret',
            'scopes': [
                'spinta_getall'
            ]
        }
    )
    result = cli.invoke(rc, [
        'upgrade',
        '-r', 'clients'
    ])
    assert script_check_status_message(UPGRADE_CLIENTS_SCRIPT, UPGRADE_CHECK_STATUS_REQUIRED) in result.stdout
    assert client_migration_status_message('TEST.yml', CLIENT_STATUS_SUCCESS) in result.stdout
    assert "Created keymap with 1 users" in result.stdout

    # Run again
    result = cli.invoke(rc, [
        'upgrade',
        '-r', UPGRADE_CLIENTS_SCRIPT
    ])
    assert script_check_status_message(UPGRADE_CLIENTS_SCRIPT, UPGRADE_CHECK_STATUS_PASSED) in result.stdout
    assert "Created keymap" not in result.stdout

    # Add new client
    create_old_client_file(
        clients_path, {
            'client_id': 'NEW',
            'client_secret_hash': 'secret',
            'scopes': [
                'spinta_getall'
            ]
        }
    )
    result = cli.invoke(rc, [
        'upgrade',
        '-r', UPGRADE_CLIENTS_SCRIPT
    ])
    assert script_check_status_message(UPGRADE_CLIENTS_SCRIPT, UPGRADE_CHECK_STATUS_PASSED) in result.stdout
    assert "Created keymap" not in result.stdout

    keymap_path = get_keymap_path(clients_path)
    assert keymap_path.exists()

    keymap = get_yaml_data(keymap_path)
    assert list(keymap.keys()) == ['TEST']


def test_upgrade_clients_invalid_client(context,
    rc,
    cli: SpintaCliRunner,
    tmp_path: pathlib.Path
):
    clients_path = get_clients_path(tmp_path)
    os.makedirs(clients_path, exist_ok=True)
    rc = rc.fork({
        'config_path': str(tmp_path),
        'default_auth_client': None
    })

    # Create already existing file, to imitate old structure
    create_old_client_file(
        clients_path, {}, 'TEST'
    )
    items = os.listdir(clients_path)
    assert items == ['TEST.yml']

    result = cli.invoke(rc, [
        'upgrade',
        '-r', UPGRADE_CLIENTS_SCRIPT
    ])
    assert result.exit_code == 0
    assert script_check_status_message(UPGRADE_CLIENTS_SCRIPT, UPGRADE_CHECK_STATUS_REQUIRED) in result.stdout
    assert client_migration_status_message('TEST.yml', CLIENT_STATUS_FAILED_INVALID) in result.stdout
    assert "Created keymap with 0 users" in result.stdout

    # Check is file still exists and if new folders got created
    items = os.listdir(clients_path)
    assert Counter(items) == Counter(['TEST.yml', 'helpers', 'id'])

    # Ensure keymap.yml exists and it only contains `TEST` client.
    keymap_path = get_keymap_path(clients_path)
    assert keymap_path.exists()
    keymap = get_yaml_data(keymap_path)

    assert list(keymap.keys()) == []

    id_path = get_id_path(clients_path)
    items = os.listdir(id_path)
    assert items == []


def test_upgrade_clients_invalid_client_missing_id(context,
    rc,
    cli: SpintaCliRunner,
    tmp_path: pathlib.Path
):
    clients_path = get_clients_path(tmp_path)
    os.makedirs(clients_path, exist_ok=True)
    rc = rc.fork({
        'config_path': str(tmp_path),
        'default_auth_client': None
    })

    # Create already existing file, to imitate old structure
    create_old_client_file(
        clients_path, {
            'client_secret_hash': 'secret',
            'scopes': [
                'spinta_getall'
            ]
        }, 'TEST'
    )
    items = os.listdir(clients_path)
    assert items == ['TEST.yml']

    result = cli.invoke(rc, [
        'upgrade',
        '-r', UPGRADE_CLIENTS_SCRIPT
    ])
    assert result.exit_code == 0
    assert script_check_status_message(UPGRADE_CLIENTS_SCRIPT, UPGRADE_CHECK_STATUS_REQUIRED) in result.stdout
    assert client_migration_status_message('TEST.yml', CLIENT_STATUS_FAILED_MISSING_ID) in result.stdout
    assert "Created keymap with 0 users" in result.stdout

    # Check is file still exists and if new folders got created
    items = os.listdir(clients_path)
    assert Counter(items) == Counter(['TEST.yml', 'helpers', 'id'])

    # Ensure keymap.yml exists and it only contains `TEST` client.
    keymap_path = get_keymap_path(clients_path)
    assert keymap_path.exists()
    keymap = get_yaml_data(keymap_path)

    assert list(keymap.keys()) == []

    id_path = get_id_path(clients_path)
    items = os.listdir(id_path)
    assert items == []


def test_upgrade_clients_invalid_client_missing_secret(context,
    rc,
    cli: SpintaCliRunner,
    tmp_path: pathlib.Path
):
    clients_path = get_clients_path(tmp_path)
    os.makedirs(clients_path, exist_ok=True)
    rc = rc.fork({
        'config_path': str(tmp_path),
        'default_auth_client': None
    })

    # Create already existing file, to imitate old structure
    create_old_client_file(
        clients_path, {
            'client_id': 'TEST',
            'scopes': [
                'spinta_getall'
            ]
        }, 'TEST'
    )
    items = os.listdir(clients_path)
    assert items == ['TEST.yml']

    result = cli.invoke(rc, [
        'upgrade',
        '-r', UPGRADE_CLIENTS_SCRIPT
    ])
    assert result.exit_code == 0
    assert script_check_status_message(UPGRADE_CLIENTS_SCRIPT, UPGRADE_CHECK_STATUS_REQUIRED) in result.stdout
    assert client_migration_status_message('TEST.yml', CLIENT_STATUS_FAILED_MISSING_SECRET) in result.stdout
    assert "Created keymap with 0 users" in result.stdout

    # Check is file still exists and if new folders got created
    items = os.listdir(clients_path)
    assert Counter(items) == Counter(['TEST.yml', 'helpers', 'id'])

    # Ensure keymap.yml exists and it only contains `TEST` client.
    keymap_path = get_keymap_path(clients_path)
    assert keymap_path.exists()
    keymap = get_yaml_data(keymap_path)

    assert list(keymap.keys()) == []

    id_path = get_id_path(clients_path)
    items = os.listdir(id_path)
    assert items == []


def test_upgrade_clients_invalid_client_missing_scopes(context,
    rc,
    cli: SpintaCliRunner,
    tmp_path: pathlib.Path
):
    clients_path = get_clients_path(tmp_path)
    os.makedirs(clients_path, exist_ok=True)
    rc = rc.fork({
        'config_path': str(tmp_path),
        'default_auth_client': None
    })

    # Create already existing file, to imitate old structure
    create_old_client_file(
        clients_path, {
            'client_id': 'TEST',
            'client_secret_hash': 'secret',
        }, 'TEST'
    )
    items = os.listdir(clients_path)
    assert items == ['TEST.yml']

    result = cli.invoke(rc, [
        'upgrade',
        '-r', UPGRADE_CLIENTS_SCRIPT
    ])
    assert result.exit_code == 0
    assert script_check_status_message(UPGRADE_CLIENTS_SCRIPT, UPGRADE_CHECK_STATUS_REQUIRED) in result.stdout
    assert client_migration_status_message('TEST.yml', CLIENT_STATUS_FAILED_MISSING_SCOPES) in result.stdout
    assert "Created keymap with 0 users" in result.stdout

    # Check is file still exists and if new folders got created
    items = os.listdir(clients_path)
    assert Counter(items) == Counter(['TEST.yml', 'helpers', 'id'])

    # Ensure keymap.yml exists and it only contains `TEST` client.
    keymap_path = get_keymap_path(clients_path)
    assert keymap_path.exists()
    keymap = get_yaml_data(keymap_path)

    assert list(keymap.keys()) == []

    id_path = get_id_path(clients_path)
    items = os.listdir(id_path)
    assert items == []


def test_upgrade_clients_force_upgrade(context,
    rc,
    cli: SpintaCliRunner,
    tmp_path: pathlib.Path
):
    clients_path = get_clients_path(tmp_path)

    # Create emtpy folders and keymap.yml (imitate running empty project with _ensure_config_dir)
    ensure_client_folders_exist(clients_path)

    rc = rc.fork({
        'config_path': str(tmp_path),
        'default_auth_client': None
    })

    # Create and migrate files
    create_old_client_file(
        clients_path, {
            'client_id': 'TEST',
            'client_secret_hash': 'secret',
            'scopes': [
                'spinta_getall'
            ]
        }
    )
    result = cli.invoke(rc, [
        'upgrade',
        '-r', UPGRADE_CLIENTS_SCRIPT
    ])
    assert script_check_status_message(UPGRADE_CLIENTS_SCRIPT, UPGRADE_CHECK_STATUS_REQUIRED) in result.stdout

    # Add new client
    create_old_client_file(
        clients_path, {
            'client_id': 'NEW',
            'client_secret_hash': 'secret',
            'scopes': [
                'spinta_getall'
            ]
        }
    )
    result = cli.invoke(rc, [
        'upgrade',
        '-r', UPGRADE_CLIENTS_SCRIPT
    ])
    assert script_check_status_message(UPGRADE_CLIENTS_SCRIPT, UPGRADE_CHECK_STATUS_PASSED) in result.stdout

    keymap_path = get_keymap_path(clients_path)
    assert keymap_path.exists()

    keymap = get_yaml_data(keymap_path)
    assert list(keymap.keys()) == ['TEST']

    # Force check
    result = cli.invoke(rc, [
        'upgrade',
        '-r', UPGRADE_CLIENTS_SCRIPT,
        '-f'
    ])
    assert script_check_status_message(UPGRADE_CLIENTS_SCRIPT, UPGRADE_CHECK_STATUS_FORCED) in result.stdout
    assert client_migration_status_message('NEW.yml', CLIENT_STATUS_SUCCESS) in result.stdout
    assert client_migration_status_message('TEST.yml', CLIENT_STATUS_SKIPPED_MIGRATED) in result.stdout
    assert "Created keymap with 2 users" in result.stdout

    keymap_path = get_keymap_path(clients_path)
    assert keymap_path.exists()

    keymap = get_yaml_data(keymap_path)
    assert Counter(list(keymap.keys())) == Counter(['TEST', 'NEW'])

    new_id = keymap['NEW']

    assert is_str_uuid(new_id)

    client_file_path = get_client_file_path(clients_path, new_id)
    assert client_file_path.exists()

    client_data = get_yaml_data(client_file_path)
    assert client_data == {
        'client_id': new_id,
        'client_name': 'NEW',
        'client_secret_hash': 'secret',
        'scopes': [
            'spinta_getall'
        ]
    }


def test_upgrade_clients_force_upgrade_destructive(context,
    rc,
    cli: SpintaCliRunner,
    tmp_path: pathlib.Path
):
    clients_path = get_clients_path(tmp_path)

    # Create emtpy folders and keymap.yml (imitate running empty project with _ensure_config_dir)
    ensure_client_folders_exist(clients_path)

    rc = rc.fork({
        'config_path': str(tmp_path),
        'default_auth_client': None
    })

    # Create and migrate files
    create_old_client_file(
        clients_path, {
            'client_id': 'TEST',
            'client_secret_hash': 'secret',
            'scopes': [
                'spinta_getall'
            ]
        }
    )
    result = cli.invoke(rc, [
        'upgrade',
        '-r', UPGRADE_CLIENTS_SCRIPT
    ])
    assert script_check_status_message(UPGRADE_CLIENTS_SCRIPT, UPGRADE_CHECK_STATUS_REQUIRED) in result.stdout

    keymap_path = get_keymap_path(clients_path)
    assert keymap_path.exists()

    keymap = get_yaml_data(keymap_path)
    assert Counter(list(keymap.keys())) == Counter(['TEST'])

    test_id = keymap['TEST']
    assert is_str_uuid(test_id)

    # Update client scopes
    create_old_client_file(
        clients_path, {
            'client_id': 'TEST',
            'client_secret_hash': 'secret',
            'scopes': [
                'spinta_getall',
                'spinta_update'
            ]
        }
    )
    result = cli.invoke(rc, [
        'upgrade',
        '-r', UPGRADE_CLIENTS_SCRIPT
    ])
    assert script_check_status_message(UPGRADE_CLIENTS_SCRIPT, UPGRADE_CHECK_STATUS_PASSED) in result.stdout

    result = cli.invoke(rc, [
        'upgrade',
        '-r', UPGRADE_CLIENTS_SCRIPT,
        '-f'
    ])
    assert script_check_status_message(UPGRADE_CLIENTS_SCRIPT, UPGRADE_CHECK_STATUS_FORCED) in result.stdout
    assert client_migration_status_message('TEST.yml', CLIENT_STATUS_SKIPPED_MIGRATED) in result.stdout

    # Force check
    result = cli.invoke(rc, [
        'upgrade',
        '-r', UPGRADE_CLIENTS_SCRIPT,
        '-f', '-d'
    ])
    assert script_check_status_message(UPGRADE_CLIENTS_SCRIPT, UPGRADE_CHECK_STATUS_FORCED) in result.stdout
    assert client_migration_status_message('TEST.yml', CLIENT_STATUS_SUCCESS) in result.stdout
    assert "DESTRUCTIVE MODE" in result.stdout

    keymap = get_yaml_data(keymap_path)
    assert Counter(list(keymap.keys())) == Counter(['TEST'])

    new_test_id = keymap['TEST']
    assert is_str_uuid(new_test_id)
    assert test_id == new_test_id

    client_file_path = get_client_file_path(clients_path, new_test_id)
    assert client_file_path.exists()

    client_data = get_yaml_data(client_file_path)
    assert client_data == {
        'client_id': test_id,
        'client_name': 'TEST',
        'client_secret_hash': 'secret',
        'scopes': [
            'spinta_getall',
            'spinta_update'
        ]
    }


def test_upgrade_new_columns(
    context,
    rc,
    cli: SpintaCliRunner,
    tmp_path: pathlib.Path,
):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type    | ref     | source      | source.type
    datasets/gov/example     |         |         |             |
      | data                 | sql     |         | sqlite://   | sqlite
                             |         |         |             |
      |   |   | Country      |         | code    | salis       | table
      |   |   |   | code     | integer |         | kodas       | test
      |   |   |   | name     | string  |         | pavadinimas | varchar(255)
      |   |   |   | driving  | string  |         | vairavimas  | varchar(1)
      |   |   |   |          | enum    |         | l           |
      |   |   |   |          |         |         | r           |  
                             |         |         |             |
      |   |   | City         |         | name    | miestas     | view materialized
      |   |   |   | name     | string  |         | pavadinimas | varchar(255)
      |   |   |   | country  | ref     | Country | salis       | integer
    '''))

    cli.invoke(rc, [
        'upgrade',
        '--run',
        'new_columns',
        # '-o', tmp_path / 'result.csv',
        tmp_path / 'manifest.csv',
    ])

    manifest = load_manifest(rc, tmp_path / 'manifest.csv')
    assert manifest == '''
    d | r | b | m            | property | type    | ref         | source            | source.type  | prepare | level | access | uri | title | description | status | visibility | eli | count | origin
    datasets/gov/example     |          |         |             |                   |              |         |       |        |     |       |             |        |            |     |       | 
      | data                 | sql      |         | sqlite://   | sqlite            |              |         |       |        |     |       |             |        |            |     |       |
                             |          |         |             |                   |              |         |       |        |     |       |             |        |            |     |       |
      |   |   | Country      |          | code    | salis       | table             |              |         |       |        |     |       |             |        |            |     |       |
      |   |   |   | code     | integer  |         | kodas       | test              |              |         |       |        |     |       |             |        |            |     |       |
      |   |   |   | name     | string   |         | pavadinimas | varchar(255)      |              |         |       |        |     |       |             |        |            |     |       |
      |   |   |   | driving  | string   |         | vairavimas  | varchar(1)        |              |         |       |        |     |       |             |        |            |     |       |
                             | enum     |         | l           |                   |              |         |       |        |     |       |             |        |            |     |       |
                             |          |         | r           |                   |              |         |       |        |     |       |             |        |            |     |       |
                             |          |         |             |                   |              |         |       |        |     |       |             |        |            |     |       |
      |   |   | City         |          | name    | miestas     | view materialized |              |         |       |        |     |       |             |        |            |     |       |
      |   |   |   | name     | string   |         | pavadinimas | varchar(255)      |              |         |       |        |     |       |             |        |            |     |       |
      |   |   |   | country  | ref      | Country | salis       | integer           |              |         |       |        |     |       |             |        |            |     |       |
    '''
