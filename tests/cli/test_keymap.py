import re
import uuid

import pytest
import sqlalchemy as sa

from spinta.components import Context
from spinta.core.enums import Action
from spinta.testing.data import send
from spinta.testing.datasets import create_sqlite_db
from spinta.testing.tabular import create_tabular_manifest
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.config import RawConfig
from spinta.testing.client import create_rc, configure_remote_server, RemoteServer


@pytest.fixture(scope='function')
def geodb():
    with create_sqlite_db({
        'country': [
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('code', sa.Text),
            sa.Column('name', sa.Text),
        ],
        'cities': [
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('name', sa.Text),
            sa.Column('country', sa.Integer),
        ],
    }) as db:
        db.write('country', [
            {'code': 'lt', 'name': 'Lietuva', 'id': 1},
            {'code': 'lv', 'name': 'Latvija', 'id': 2},
            {'code': 'ee', 'name': 'Estija', 'id': 3},
        ])
        db.write('cities', [
            {'name': 'Vilnius', 'country': 1},
        ])
        yield db


def check_keymap_state(context: Context, table_name: str):
    keymap = context.get('store').keymaps['default']
    with keymap.engine.connect() as conn:
        table = keymap.get_table(table_name)
        query = sa.select([table])
        values = conn.execute(query).fetchall()
        return values


@pytest.fixture(scope='function')
def reset_keymap(context):
    def _reset_keymap():
        keymap = context.get('store').keymaps['default']
        with keymap.engine.connect() as conn:
            for table in keymap.metadata.tables.values():
                conn.execute(table.delete())
    _reset_keymap()
    yield
    _reset_keymap()


def test_keymap_sync_dry_run(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request
):
    table = '''
                d | r | b | m | property | type    | ref                             | source         | level | access
                syncdataset             |         |                                 |                |       |
                  | db                   | sql     |                                 |                |       |
                  |   |   | City         |         | id                              | cities         | 4     |
                  |   |   |   | id       | integer |                                 | id             | 4     | open
                  |   |   |   | name     | string  |                                 | name           | 2     | open
                  |   |   |   | country  | ref     | /syncdataset/countries/Country | country        | 4     | open
                  |   |   |   |          |         |                                 |                |       |
                syncdataset/countries   |         |                                 |                |       |
                  |   |   | Country      |         | code                            |                | 4     |
                  |   |   |   | code     | integer |                                 |                | 4     | open
                  |   |   |   | name     | string  |                                 |                | 2     | open
                '''
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable(table))
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    assert remote.url == 'https://example.com/'
    remote.app.authmodel('syncdataset/countries/Country', ['insert', 'wipe'])
    resp = remote.app.post('https://example.com/syncdataset/countries/Country', json={
        'code': 2,
    })

    manifest = tmp_path / 'manifest.csv'

    # Check keymap state before sync for Country
    keymap_before_sync = check_keymap_state(context, 'syncdataset/countries/Country')
    assert len(keymap_before_sync) == 0

    result = cli.invoke(localrc, [
        'keymap', 'sync', manifest,
        '-i', remote.url,
        '--credentials', remote.credsfile,
        '--dry-run',
        '--no-progress-bar',
    ])
    assert result.exit_code == 0
    responses.remove('POST', re.compile(r'https://example\.com/.*'))

    # Check keymap state after sync for Country
    keymap_after_sync = check_keymap_state(context, 'syncdataset/countries/Country')
    assert len(keymap_after_sync) == 0


def test_keymap_sync(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request,
    reset_keymap
):
    table = '''
            d | r | b | m | property | type    | ref                             | source         | level | access
            syncdataset             |         |                                 |                |       |
              | db                   | sql     |                                 |                |       |
              |   |   | City         |         | id                              | cities         | 4     |
              |   |   |   | id       | integer |                                 | id             | 4     | open
              |   |   |   | name     | string  |                                 | name           | 2     | open
              |   |   |   | country  | ref     | /syncdataset/countries/Country | country        | 4     | open
              |   |   |   |          |         |                                 |                |       |
            syncdataset/countries   |         |                                 |                |       |
              |   |   | Country      |         | code                            |                | 4     |
              |   |   |   | code     | integer |                                 |                | 4     | open
              |   |   |   | name     | string  |                                 |                | 2     | open
            '''
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable(table))
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    assert remote.url == 'https://example.com/'
    remote.app.authmodel('syncdataset/countries/Country', ['insert', 'wipe'])
    resp = remote.app.post('https://example.com/syncdataset/countries/Country', json={
        'code': 2,
    })
    country_id = resp.json()['_id']

    manifest = tmp_path / 'manifest.csv'

    # Check keymap state before sync for Country
    keymap_before_sync = check_keymap_state(context, 'syncdataset/countries/Country')
    assert len(keymap_before_sync) == 0

    result = cli.invoke(localrc, [
        'keymap', 'sync', manifest,
        '-i', remote.url,
        '--credentials', remote.credsfile,
        '--no-progress-bar',
    ])
    assert result.exit_code == 0

    # Check keymap state after sync for Country
    keymap_after_sync = check_keymap_state(context, 'syncdataset/countries/Country')
    assert len(keymap_after_sync) == 1
    assert keymap_after_sync[0][0] == country_id


def test_keymap_sync_more_entries(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request,
    reset_keymap
):
    table = '''
            d | r | b | m | property | type    | ref                             | source         | level | access
            largedataset             |         |                                 |                |       |
              | db                   | sql     |                                 |                |       |
              |   |   | City         |         | id                              | cities         | 4     |
              |   |   |   | id       | integer |                                 | id             | 4     | open
              |   |   |   | name     | string  |                                 | name           | 2     | open
              |   |   |   | country  | ref     | /largedataset/countries/Country | country        | 4     | open
              |   |   |   |          |         |                                 |                |       |
            largedataset/countries   |         |                                 |                |       |
              |   |   | Country      |         | code                            |                | 4     |
              |   |   |   | code     | integer |                                 |                | 4     | open
              |   |   |   | name     | string  |                                 |                | 2     | open
            '''
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable(table))
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    assert remote.url == 'https://example.com/'
    remote.app.authmodel('largedataset/countries/Country', ['insert', 'wipe'])

    entry_ids = [
        remote.app.post('https://example.com/largedataset/countries/Country', json={'code': i}).json()['_id']
        for i in range(10)]

    keymap_before_sync = check_keymap_state(context, 'largedataset/countries/Country')
    assert len(keymap_before_sync) == 0

    manifest = tmp_path / 'manifest.csv'

    result = cli.invoke(localrc, [
        'keymap', 'sync', manifest,
        '-i', remote.url,
        '--credentials', remote.credsfile
    ])
    assert result.exit_code == 0

    keymap_after_sync = check_keymap_state(context, 'largedataset/countries/Country')
    assert len(keymap_after_sync) == 10
    keymap_keys = [entry[0] for entry in keymap_after_sync]
    assert all(key in entry_ids for key in keymap_keys)


def test_keymap_sync_dataset(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request,
    reset_keymap
):
    table = '''
            d | r | b | m | property | type    | ref                             | source         | level | access
            syncdataset             |         |                                 |                |       |
              | db                   | sql     |                                 |                |       |
              |   |   | City         |         | id                              | cities         | 4     |
              |   |   |   | id       | integer |                                 | id             | 4     | open
              |   |   |   | name     | string  |                                 | name           | 2     | open
              |   |   |   | country  | ref     | /syncdataset/countries/Country | country        | 4     | open
              |   |   |   |          |         |                                 |                |       |
            syncdataset/countries   |         |                                 |                |       |
              |   |   | Country      |         | code                            |                | 4     |
              |   |   |   | code     | integer |                                 |                | 4     | open
              |   |   |   | name     | string  |                                 |                | 2     | open
            '''
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable(table))
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    assert remote.url == 'https://example.com/'
    remote.app.authmodel('syncdataset/countries/Country', ['insert', 'wipe'])
    resp = remote.app.post('https://example.com/syncdataset/countries/Country', json={
        'code': 2,
    })
    country_id = resp.json()['_id']

    manifest = tmp_path / 'manifest.csv'

    # Check keymap state before sync for Country
    keymap_before_sync = check_keymap_state(context, 'syncdataset/countries/Country')
    assert len(keymap_before_sync) == 0

    result = cli.invoke(localrc, [
        'keymap', 'sync', manifest,
        '-i', remote.url,
        '--credentials', remote.credsfile,
        '-d', 'syncdataset',
        '--no-progress-bar',
    ])
    assert result.exit_code == 0

    # Check keymap state before sync for Country
    keymap_after_sync = check_keymap_state(context, 'syncdataset/countries/Country')
    assert len(keymap_after_sync) == 1
    assert keymap_after_sync[0][0] == country_id


def test_keymap_sync_no_changes(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request,
    reset_keymap
):
    table = '''
            d | r | b | m | property | type    | ref                             | source         | level | access
            syncdataset             |         |                                 |                |       |
              | db                   | sql     |                                 |                |       |
              |   |   | City         |         | id                              | cities         | 4     |
              |   |   |   | id       | integer |                                 | id             | 4     | open
              |   |   |   | name     | string  |                                 | name           | 2     | open
              |   |   |   | country  | ref     | /syncdataset/countries/Country | country        | 4     | open
              |   |   |   |          |         |                                 |                |       |
            syncdataset/countries   |         |                                 |                |       |
              |   |   | Country      |         | code                            |                | 4     |
              |   |   |   | code     | integer |                                 |                | 4     | open
              |   |   |   | name     | string  |                                 |                | 2     | open
            '''
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable(table))
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    assert remote.url == 'https://example.com/'
    remote.app.authmodel('syncdataset/countries/Country', ['insert', 'wipe'])
    resp = remote.app.post('https://example.com/syncdataset/countries/Country', json={
        'code': 2,
    })
    country_id = resp.json()['_id']

    # Check keymap state before sync for Country
    keymap_before_sync = check_keymap_state(context, 'syncdataset/countries/Country')
    assert len(keymap_before_sync) == 0

    manifest = tmp_path / 'manifest.csv'

    result = cli.invoke(localrc, [
        'keymap', 'sync', manifest,
        '-i', remote.url,
        '--credentials', remote.credsfile,
        '--no-progress-bar',
    ])
    assert result.exit_code == 0

    # Run sync again with no changes
    result = cli.invoke(localrc, [
        'keymap', 'sync', manifest,
        '-i', remote.url,
        '--credentials', remote.credsfile,
        '--no-progress-bar',
    ])
    assert result.exit_code == 0

    keymap_after_sync = check_keymap_state(context, 'syncdataset/countries/Country')
    assert len(keymap_after_sync) == 1
    assert keymap_after_sync[0][0] == country_id


def test_keymap_sync_consequitive_changes(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request,
    reset_keymap
):
    table = '''
            d | r | b | m | property | type    | ref                             | source         | level | access
            syncdataset             |         |                                 |                |       |
              | db                   | sql     |                                 |                |       |
              |   |   | City         |         | id                              | cities         | 4     |
              |   |   |   | id       | integer |                                 | id             | 4     | open
              |   |   |   | name     | string  |                                 | name           | 2     | open
              |   |   |   | country  | ref     | /syncdataset/countries/Country | country        | 4     | open
              |   |   |   |          |         |                                 |                |       |
            syncdataset/countries   |         |                                 |                |       |
              |   |   | Country      |         | code                            |                | 4     |
              |   |   |   | code     | integer |                                 |                | 4     | open
              |   |   |   | name     | string  |                                 |                | 2     | open
            '''
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable(table))
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    assert remote.url == 'https://example.com/'
    remote.app.authmodel('syncdataset/countries/Country', ['insert', 'wipe'])
    resp = remote.app.post('https://example.com/syncdataset/countries/Country', json={
        'code': 2,
    })
    country_id_1 = resp.json()['_id']

    # Check keymap state before sync for Country
    keymap_before_sync = check_keymap_state(context, 'syncdataset/countries/Country')
    assert len(keymap_before_sync) == 0

    manifest = tmp_path / 'manifest.csv'

    result = cli.invoke(localrc, [
        'keymap', 'sync', manifest,
        '-i', remote.url,
        '--credentials', remote.credsfile,
        '--no-progress-bar',
    ])
    assert result.exit_code == 0
    keymap_after_sync = check_keymap_state(context, 'syncdataset/countries/Country')
    assert len(keymap_after_sync) == 1
    assert keymap_after_sync[0][0] == country_id_1

    remote.app.authmodel('syncdataset/countries/Country', ['insert', 'wipe'])
    resp = remote.app.post('https://example.com/syncdataset/countries/Country', json={
        'code': 3,
    })
    country_id_2 = resp.json()['_id']
    result = cli.invoke(localrc, [
        'keymap', 'sync', manifest,
        '-i', remote.url,
        '--credentials', remote.credsfile,
        '--no-progress-bar',
    ])
    assert result.exit_code == 0

    keymap_after_sync = check_keymap_state(context, 'syncdataset/countries/Country')
    assert len(keymap_after_sync) == 2
    assert keymap_after_sync[0][0] == country_id_1
    assert keymap_after_sync[1][0] == country_id_2


def test_keymap_sync_missing_input(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb):
    table = '''
        d | r | b | m | property | type    | ref                             | source         | level | access
        syncdataset             |         |                                 |                |       |
          | db                   | sql     |                                 |                |       |
          |   |   | City         |         | id                              | cities         | 4     |
          |   |   |   | id       | integer |                                 | id             | 4     | open
          |   |   |   | name     | string  |                                 | name           | 2     | open
          |   |   |   | country  | ref     | /syncdataset/countries/Country | country        | 4     | open
          |   |   |   |          |         |                                 |                |       |
        syncdataset/countries   |         |                                 |                |       |
          |   |   | Country      |         | code                            |                | 4     |
          |   |   |   | code     | integer |                                 |                | 4     | open
          |   |   |   | name     | string  |                                 |                | 2     | open
        '''
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable(table))
    localrc = create_rc(rc, tmp_path, geodb)
    result = cli.invoke(localrc, ['keymap', 'sync', str(tmp_path / 'manifest.csv')], fail=False)
    assert result.exit_code == 1
    assert "Input source is required." in result.stderr


def test_keymap_sync_invalid_credentials(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request
):
    table = '''
            d | r | b | m | property | type    | ref                             | source         | level | access
            syncdataset             |         |                                 |                |       |
              | db                   | sql     |                                 |                |       |
              |   |   | City         |         | id                              | cities         | 4     |
              |   |   |   | id       | integer |                                 | id             | 4     | open
              |   |   |   | name     | string  |                                 | name           | 2     | open
              |   |   |   | country  | ref     | /syncdataset/countries/Country | country        | 4     | open
              |   |   |   |          |         |                                 |                |       |
            syncdataset/countries   |         |                                 |                |       |
              |   |   | Country      |         | code                            |                | 4     |
              |   |   |   | code     | integer |                                 |                | 4     | open
              |   |   |   | name     | string  |                                 |                | 2     | open
            '''
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable(table))
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    manifest = tmp_path / 'manifest.csv'

    responses.remove('POST', re.compile(r'https://example\.com/.*'))

    result = cli.invoke(localrc, [
        'keymap', 'sync', manifest,
        '-i', remote.url,
        '--credentials', "invalid_credentials",
        '--no-progress-bar',
    ], fail=False)
    assert result.exit_code == 1


def test_keymap_sync_no_credentials(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request
):
    table = '''
            d | r | b | m | property | type    | ref                             | source         | level | access
            syncdataset             |         |                                 |                |       |
              | db                   | sql     |                                 |                |       |
              |   |   | City         |         | id                              | cities         | 4     |
              |   |   |   | id       | integer |                                 | id             | 4     | open
              |   |   |   | name     | string  |                                 | name           | 2     | open
              |   |   |   | country  | ref     | /syncdataset/countries/Country | country        | 4     | open
              |   |   |   |          |         |                                 |                |       |
            syncdataset/countries   |         |                                 |                |       |
              |   |   | Country      |         | code                            |                | 4     |
              |   |   |   | code     | integer |                                 |                | 4     | open
              |   |   |   | name     | string  |                                 |                | 2     | open
            '''
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable(table))
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    manifest = tmp_path / 'manifest.csv'

    responses.remove('POST', re.compile(r'https://example\.com/.*'))

    # Credentials not present in credentials.cfg (Remote client credentials not found for 'https://example.com/')
    result = cli.invoke(localrc, [
        'keymap', 'sync', manifest,
        '-i', remote.url,
        '--no-progress-bar',
    ], fail=False)
    assert result.exit_code == 1


def test_keymap_sync_non_existent_dataset(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request
):
    table = '''
            d | r | b | m | property | type    | ref                             | source         | level | access
            syncdataset             |         |                                 |                |       |
              | db                   | sql     |                                 |                |       |
              |   |   | City         |         | id                              | cities         | 4     |
              |   |   |   | id       | integer |                                 | id             | 4     | open
              |   |   |   | name     | string  |                                 | name           | 2     | open
              |   |   |   | country  | ref     | /syncdataset/countries/Country | country        | 4     | open
              |   |   |   |          |         |                                 |                |       |
            syncdataset/countries   |         |                                 |                |       |
              |   |   | Country      |         | code                            |                | 4     |
              |   |   |   | code     | integer |                                 |                | 4     | open
              |   |   |   | name     | string  |                                 |                | 2     | open
            '''
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable(table))
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    manifest = tmp_path / 'manifest.csv'

    responses.remove('POST', re.compile(r'https://example\.com/.*'))

    result = cli.invoke(localrc, [
        'keymap', 'sync', manifest,
        '-i', remote.url,
        '--credentials', remote.credsfile,
        '--dataset', 'non_existent_dataset',
        '--no-progress-bar',
    ], fail=False)

    assert result.exit_code == 1
    assert "'dataset' not found" in result.stderr


def test_keymap_sync_with_pages(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request,
    reset_keymap
):
    table = '''
            d | r | b | m | property | type    | ref                             | source         | level | access
            largedataset             |         |                                 |                |       |
              | db                   | sql     |                                 |                |       |
              |   |   | City         |         | id                              | cities         | 4     |
              |   |   |   | id       | integer |                                 | id             | 4     | open
              |   |   |   | name     | string  |                                 | name           | 2     | open
              |   |   |   | country  | ref     | /largedataset/countries/Country | country        | 4     | open
              |   |   |   |          |         |                                 |                |       |
            largedataset/countries   |         |                                 |                |       |
              |   |   | Country      |         | code                            |                | 4     |
              |   |   |   | code     | integer |                                 |                | 4     | open
              |   |   |   | name     | string  |                                 |                | 2     | open
            '''
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable(table))
    localrc = create_rc(rc, tmp_path, geodb)
    localrc = localrc.fork({
        "sync_page_size": 3
    })
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    assert remote.url == 'https://example.com/'
    remote.app.authmodel('largedataset/countries/Country', ['insert', 'wipe'])

    entry_ids = [
        remote.app.post('https://example.com/largedataset/countries/Country', json={'code': i}).json()['_id']
        for i in range(10)]

    keymap_before_sync = check_keymap_state(context, 'largedataset/countries/Country')
    assert len(keymap_before_sync) == 0

    manifest = tmp_path / 'manifest.csv'

    result = cli.invoke(localrc, [
        'keymap', 'sync', manifest,
        '-i', remote.url,
        '--credentials', remote.credsfile
    ])
    assert result.exit_code == 0

    keymap_after_sync = check_keymap_state(context, 'largedataset/countries/Country')
    assert len(keymap_after_sync) == 10
    keymap_keys = [entry[0] for entry in keymap_after_sync]
    assert all(key in entry_ids for key in keymap_keys)


def test_keymap_sync_with_transaction_batches(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request,
    reset_keymap
):
    table = '''
            d | r | b | m | property | type    | ref                             | source         | level | access
            largedataset             |         |                                 |                |       |
              | db                   | sql     |                                 |                |       |
              |   |   | City         |         | id                              | cities         | 4     |
              |   |   |   | id       | integer |                                 | id             | 4     | open
              |   |   |   | name     | string  |                                 | name           | 2     | open
              |   |   |   | country  | ref     | /largedataset/countries/Country | country        | 4     | open
              |   |   |   |          |         |                                 |                |       |
            largedataset/countries   |         |                                 |                |       |
              |   |   | Country      |         | code                            |                | 4     |
              |   |   |   | code     | integer |                                 |                | 4     | open
              |   |   |   | name     | string  |                                 |                | 2     | open
            '''
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable(table))
    localrc = create_rc(rc, tmp_path, geodb)
    localrc = localrc.fork({
        "sync_page_size": 3,
        "keymaps": {
            "default": {
                "type": "sqlalchemy",
                "sync_transaction_size": 4
            }
        }
    })
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    assert remote.url == 'https://example.com/'
    remote.app.authmodel('largedataset/countries/Country', ['insert', 'wipe'])

    entry_ids = [
        remote.app.post('https://example.com/largedataset/countries/Country', json={'code': i}).json()['_id']
        for i in range(10)]

    keymap_before_sync = check_keymap_state(context, 'largedataset/countries/Country')
    assert len(keymap_before_sync) == 0

    manifest = tmp_path / 'manifest.csv'

    result = cli.invoke(localrc, [
        'keymap', 'sync', manifest,
        '-i', remote.url,
        '--credentials', remote.credsfile
    ])
    assert result.exit_code == 0

    keymap_after_sync = check_keymap_state(context, 'largedataset/countries/Country')
    assert len(keymap_after_sync) == 10
    keymap_keys = [entry[0] for entry in keymap_after_sync]
    assert all(key in entry_ids for key in keymap_keys)


def test_keymap_sync_insert(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request,
    reset_keymap
):
    table = '''
        d | r | b | m | property | type    | ref                             | source         | level | access
        syncdataset              |         |                                 |                |       |
          | db                   | sql     |                                 |                |       |
          |   |   | City         |         | id                              | cities         | 4     |
          |   |   |   | id       | integer |                                 | id             | 4     | open
          |   |   |   | name     | string  |                                 | name           | 2     | open
          |   |   |   | country  | ref     | /syncdataset/countries/Country  | country        | 4     | open
          |   |   |   |          |         |                                 |                |       |
        syncdataset/countries    |         |                                 |                |       |
          |   |   | Country      |         | code                            |                | 4     |
          |   |   |   | code     | integer |                                 |                | 4     | open
          |   |   |   | name     | string  |                                 |                | 2     | open
    '''
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable(table))
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    store = remote.app.context.get('store')
    manifest = store.manifest
    keymap = manifest.keymap
    request.addfinalizer(remote.app.context.wipe_all)

    model = 'syncdataset/countries/Country'
    remote.app.authmodel(model, ['insert', 'wipe', 'changes'])
    obj = remote.app.post(model, json={'code': 1})
    country_id_1 = obj.json()['_id']
    assert send(remote.app, model, ':changes/-1?limit(1)', select=['_cid', '_op', '_id', 'code']) == [
        {'_cid': 1, '_op': 'insert', '_id': country_id_1, 'code': 1},
    ]

    # Check keymap state before sync for Country
    keymap_before_sync = check_keymap_state(context, model)
    assert len(keymap_before_sync) == 0

    manifest = tmp_path / 'manifest.csv'

    result = cli.invoke(localrc, [
        'keymap', 'sync', manifest,
        '-i', remote.url,
        '--credentials', remote.credsfile,
        '--no-progress-bar',
    ])
    assert result.exit_code == 0
    keymap_after_sync = check_keymap_state(context, model)
    assert len(keymap_after_sync) == 1
    assert keymap_after_sync[0][0] == country_id_1
    with keymap as km:
        assert km.decode(model, country_id_1) == 1


def test_keymap_sync_update(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request,
    reset_keymap
):
    table = '''
        d | r | b | m | property | type    | ref                             | source         | level | access
        syncdataset              |         |                                 |                |       |
          | db                   | sql     |                                 |                |       |
          |   |   | City         |         | id                              | cities         | 4     |
          |   |   |   | id       | integer |                                 | id             | 4     | open
          |   |   |   | name     | string  |                                 | name           | 2     | open
          |   |   |   | country  | ref     | /syncdataset/countries/Country  | country        | 4     | open
          |   |   |   |          |         |                                 |                |       |
        syncdataset/countries    |         |                                 |                |       |
          |   |   | Country      |         | code                            |                | 4     |
          |   |   |   | code     | integer |                                 |                | 4     | open
          |   |   |   | name     | string  |                                 |                | 2     | open
    '''
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable(table))
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    store = remote.app.context.get('store')
    manifest = store.manifest
    keymap = manifest.keymap
    request.addfinalizer(remote.app.context.wipe_all)

    model = 'syncdataset/countries/Country'
    remote.app.authmodel(model, ['insert', 'wipe', 'changes', 'update'])
    obj = remote.app.post(model, json={'code': 1, 'name': 'a'}).json()
    country_id_1 = obj['_id']

    assert send(remote.app, model, ':changes', select=['_cid', '_op', '_id', 'code']) == [
        {'_cid': 1, '_op': 'insert', '_id': country_id_1, 'code': 1},
    ]

    # Check keymap state before sync for Country
    keymap_before_sync = check_keymap_state(context, model)
    assert len(keymap_before_sync) == 0

    manifest = tmp_path / 'manifest.csv'

    result = cli.invoke(localrc, [
        'keymap', 'sync', manifest,
        '-i', remote.url,
        '--credentials', remote.credsfile,
        '--no-progress-bar',
    ])
    assert result.exit_code == 0
    keymap_after_sync = check_keymap_state(context, model)
    assert len(keymap_after_sync) == 1
    assert keymap_after_sync[0][0] == country_id_1
    with keymap as km:
        assert km.decode(model, country_id_1) == 1

    hash_, value = keymap_after_sync[0][1], keymap_after_sync[0][2]

    obj = remote.app.post(f'{model}/{country_id_1}', json={
        '_op': Action.UPDATE.value,
        '_revision': obj['_revision'],
        'code': 10
    }).json()
    country_id_2 = obj['_id']
    assert country_id_1 == country_id_2

    assert send(remote.app, model, ':changes', select=['_cid', '_op', '_id', 'code']) == [
        {'_cid': 1, '_op': 'insert', '_id': country_id_1, 'code': 1},
        {'_cid': 2, '_op': 'update', '_id': country_id_2, 'code': 10},
    ]

    result = cli.invoke(localrc, [
        'keymap', 'sync', manifest,
        '-i', remote.url,
        '--credentials', remote.credsfile,
        '--no-progress-bar',
    ])
    assert result.exit_code == 0
    keymap_after_sync = check_keymap_state(context, model)
    assert len(keymap_after_sync) == 1
    assert keymap_after_sync[0][0] == country_id_2
    assert keymap_after_sync[0][1] != hash_
    assert keymap_after_sync[0][2] != value
    with keymap as km:
        assert km.decode(model, country_id_2) == 10


def test_keymap_sync_patch(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request,
    reset_keymap
):
    table = '''
        d | r | b | m | property | type    | ref                             | source         | level | access
        syncdataset              |         |                                 |                |       |
          | db                   | sql     |                                 |                |       |
          |   |   | City         |         | id                              | cities         | 4     |
          |   |   |   | id       | integer |                                 | id             | 4     | open
          |   |   |   | name     | string  |                                 | name           | 2     | open
          |   |   |   | country  | ref     | /syncdataset/countries/Country  | country        | 4     | open
          |   |   |   |          |         |                                 |                |       |
        syncdataset/countries    |         |                                 |                |       |
          |   |   | Country      |         | code                            |                | 4     |
          |   |   |   | code     | integer |                                 |                | 4     | open
          |   |   |   | name     | string  |                                 |                | 2     | open
    '''
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable(table))
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    store = remote.app.context.get('store')
    manifest = store.manifest
    keymap = manifest.keymap
    request.addfinalizer(remote.app.context.wipe_all)

    model = 'syncdataset/countries/Country'
    remote.app.authmodel(model, ['insert', 'wipe', 'changes', 'patch'])
    obj = remote.app.post(model, json={'code': 1, 'name': 'a'}).json()
    country_id_1 = obj['_id']

    assert send(remote.app, model, ':changes', select=['_cid', '_op', '_id', 'code']) == [
        {'_cid': 1, '_op': 'insert', '_id': country_id_1, 'code': 1},
    ]

    # Check keymap state before sync for Country
    keymap_before_sync = check_keymap_state(context, model)
    assert len(keymap_before_sync) == 0

    manifest = tmp_path / 'manifest.csv'

    result = cli.invoke(localrc, [
        'keymap', 'sync', manifest,
        '-i', remote.url,
        '--credentials', remote.credsfile,
        '--no-progress-bar',
    ])
    assert result.exit_code == 0
    keymap_after_sync = check_keymap_state(context, model)
    assert len(keymap_after_sync) == 1
    assert keymap_after_sync[0][0] == country_id_1
    with keymap as km:
        assert km.decode(model, country_id_1) == 1

    hash_, value = keymap_after_sync[0][1], keymap_after_sync[0][2]

    obj = remote.app.post(f'{model}/{country_id_1}', json={
        '_op': Action.PATCH.value,
        '_revision': obj['_revision'],
        'code': 10
    }).json()
    country_id_2 = obj['_id']
    assert country_id_1 == country_id_2

    assert send(remote.app, model, ':changes', select=['_cid', '_op', '_id', 'code']) == [
        {'_cid': 1, '_op': 'insert', '_id': country_id_1, 'code': 1},
        {'_cid': 2, '_op': 'patch', '_id': country_id_2, 'code': 10},
    ]

    result = cli.invoke(localrc, [
        'keymap', 'sync', manifest,
        '-i', remote.url,
        '--credentials', remote.credsfile,
        '--no-progress-bar',
    ])
    assert result.exit_code == 0
    keymap_after_sync = check_keymap_state(context, model)
    assert len(keymap_after_sync) == 1
    assert keymap_after_sync[0][0] == country_id_2
    assert keymap_after_sync[0][1] != hash_
    assert keymap_after_sync[0][2] != value
    with keymap as km:
        assert km.decode(model, country_id_2) == 10


def test_keymap_sync_upsert_insert(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request,
    reset_keymap
):
    table = '''
        d | r | b | m | property | type    | ref                             | source         | level | access
        syncdataset              |         |                                 |                |       |
          | db                   | sql     |                                 |                |       |
          |   |   | City         |         | id                              | cities         | 4     |
          |   |   |   | id       | integer |                                 | id             | 4     | open
          |   |   |   | name     | string  |                                 | name           | 2     | open
          |   |   |   | country  | ref     | /syncdataset/countries/Country  | country        | 4     | open
          |   |   |   |          |         |                                 |                |       |
        syncdataset/countries    |         |                                 |                |       |
          |   |   | Country      |         | code                            |                | 4     |
          |   |   |   | code     | integer |                                 |                | 4     | open
          |   |   |   | name     | string  |                                 |                | 2     | open
    '''
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable(table))
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    store = remote.app.context.get('store')
    manifest = store.manifest
    keymap = manifest.keymap
    request.addfinalizer(remote.app.context.wipe_all)

    model = 'syncdataset/countries/Country'
    remote.app.authmodel(model, ['insert', 'wipe', 'changes', 'upsert'])
    obj = remote.app.post(model, json={'code': 1, 'name': 'a'}).json()
    country_id_1 = obj['_id']

    assert send(remote.app, model, ':changes', select=['_cid', '_op', '_id', 'code']) == [
        {'_cid': 1, '_op': 'insert', '_id': country_id_1, 'code': 1},
    ]

    # Check keymap state before sync for Country
    keymap_before_sync = check_keymap_state(context, model)
    assert len(keymap_before_sync) == 0

    manifest = tmp_path / 'manifest.csv'

    result = cli.invoke(localrc, [
        'keymap', 'sync', manifest,
        '-i', remote.url,
        '--credentials', remote.credsfile,
        '--no-progress-bar',
    ])
    assert result.exit_code == 0
    keymap_after_sync = check_keymap_state(context, model)
    assert len(keymap_after_sync) == 1
    assert keymap_after_sync[0][0] == country_id_1
    with keymap as km:
        assert km.decode(model, country_id_1) == 1

    hash_, value = keymap_after_sync[0][1], keymap_after_sync[0][2]

    country_id_2 = str(uuid.uuid4())
    obj = remote.app.post(f'{model}/{country_id_2}', json={
        '_op': Action.UPSERT.value,
        'code': 10
    }).json()
    country_id_2 = obj['_id']
    assert country_id_1 != country_id_2

    assert send(remote.app, model, ':changes', select=['_cid', '_op', '_id', 'code']) == [
        {'_cid': 1, '_op': 'insert', '_id': country_id_1, 'code': 1},
        {'_cid': 2, '_op': 'upsert', '_id': country_id_2, 'code': 10},
    ]

    result = cli.invoke(localrc, [
        'keymap', 'sync', manifest,
        '-i', remote.url,
        '--credentials', remote.credsfile,
        '--no-progress-bar',
    ])
    assert result.exit_code == 0
    keymap_after_sync = check_keymap_state(context, model)
    assert len(keymap_after_sync) == 2
    assert keymap_after_sync[0][0] == country_id_1
    assert keymap_after_sync[0][1] == hash_
    assert keymap_after_sync[0][2] == value
    with keymap as km:
        assert km.decode(model, country_id_1) == 1

    assert keymap_after_sync[1][0] == country_id_2
    assert keymap_after_sync[1][1] != hash_
    assert keymap_after_sync[1][2] != value
    with keymap as km:
        assert km.decode(model, country_id_2) == 10


def test_keymap_sync_upsert_update(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request,
    reset_keymap
):
    table = '''
        d | r | b | m | property | type    | ref                             | source         | level | access
        syncdataset              |         |                                 |                |       |
          | db                   | sql     |                                 |                |       |
          |   |   | City         |         | id                              | cities         | 4     |
          |   |   |   | id       | integer |                                 | id             | 4     | open
          |   |   |   | name     | string  |                                 | name           | 2     | open
          |   |   |   | country  | ref     | /syncdataset/countries/Country  | country        | 4     | open
          |   |   |   |          |         |                                 |                |       |
        syncdataset/countries    |         |                                 |                |       |
          |   |   | Country      |         | code                            |                | 4     |
          |   |   |   | code     | integer |                                 |                | 4     | open
          |   |   |   | name     | string  |                                 |                | 2     | open
    '''
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable(table))
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    store = remote.app.context.get('store')
    manifest = store.manifest
    keymap = manifest.keymap
    request.addfinalizer(remote.app.context.wipe_all)

    model = 'syncdataset/countries/Country'
    remote.app.authmodel(model, ['insert', 'wipe', 'changes', 'upsert'])
    obj = remote.app.post(model, json={'code': 1, 'name': 'a'}).json()
    country_id_1 = obj['_id']

    assert send(remote.app, model, ':changes', select=['_cid', '_op', '_id', 'code']) == [
        {'_cid': 1, '_op': 'insert', '_id': country_id_1, 'code': 1},
    ]

    # Check keymap state before sync for Country
    keymap_before_sync = check_keymap_state(context, model)
    assert len(keymap_before_sync) == 0

    manifest = tmp_path / 'manifest.csv'

    result = cli.invoke(localrc, [
        'keymap', 'sync', manifest,
        '-i', remote.url,
        '--credentials', remote.credsfile,
        '--no-progress-bar',
    ])
    assert result.exit_code == 0
    keymap_after_sync = check_keymap_state(context, model)
    assert len(keymap_after_sync) == 1
    assert keymap_after_sync[0][0] == country_id_1
    with keymap as km:
        assert km.decode(model, country_id_1) == 1

    hash_, value = keymap_after_sync[0][1], keymap_after_sync[0][2]

    obj = remote.app.post(f'{model}/{country_id_1}', json={
        '_op': Action.UPSERT.value,
        'code': 10
    }).json()
    country_id_2 = obj['_id']
    assert country_id_1 == country_id_2

    assert send(remote.app, model, ':changes', select=['_cid', '_op', '_id', 'code']) == [
        {'_cid': 1, '_op': 'insert', '_id': country_id_1, 'code': 1},
        {'_cid': 2, '_op': 'upsert', '_id': country_id_2, 'code': 10},
    ]

    result = cli.invoke(localrc, [
        'keymap', 'sync', manifest,
        '-i', remote.url,
        '--credentials', remote.credsfile,
        '--no-progress-bar',
    ])
    assert result.exit_code == 0
    keymap_after_sync = check_keymap_state(context, model)
    assert len(keymap_after_sync) == 1
    assert keymap_after_sync[0][0] == country_id_2
    assert keymap_after_sync[0][1] != hash_
    assert keymap_after_sync[0][2] != value
    with keymap as km:
        assert km.decode(model, country_id_2) == 10
