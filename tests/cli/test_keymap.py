import re
from urllib.request import Request

import pytest
import responses
import sqlalchemy as sa

from spinta.components import Context
from spinta.exceptions import UnauthorizedKeymapSync
from spinta.testing.data import listdata
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


def check_keymap_state(context: Context, table_name: str, request: Request, remote: RemoteServer = None, before: bool = True):
    keymap = context.get('store').keymaps['default']
    with keymap.engine.connect() as conn:
        table = keymap.get_table(table_name)
        query = sa.select([table])
        value = conn.execute(query).scalar()
        if not before:
            request.addfinalizer(lambda: conn.close())
        else:
            request.addfinalizer(lambda: remote.app.context.wipe_all())
        return value


def test_keymap_sync_basic(
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
    country_id = resp.json()['_id']

    manifest = tmp_path / 'manifest.csv'

    # Check keymap state before sync for Country
    check_keymap_state(context, 'syncdataset/countries/Country', remote = remote,request=request,)

    result = cli.invoke(localrc, [
        'keymap', 'sync', manifest,
        '-i', remote.url,
        '--credentials', remote.credsfile,
    ])
    assert result.exit_code == 0

    # Check keymap state before sync for Country
    keymap_after_sync = check_keymap_state(context, 'syncdataset/countries/Country', request=request, before = False)
    assert keymap_after_sync is not None
    assert keymap_after_sync == country_id


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
    result = cli.invoke(localrc,['keymap', 'sync', str(tmp_path / 'manifest.csv')], fail=False)
    assert result.exit_code == 1
    assert "Input source is required." in result.output


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
    ], fail=False)

    assert result.exit_code == 1
    assert "'dataset' not found" in result.output


@pytest.mark.skip
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

    manifest = tmp_path / 'manifest.csv'
    create_tabular_manifest(rc, manifest, striptable(''))
    result = cli.invoke(rc, [
        'keymap', 'sync', str(manifest),
        '-i', remote.url,
        '--dry-run',
    ])
    assert result.exit_code == 0
    assert "Dry run" in result.output


