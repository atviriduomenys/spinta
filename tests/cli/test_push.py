import pytest
import sqlalchemy as sa
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.data import listdata
from spinta.testing.datasets import create_sqlite_db
from spinta.testing.tabular import create_tabular_manifest
from tests.datasets.test_sql import configure_remote_server, create_rc


@pytest.fixture(scope='module')
def geodb():
    with create_sqlite_db({
        'salis': [
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('kodas', sa.Text),
            sa.Column('pavadinimas', sa.Text),
        ],
        'cities': [
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('name', sa.Text),
            sa.Column('country', sa.Integer),
        ]
    }) as db:
        db.write('salis', [
            {'kodas': 'lt', 'pavadinimas': 'Lietuva', 'id': 1},
            {'kodas': 'lv', 'pavadinimas': 'Latvija', 'id': 2},
            {'kodas': 'ee', 'pavadinimas': 'Estija', 'id': 3},
        ])
        db.write('cities', [
            {'name': 'Vilnius', 'country': 2},
        ])
        yield db


@pytest.fixture(scope='module')
def errordb():
    with create_sqlite_db({
        'salis': [
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('kodas', sa.Text),
            sa.Column('pavadinimas', sa.Text),
        ]
    }) as db:
        db.write('salis', [
            {'kodas': 'lt', 'pavadinimas': 'Lietuva'},
            {'kodas': 'lt', 'pavadinimas': 'Latvija'},
            {'kodas': 'lt', 'pavadinimas': 'Estija'},
        ])
        yield db


def test_push_with_progress_bar(
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request
):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property| type   | ref     | source       | access
    datasets/gov/example    |        |         |              |
      | data                | sql    |         |              |
      |   |                 |        |         |              |
      |   |   | Country     |        | code    | salis        |
      |   |   |   | code    | string |         | kodas        | open
      |   |   |   | name    | string |         | pavadinimas  | open
    '''))

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == 'https://example.com/'
    result = cli.invoke(localrc, [
        'push',
        '-d', 'datasets/gov/example',
        '-o', remote.url,
        '--credentials', remote.credsfile,
    ])
    assert result.exit_code == 0
    assert "Count rows:   0%" in result.stderr
    assert "PUSH:   0%|          | 0/3" in result.stderr
    assert "PUSH: 100%|##########| 3/3" in result.stderr


def test_push_without_progress_bar(
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request
):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property| type   | ref     | source       | access
    datasets/gov/example    |        |         |              |
      | data                | sql    |         |              |
      |   |                 |        |         |              |
      |   |   | Country     |        | code    | salis        |
      |   |   |   | code    | string |         | kodas        | open
      |   |   |   | name    | string |         | pavadinimas  | open
    '''))

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == 'https://example.com/'
    result = cli.invoke(localrc, [
        'push',
        '-d', 'datasets/gov/example',
        '-o', remote.url,
        '--credentials', remote.credsfile,
        '--no-progress-bar',
    ])
    assert result.exit_code == 0
    assert result.stderr == ""


def test_push_error_exit_code(
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    errordb,
    request
):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property| type    | ref     | source       | access
    datasets/gov/example    |         |         |              |
      | data                | sql     |         |              |
      |   |                 |         |         |              |
      |   |   | Country     |         | code    | salis        |
      |   |   |   | code    | string unique|         | kodas        | open
      |   |   |   | name    | string  |         | pavadinimas  | open
    '''))

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, errordb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == 'https://example.com/'
    result = cli.invoke(localrc, [
        'push',
        '-d', 'datasets/gov/example',
        '-o', remote.url,
        '--credentials', remote.credsfile,
    ], fail=False)
    assert result.exit_code == 1


def test_push_error_exit_code_with_bad_resource(
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    request
):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable(f'''
    d | r | b | m | property| type    | ref     | source       | access
    datasets/gov/example    |         |         |              |
      | data                | sql     |         | sqlite:///{tmp_path}/bad.db |
      |   |                 |         |         |              |
      |   |   | Country     |         | code    | salis        |
      |   |   |   | code    | string  |         | kodas        | open
      |   |   |   | name    | string  |         | pavadinimas  | open
    '''))

    localrc = rc.fork({
        'manifests': {
            'default': {
                'type': 'tabular',
                'path': str(tmp_path / 'manifest.csv'),
                'backend': 'sql',
                'keymap': 'default',
            },
        },
        'backends': {
            'sql': {
                'type': 'sql',
                'dsn': f"sqlite:///{tmp_path}/bad.db",
            },
        },
        # tests/config/clients/3388ea36-4a4f-4821-900a-b574c8829d52.yml
        'default_auth_client': '3388ea36-4a4f-4821-900a-b574c8829d52',
    })

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == 'https://example.com/'
    result = cli.invoke(localrc, [
        'push',
        '-d', 'datasets/gov/example',
        '-o', remote.url,
        '--credentials', remote.credsfile,
    ], fail=False)
    assert result.exit_code == 1


def test_push_ref_with_level_3(
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request
):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type     | ref      | source      | level | access
    level3dataset             |          |          |             |       |
      | db                   | sql      |          |             |       |
      |   |   | City         |          | id       | cities      | 4     |
      |   |   |   | id       | integer  |          | id          | 4     | open
      |   |   |   | name     | string   |          | name        | 4     | open
      |   |   |   | country  | ref      | Country  | country     | 3     | open
      |   |   |   |          |          |          |             |       |
      |   |   | Country      |          | id       | salis       | 4     |
      |   |   |   | code     | string   |          | kodas       | 4     | open
      |   |   |   | name     | string   |          | pavadinimas | 4     | open
      |   |   |   | id       | integer  |          | id          | 4     | open
    '''))

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == 'https://example.com/'
    result = cli.invoke(localrc, [
        'push',
        '-d', 'level3dataset',
        '-o', remote.url,
        '--credentials', remote.credsfile,
        '--no-progress-bar',
    ])
    assert result.exit_code == 0

    remote.app.authmodel('level3dataset/City', ['getall', 'search'])
    resp_city = remote.app.get('level3dataset/City')

    assert resp_city.status_code == 200
    assert listdata(resp_city, 'name') == ['Vilnius']
    assert len(listdata(resp_city, 'id', 'name', 'country')) == 1
    assert 'id' in listdata(resp_city, 'country')[0].keys()


def test_push_ref_with_level_4(
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request
):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type     | ref      | source      | level | access
    level4dataset             |          |          |             |       |
      | db                   | sql      |          |             |       |
      |   |   | City         |          | id       | cities      | 4     |
      |   |   |   | id       | integer  |          | id          | 4     | open
      |   |   |   | name     | string   |          | name        | 4     | open
      |   |   |   | country  | ref      | Country  | country     | 4     | open
      |   |   |   |          |          |          |             |       |
      |   |   | Country      |          | id       | salis       | 4     |
      |   |   |   | code     | string   |          | kodas       | 4     | open
      |   |   |   | name     | string   |          | pavadinimas | 4     | open
      |   |   |   | id       | integer  |          | id          | 4     | open
    '''))

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == 'https://example.com/'
    result = cli.invoke(localrc, [
        'push',
        '-d', 'level4dataset',
        '-o', remote.url,
        '--credentials', remote.credsfile,
        '--no-progress-bar',
    ])
    assert result.exit_code == 0

    remote.app.authmodel('level4dataset/City', ['getall', 'search'])
    resp_city = remote.app.get('level4dataset/City')

    assert resp_city.status_code == 200
    assert listdata(resp_city, 'name') == ['Vilnius']
    assert len(listdata(resp_city, 'id', 'name', 'country')) == 1
    assert '_id' in listdata(resp_city, 'country')[0].keys()


def test_push_pagination_incremental(
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request
):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type     | ref      | source      | level | access
    paginated             |          |          |             |       |
      | db                   | sql      |          |             |       |
      |   |   |   |          |          |          |             |       |
      |   |   | Country      |          | id       | salis       | 4     |
      |   |   |   | code     | string   |          | kodas       | 4     | open
      |   |   |   | name     | string   |          | pavadinimas | 4     | open
      |   |   |   | id       | integer  |          | id          | 4     | open
    '''))

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == 'https://example.com/'
    result = cli.invoke(localrc, [
        'push',
        '-d', 'paginated',
        '-o', remote.url,
        '--credentials', remote.credsfile
    ])
    assert result.exit_code == 0
    assert 'PUSH: 100%|##########| 3/3' in result.stderr

    geodb.write('salis', [
        {'kodas': 'test', 'pavadinimas': 'Test', 'id': 10},
    ])

    result = cli.invoke(localrc, [
        'push',
        '-d', 'paginated',
        '-o', remote.url,
        '--credentials', remote.credsfile,
        '--incremental'
    ])
    assert result.exit_code == 0
    assert 'PUSH: 100%|##########| 1/1' in result.stderr


def test_push_pagination_without_incremental(
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request
):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type     | ref      | source      | level | access
    paginated             |          |          |             |       |
      | db                   | sql      |          |             |       |
      |   |   |   |          |          |          |             |       |
      |   |   | Country      |          | id       | salis       | 4     |
      |   |   |   | code     | string   |          | kodas       | 4     | open
      |   |   |   | name     | string   |          | pavadinimas | 4     | open
      |   |   |   | id       | integer  |          | id          | 4     | open
    '''))

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == 'https://example.com/'
    result = cli.invoke(localrc, [
        'push',
        '-d', 'paginated',
        '-o', remote.url,
        '--credentials', remote.credsfile
    ])
    assert result.exit_code == 0
    assert 'PUSH: 100%|##########| 3/3' in result.stderr

    geodb.write('salis', [
        {'kodas': 'test', 'pavadinimas': 'Test', 'id': 10},
    ])

    result = cli.invoke(localrc, [
        'push',
        '-d', 'paginated',
        '-o', remote.url,
        '--credentials', remote.credsfile
    ])
    assert result.exit_code == 0
    assert 'PUSH: 100%|##########| 4/4' in result.stderr


def test_push_pagination_incremental_with_page_valid(
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request
):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type     | ref      | source      | level | access
    paginated             |          |          |             |       |
      | db                   | sql      |          |             |       |
      |   |   |   |          |          |          |             |       |
      |   |   | Country      |          | id       | salis       | 4     |
      |   |   |   | code     | string   |          | kodas       | 4     | open
      |   |   |   | name     | string   |          | pavadinimas | 4     | open
      |   |   |   | id       | integer  |          | id          | 4     | open
    '''))

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == 'https://example.com/'
    result = cli.invoke(localrc, [
        'push',
        '-d', 'paginated',
        '-o', remote.url,
        '--credentials', remote.credsfile,
        '--incremental',
        '--model', 'paginated/Country',
        '--page', '2'
    ])
    assert result.exit_code == 0
    assert 'PUSH: 100%|##########| 1/1' in result.stderr

    geodb.write('salis', [
        {'kodas': 'test', 'pavadinimas': 'Test', 'id': 10},
    ])

    result = cli.invoke(localrc, [
        'push',
        '-d', 'paginated',
        '-o', remote.url,
        '--credentials', remote.credsfile,
        '--incremental',
        '--model', 'paginated/Country',
        '--page', '2'
    ])
    assert result.exit_code == 0
    assert 'PUSH: 100%|##########| 2/2' in result.stderr


def test_push_pagination_incremental_with_page_invalid(
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request
):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type     | ref      | source      | level | access
    paginated             |          |          |             |       |
      | db                   | sql      |          |             |       |
      |   |   |   |          |          |          |             |       |
      |   |   | Country      |          | id       | salis       | 4     |
      |   |   |   | code     | string   |          | kodas       | 4     | open
      |   |   |   | name     | string   |          | pavadinimas | 4     | open
      |   |   |   | id       | integer  |          | id          | 4     | open
    '''))

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == 'https://example.com/'
    result = cli.invoke(localrc, [
        'push',
        '-d', 'paginated',
        '-o', remote.url,
        '--credentials', remote.credsfile,
        '--incremental',
        '--model', 'paginated/Country',
        '--page', '2',
        '--page', 'test'
    ], fail=False)
    assert result.exit_code == 1
