import pytest
import sqlalchemy as sa
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.data import listdata
from spinta.testing.datasets import create_sqlite_db
from spinta.testing.tabular import create_tabular_manifest
from tests.datasets.test_sql import configure_remote_server, create_rc


@pytest.fixture(scope='module')
def base_geodb():
    with create_sqlite_db({
        'city': [
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('code', sa.Text),
            sa.Column('name', sa.Text),
            sa.Column('location', sa.Text),
        ],
        'location': [
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('name', sa.Text),
            sa.Column('code', sa.Text),
        ]
    }) as db:
        db.write('location', [
            {'code': 'lt', 'name': 'Vilnius', 'id': 1},
            {'code': 'lv', 'name': 'Ryga', 'id': 2},
            {'code': 'ee', 'name': 'Talin', 'id': 3},
        ])
        db.write('city', [
            {'id': 2, 'name': 'Ryga', 'code': 'lv', 'location': 'Latvia'},
        ])
        yield db


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


def test_push_with_base(
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    request,
    base_geodb
):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type     | ref      | source      | level | access
    level4basedataset            |          |          |             |       |
      | db                   | sql      |          |             |       |
      |   |   | Location     |          | id       | location    | 4     |
      |   |   |   | id       | integer  |          | id          | 4     | open
      |   |   |   | name     | string   |          | name        | 4     | open
      |   |   |   | code     | string   |          | code        | 4     | open
      |   |   |   |          |          |          |             |       |
      |   | Location |           |          |          |          |             |       |
      |   |   | City         |          | id       | city        | 4     |
      |   |   |   | code     |    |          | code        | 4     | open
      |   |   |   | name     |    |          | name        | 4     | open
      |   |   |   | id       | integer  |          | id          | 4     | open
      |   |   |   | location | string   |          | location    | 4     | open
    '''))

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, base_geodb)
    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == 'https://example.com/'
    result = cli.invoke(localrc, [
        'push',
        '-d', 'level4basedataset',
        '-o', remote.url,
        '--credentials', remote.credsfile,
        '--no-progress-bar',
    ])

    assert result.exit_code == 0
    remote.app.authmodel('level4basedataset/Location', ['getall', 'search'])
    resp_location = remote.app.get('level4basedataset/Location')

    locations = listdata(resp_location, '_id', 'id')
    ryga_id = None
    for _id, id in locations:
        if id == 2:
            ryga_id = _id
            break

    remote.app.authmodel('level4basedataset/City', ['getall', 'search'])
    resp_city = remote.app.get('level4basedataset/City')
    assert resp_city.status_code == 200
    assert listdata(resp_city, '_id', 'name') == [(ryga_id, 'Ryga')]
    assert len(listdata(resp_city, 'id', 'name', 'location', 'code')) == 1


def test_push_with_base_different_ref(
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    request,
    base_geodb
):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type     | ref      | source      | level | access
    level4basedatasetref           |          |          |             |       |
      | db                   | sql      |          |             |       |
      |   |   | Location     |          | id       | location    | 4     |
      |   |   |   | id       | integer  |          | id          | 4     | open
      |   |   |   | name     | string   |          | name        | 4     | open
      |   |   |   | code     | string   |          | code        | 4     | open
      |   |   |   |          |          |          |             |       |
      |   | Location |           |          |          | name     |             | 3     |
      |   |   | City         |          | id       | city        | 4     |
      |   |   |   | code     |    |          | code        | 4     | open
      |   |   |   | name     |    |          | name        | 4     | open
      |   |   |   | id       | integer  |          | id          | 4     | open
      |   |   |   | location | string   |          | location    | 4     | open
    '''))

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, base_geodb)
    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == 'https://example.com/'
    result = cli.invoke(localrc, [
        'push',
        '-d', 'level4basedatasetref',
        '-o', remote.url,
        '--credentials', remote.credsfile,
        '--no-progress-bar',
    ])

    assert result.exit_code == 0
    remote.app.authmodel('level4basedatasetref/Location', ['getall', 'search'])
    resp_location = remote.app.get('level4basedatasetref/Location')

    locations = listdata(resp_location, '_id', 'name')
    ryga_id = None
    for _id, name in locations:
        if name == 'Ryga':
            ryga_id = _id
            break

    remote.app.authmodel('level4basedatasetref/City', ['getall', 'search'])
    resp_city = remote.app.get('level4basedatasetref/City')
    assert resp_city.status_code == 200
    assert listdata(resp_city, '_id', 'name') == [(ryga_id, 'Ryga')]
    assert len(listdata(resp_city, 'id', 'name', 'location', 'code')) == 1
