import pytest
import sqlalchemy as sa
import sqlalchemy_utils as su

from spinta.exceptions import UnauthorizedKeymapSync
from spinta.manifests.tabular.helpers import striptable
from spinta.core.config import RawConfig
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.client import create_client, create_rc, configure_remote_server, create_test_client
from spinta.testing.data import listdata
from spinta.testing.datasets import create_sqlite_db, Sqlite
from spinta.testing.tabular import create_tabular_manifest


@pytest.fixture(scope='function')
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


@pytest.fixture(scope='function')
def text_geodb():
    with create_sqlite_db({
        'city': [
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('name', sa.Text),
            sa.Column('name_lt', sa.Text),
            sa.Column('name_pl', sa.Text),
            sa.Column('country', sa.Integer),
        ],
        'country': [
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('name_lt', sa.Text),
            sa.Column('name_en', sa.Text),
        ],
    }) as db:
        db.write('country', [
            {'name_lt': 'Lietuva', 'name_en': None, 'id': 1},
            {'name_lt': None, 'name_en': 'Latvia', 'id': 2},
            {'name_lt': 'Lenkija', 'name_en': 'Poland', 'id': 3},
        ])
        db.write('city', [
            {'id': 1, 'name': 'VLN', 'name_lt': 'Vilnius', 'name_pl': 'Vilna', 'country': 1},
        ])
        yield db


@pytest.fixture(scope='function')
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
        ],
        'nullable': [
            sa.Column('id', sa.Integer),
            sa.Column('name', sa.Text),
            sa.Column('code', sa.Text)
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
        db.write('nullable', [
            {'id': 0, 'name': 'Test', 'code': '0'},
            {'id': 0, 'name': 'Test', 'code': '1'},
            {'id': 0, 'name': 'Test0', 'code': None},
            {'id': 0, 'name': None, 'code': '0'},
            {'id': 0, 'name': None, 'code': None},
            {'id': 1, 'name': 'Test', 'code': None},
            {'id': 1, 'name': None, 'code': None},
            {'id': None, 'name': 'Test', 'code': None},
            {'id': None, 'name': 'Test', 'code': '0'},

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
    context,
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request
):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
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
    context,
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request
):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
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
    context,
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    errordb,
    request
):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
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
    context,
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    request
):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable(f'''
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
    context,
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request
):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type     | ref      | source      | level | access
    level3dataset            |          |          |             |       |
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
    assert listdata(resp_city, 'id', 'name', 'country')[0] == (1, 'Vilnius', {'id': 2})
    assert 'id' in listdata(resp_city, 'country')[0].keys()


def test_push_ref_with_level_4(
    context,
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request
):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type     | ref      | source      | level | access
    level4dataset            |          |          |             |       |
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


def test_push_with_resource_check(
    context,
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request
):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property  | type   | ref     | source       | access
    datasets/gov/exampleRes   |        |         |              |
      | data                  | sql    |         |              |
      |   |   | CountryRes    |        | code    | salis        |
      |   |   |   | code      | string |         | kodas        | open
      |   |   |   | name      | string |         | pavadinimas  | open
      |   |                   |        |         |              |
    datasets/gov/exampleNoRes |        |         |              |
      |   |   | CountryNoRes  |        |         |              |
      |   |   |   | code      | string |         |              | open
      |   |   |   | name      | string |         |              | open
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
        '-d', 'datasets/gov/exampleRes',
        '-o', remote.url,
        '--credentials', remote.credsfile,
        '--no-progress-bar',
    ])
    assert result.exit_code == 0

    result = cli.invoke(localrc, [
        'push',
        '-d', 'datasets/gov/exampleNoRes',
        '-o', remote.url,
        '--credentials', remote.credsfile,
        '--no-progress-bar',
    ])
    assert result.exit_code == 0

    remote.app.authmodel('datasets/gov/exampleRes/CountryRes', ['getall'])
    resp_res = remote.app.get('/datasets/gov/exampleRes/CountryRes')
    assert len(listdata(resp_res)) == 3

    remote.app.authmodel('datasets/gov/exampleNoRes/CountryNoRes', ['getall'])
    resp_no_res = remote.app.get('/datasets/gov/exampleNoRes/CountryNoRes')
    assert len(listdata(resp_no_res)) == 0


def test_push_ref_with_level_no_source(
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
    leveldataset             |         |                                 |                |       |
      | db                   | sql     |                                 |                |       |
      |   |   | City         |         | id                              | cities         | 4     |
      |   |   |   | id       | integer |                                 | id             | 4     | open
      |   |   |   | name     | string  |                                 | name           | 2     | open
      |   |   |   | country  | ref     | /leveldataset/countries/Country | country        | 3     | open
      |   |   |   |          |         |                                 |                |       |
    leveldataset/countries   |         |                                 |                |       |
      |   |   | Country      |         | code                            |                | 4     |
      |   |   |   | code     | string  |                                 |                | 4     | open
      |   |   |   | name     | string  |                                 |                | 2     | open
    '''
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable(table))

    app = create_client(rc, tmp_path, geodb)
    app.authmodel('leveldataset', ['getall'])
    resp = app.get('leveldataset/City')
    assert listdata(resp, 'id', 'name', 'country')[0] == (1, 'Vilnius', {'code': 2})

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == 'https://example.com/'
    result = cli.invoke(localrc, [
        'push',
        '-o', remote.url,
        '--credentials', remote.credsfile,
        '--no-progress-bar',
    ])
    assert result.exit_code == 0
    remote.app.authmodel('leveldataset/City', ['getall', 'search'])
    resp_city = remote.app.get('leveldataset/City')

    assert resp_city.status_code == 200
    assert listdata(resp_city, 'name') == ['Vilnius']
    assert listdata(resp_city, 'id', 'name', 'country')[0] == (1, 'Vilnius', {'code': '2'})


def test_push_ref_with_level_no_source_status_code_400_check(
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
    leveldataset             |         |                                 |                |       |
      | db                   | sql     |                                 |                |       |
      |   |   | City         |         | id                              | cities         | 4     |
      |   |   |   | id       | integer |                                 | id             | 4     | open
      |   |   |   | name     | string  |                                 | name           | 2     | open
      |   |   |   | country  | ref     | /leveldataset/countries/Country | country        | 3     | open
      |   |   |   |          |         |                                 |                |       |
    leveldataset/countries   |         |                                 |                |       |
      |   |   | Country      |         | code                            |                | 4     |
      |   |   |   | code     | string  |                                 |                | 4     | open
      |   |   |   | name     | string  |                                 |                | 2     | open
    '''

    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable(table))

    app = create_client(rc, tmp_path, geodb)
    app.authmodel('leveldataset', ['getall'])
    resp = app.get('leveldataset/City')
    assert listdata(resp, 'id', 'name', 'country')[0] == (1, 'Vilnius', {'code': 2})

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb, 'external')

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == 'https://example.com/'
    result = cli.invoke(localrc, [
        'push',
        '-o', remote.url,
        '--credentials', remote.credsfile,
        '--no-progress-bar',
    ])
    assert result.exit_code == 0
    remote.app.authmodel('leveldataset/countries/Country', ['getall', 'search'])
    resp_city = remote.app.get('leveldataset/countries/Country')

    assert resp_city.status_code == 400


def test_push_pagination_incremental(
    context,
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request
):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
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
    context,
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request
):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type     | ref      | source      | level | access
    paginated/without             |          |          |             |       |
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
        '-d', 'paginated/without',
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
        '-d', 'paginated/without',
        '-o', remote.url,
        '--credentials', remote.credsfile
    ])
    assert result.exit_code == 0
    assert 'PUSH: 100%|##########| 4/4' in result.stderr


def test_push_pagination_incremental_with_page_valid(
    context,
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request
):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type     | ref      | source      | level | access
    paginated/valid             |          |          |             |       |
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
        '-d', 'paginated/valid',
        '-o', remote.url,
        '--credentials', remote.credsfile,
        '--incremental',
        '--model', 'paginated/valid/Country',
        '--page', '2'
    ])
    assert result.exit_code == 0
    assert 'PUSH: 100%|##########| 1/1' in result.stderr

    geodb.write('salis', [
        {'kodas': 'test', 'pavadinimas': 'Test', 'id': 10},
    ])

    result = cli.invoke(localrc, [
        'push',
        '-d', 'paginated/valid',
        '-o', remote.url,
        '--credentials', remote.credsfile,
        '--incremental',
        '--model', 'paginated/valid/Country',
        '--page', '2'
    ])
    assert result.exit_code == 0
    assert 'PUSH: 100%|##########| 2/2' in result.stderr


def test_push_pagination_incremental_with_page_invalid(
    context,
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request
):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type     | ref      | source      | level | access
    paginated/invalid             |          |          |             |       |
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
        '-d', 'paginated/invalid',
        '-o', remote.url,
        '--credentials', remote.credsfile,
        '--incremental',
        '--model', 'paginated/invalid/Country',
        '--page', '2',
        '--page', 'test'
    ], fail=False)
    assert result.exit_code == 1


def test_push_with_base(
    context,
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    request,
    base_geodb
):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
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
    context,
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    request,
    base_geodb
):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type     | ref      | source      | level | access
    level4basedatasetref           |          |          |             |       |
      | db                   | sql      |          |             |       |
      |   |   | Location     |          | id       | location    | 4     |
      |   |   |   | id       | integer  |          | id          | 4     | open
      |   |   |   | name     | string   |          | name        | 4     | open
      |   |   |   | code     | string   |          | code        | 4     | open
      |   |   |   |          |          |          |             |       |
      |   | Location |           |          |          | name     |             | 4     |
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


def test_push_with_base_level_3(
    context,
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    request,
    base_geodb
):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    d | r | base     | m | property | type     | ref      | source      | level | access
    level3basedataset               |          |          |             |       |
      | db           |   |          | sql      |          |             |       |
      |   |          | Location     |          | id       | location    | 4     |
      |   |          |   | id       | integer  |          | id          | 4     | open
      |   |          |   | name     | string   |          | name        | 4     | open
      |   |          |   | code     | string   |          | code        | 4     | open
      |   |          |   |          |          |          |             |       |
      |   | Location |   |          |          | name     |             | 3     |
      |   |          | City         |          | id       | city        | 4     |
      |   |          |   | code     |          |          | code        | 4     | open
      |   |          |   | name     | string   |          | name        | 4     | open
      |   |          |   | id       | integer  |          | id          | 4     | open
      |   |          |   | location | string   |          | location    | 4     | open
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
        '-d', 'level3basedataset',
        '-o', remote.url,
        '--credentials', remote.credsfile,
        '--no-progress-bar',
    ])

    assert result.exit_code == 0
    remote.app.authmodel('level3basedataset/Location', ['getall', 'search'])
    resp_location = remote.app.get('level3basedataset/Location')

    locations = listdata(resp_location, '_id', 'name')
    ryga_id = None
    for _id, name in locations:
        if name == 'Ryga':
            ryga_id = _id
            break

    remote.app.authmodel('level3basedataset/City', ['getall', 'search'])
    resp_city = remote.app.get('level3basedataset/City')
    assert resp_city.status_code == 200
    assert listdata(resp_city, 'name') == ['Ryga']
    assert listdata(resp_city, '_id') != [ryga_id]
    assert len(listdata(resp_city, 'id', 'name', 'location', 'code')) == 1


def test_push_sync(
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

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == 'https://example.com/'
    remote.app.authmodel('syncdataset/countries/Country', ['insert', 'wipe'])
    resp = remote.app.post('https://example.com/syncdataset/countries/Country', json={
        'code': 2
    })
    country_id = resp.json()['_id']
    result = cli.invoke(localrc, [
        'push',
        '-o', remote.url,
        '--credentials', remote.credsfile,
        '--sync',
        '--no-progress-bar',
    ])
    assert result.exit_code == 0
    remote.app.authmodel('syncdataset/City', ['getall', 'search', 'wipe'])
    resp_city = remote.app.get('syncdataset/City')
    city_id = listdata(resp_city, '_id')[0]

    assert resp_city.status_code == 200
    assert listdata(resp_city, 'name') == ['Vilnius']
    assert listdata(resp_city, '_id', 'id', 'name', 'country')[0] == (city_id, 1, 'Vilnius', {'_id': country_id})

    # Reset data
    remote.app.delete('https://example.com/syncdataset/countries/City/:wipe')
    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    result = cli.invoke(localrc, [
        'push',
        '-o', remote.url,
        '--credentials', remote.credsfile,
        '--no-progress-bar',
    ])
    assert result.exit_code == 0
    resp_city = remote.app.get('syncdataset/City')

    assert resp_city.status_code == 200
    assert listdata(resp_city, 'name') == ['Vilnius']
    assert listdata(resp_city, '_id', 'id', 'name', 'country')[0] == (city_id, 1, 'Vilnius', {'_id': country_id})


@pytest.mark.skip("Private now sends warning that model has been skipped syncing rather throwing exception")
def test_push_sync_to_private_error(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request
):
    with pytest.raises(UnauthorizedKeymapSync):
        table = '''
            d | r | b | m | property | type    | ref                             | source         | level | access
            syncdataset              |         |                                 |                |       |
              | db                   | sql     |                                 |                |       |
              |   |   | City         |         | id                              | cities         | 4     |
              |   |   |   | id       | integer |                                 | id             | 4     | open
              |   |   |   | name     | string  |                                 | name           | 2     | open
              |   |   |   | country  | ref     | /syncdataset/countries/Country | country        | 4     | open
              |   |   |   |          |         |                                 |                |       |
            syncdataset/countries    |         |                                 |                |       |
              |   |   | Country      |         | code                            |                | 4     |
              |   |   |   | code     | integer |                                 |                | 4     | private
              |   |   |   | name     | string  |                                 |                | 2     | open
            '''
        create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable(table))

        # Configure local server with SQL backend
        localrc = create_rc(rc, tmp_path, geodb)

        # Configure remote server
        remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
        request.addfinalizer(remote.app.context.wipe_all)

        # Push data from local to remote.
        assert remote.url == 'https://example.com/'
        result = cli.invoke(localrc, [
            'push',
            '-o', remote.url,
            '--credentials', remote.credsfile,
            '--sync',
            '--no-progress-bar',
        ], fail=False)
        raise result.exception


def test_push_sync_private_no_error(
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
        syncdataset              |         |                                 |                |       |
          | db                   | sql     |                                 |                |       |
          |   |   | City         |         | id                              | cities         | 4     |
          |   |   |   | id       | integer |                                 | id             | 4     | open
          |   |   |   | name     | string  |                                 | name           | 2     | open
          |   |   |   | country  | ref     | /syncdataset/countries/Country  | country        | 4     | private
          |   |   |   |          |         |                                 |                |       |
        syncdataset/countries    |         |                                 |                |       |
          |   |   | Country      |         | code                            |                | 4     |
          |   |   |   | code     | integer |                                 |                | 4     | private
          |   |   |   | name     | string  |                                 |                | 2     | open
        '''
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable(table))

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == 'https://example.com/'
    result = cli.invoke(localrc, [
        'push',
        '-o', remote.url,
        '--credentials', remote.credsfile,
        '--sync',
        '--no-progress-bar',
    ], fail=False)
    assert result.exception is None


def test_push_with_text(
    context,
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    request,
    text_geodb
):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type     | ref      | source      | level | access
    textnormal               |          |          |             |       |
      | db                   | sql      |          |             |       |
      |   |   | Country      |          | id       | country     | 4     |
      |   |   |   | id       | integer  |          | id          | 4     | open
      |   |   |   | name@lt  | string   |          | name_lt     | 4     | open
      |   |   |   | name@en  | string   |          | name_en     | 4     | open
    '''))

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, text_geodb)
    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == 'https://example.com/'
    result = cli.invoke(localrc, [
        'push',
        '-d', 'textnormal',
        '-o', remote.url,
        '--credentials', remote.credsfile,
        '--no-progress-bar',
    ])

    assert result.exit_code == 0
    remote.app.authmodel('textnormal/Country', ['getall', 'search'])
    countries = remote.app.get('textnormal/Country?select(id,name@lt,name@en)')
    assert countries.status_code == 200
    assert listdata(countries, 'id', 'name', sort=True) == [
        (1, {'en': None, 'lt': 'Lietuva'}),
        (2, {'en': 'Latvia', 'lt': None}),
        (3, {'en': 'Poland', 'lt': 'Lenkija'})
    ]


def test_push_with_text_unknown(
    context,
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    request,
    text_geodb
):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type     | ref      | source      | level | access
    textunknown              |          |          |             |       |
      | db                   | sql      |          |             |       |
      |   |   | City         |          | id       | city        | 4     |
      |   |   |   | id       | integer  |          | id          | 4     | open
      |   |   |   | name@lt  | string   |          | name_lt     | 4     | open
      |   |   |   | name@pl  | string   |          | name_pl     | 4     | open
      |   |   |   | name     | text     |          | name        | 2     | open
    '''))

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, text_geodb)
    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == 'https://example.com/'
    result = cli.invoke(localrc, [
        'push',
        '-d', 'textunknown',
        '-o', remote.url,
        '--credentials', remote.credsfile,
        '--no-progress-bar',
    ])

    assert result.exit_code == 0
    remote.app.authmodel('textunknown/City', ['getall', 'search'])
    countries = remote.app.get('textunknown/City?select(id,name@lt,name@pl,name@C)')
    assert countries.status_code == 200
    assert listdata(countries, 'id', 'name', sort=True) == [
        (1, {'': 'VLN', 'lt': 'Vilnius', 'pl': 'Vilna'}),
    ]


def test_push_postgresql(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    request,
    geodb,
):
    db = f'{postgresql}/push_db'
    if su.database_exists(db):
        su.drop_database(db)
    su.create_database(db)
    engine = sa.create_engine(db)
    with engine.connect() as conn:
        meta = sa.MetaData(conn)
        table = sa.Table(
            'cities',
            meta,
            sa.Column('id', sa.Integer),
            sa.Column('name', sa.Text)
        )
        meta.create_all()
        conn.execute(
            table.insert((0, "Test"))
        )
        conn.execute(
            table.insert((1, "Test1"))
        )
    table = f'''
        d | r | b | m | property | type    | ref                             | source         | level | access
        postgrespush              |         |                                |                |       |
          | db                   | sql     |                                 | {db}           |       |
          |   |   | City         |         | id                              | cities         | 4     |
          |   |   |   | id       | integer |                                 | id             | 4     | open
          |   |   |   | name     | string  |                                 | name           | 2     | open
        '''
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable(table))

    # Configure local server with SQL backend
    tmp = Sqlite(db)
    localrc = create_rc(rc, tmp_path, tmp)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == 'https://example.com/'
    result = cli.invoke(localrc, [
        'push',
        '-o', remote.url,
        '--credentials', remote.credsfile
    ], fail=False)
    assert result.exit_code == 0
    assert 'PUSH: 100%|##########| 2/2' in result.stderr

    result = cli.invoke(localrc, [
        'push',
        '-o', remote.url,
        '--credentials', remote.credsfile,
        '-i'
    ], fail=False)
    assert result.exit_code == 0
    assert 'PUSH: 100%|##########| 2/2' not in result.stderr
    su.drop_database(db)


def test_push_with_nulls(
    context,
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request
):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type     | ref            | source      | level | access
    nullpush                 |          |                |             |       |
      | db                   | sql      |                |             |       |
      |   |   | Nullable     |          | id, name, code | nullable    | 4     |
      |   |   |   | id       | integer  |                | id          | 4     | open
      |   |   |   | name     | string   |                | name        | 4     | open
      |   |   |   | code     | string   |                | code        | 4     | open
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
        '-d', 'nullpush',
        '-o', remote.url,
        '--credentials', remote.credsfile,
        '--no-progress-bar',
    ])
    assert result.exit_code == 0

    remote.app.authmodel('nullpush/Nullable', ['getall', 'search'])
    resp_city = remote.app.get('nullpush/Nullable')

    assert resp_city.status_code == 200
    assert listdata(resp_city, 'id', 'name', 'code') == [
        (0, 'Test', '0'),
        (0, 'Test', '1'),
        (0, 'Test0', None),
        (0, None, '0'),
        (0, None, None),
        (1, 'Test', None),
        (1, None, None),
        (None, 'Test', '0'),
        (None, 'Test', None),
    ]
    assert len(listdata(resp_city, 'id', 'name', 'country')) == 9
