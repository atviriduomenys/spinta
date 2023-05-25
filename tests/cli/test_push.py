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
            sa.Column('country', sa.Text),
        ]
    }) as db:
        db.write('salis', [
            {'kodas': 'lt', 'pavadinimas': 'Lietuva'},
            {'kodas': 'lv', 'pavadinimas': 'Latvija'},
            {'kodas': 'ee', 'pavadinimas': 'Estija'},
        ])
        db.write('cities', [
            {'name': 'Vilnius', 'country': 'lt'},
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



def test_push_with_resource_check(
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request
):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property  | type   | ref     | source       | access
    datasets/gov/exampleRes   |        |         |              |
      | data                  | sql    |         |              |
      |   |   | countryRes    |        | code    | salis        |
      |   |   |   | code      | string |         | kodas        | open
      |   |   |   | name      | string |         | pavadinimas  | open
      |   |                   |        |         |              |
      |   |   | countryNoRes  |        |         |              |
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

    remote.app.authmodel('datasets/gov/exampleRes/countryRes', ['getall'])
    resp_res = remote.app.get('/datasets/gov/exampleRes/countryRes')

    remote.app.authmodel('datasets/gov/exampleRes/countryNoRes', ['getall'])
    resp_no_res = remote.app.get('/datasets/gov/exampleRes/countryNoRes')

    assert len(listdata(resp_res)) == 3
    assert len(listdata(resp_no_res)) == 0


def test_push_ref_with_level(
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request
):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type    | ref                             | source   | level | access
    leveldataset             |         |                                 |          |       |
      | db                   | sql     |                                 |          |       |
      |   |   | City         |         | id                              | cities   | 4     |
      |   |   |   | id       | integer |                                 | id       | 4     | open
      |   |   |   | name     | string  |                                 | name     | 2     | open
      |   |   |   | country  | ref     | /leveldataset/countries/Country | country  | 3     | open
      |   |   |   |          |         |                                 |          |       |
    leveldataset/countries   |         |                                 |          |       |
      |   |   | Country      |         | code                            |          | 4     |
      |   |   |   | code     | string  |                                 |          | 4     | open
      |   |   |   | name     | string  |                                 |          | 2     | open
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
        '-d', 'leveldataset',
        '-o', remote.url,
        '--credentials', remote.credsfile,
        '--no-progress-bar',
    ])
    assert result.exit_code == 0

    result = cli.invoke(localrc, [
        'push',
        '-d', 'leveldataset/countries',
        '-o', remote.url,
        '--credentials', remote.credsfile,
        '--no-progress-bar',
    ])
    assert result.exit_code == 0

    remote.app.authmodel('leveldataset/City', ['getall', 'search'])
    resp_city = remote.app.get('leveldataset/City')

    assert resp_city.status_code == 200
    assert listdata(resp_city, 'name') == ['Vilnius']
    assert len(listdata(resp_city, 'id', 'name', 'country')) == 1
    assert 'code' in listdata(resp_city, 'country')[0].keys()
