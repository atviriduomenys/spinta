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
        ]
    }) as db:
        db.write('salis', [
            {'kodas': 'lt', 'pavadinimas': 'Lietuva'},
            {'kodas': 'lv', 'pavadinimas': 'Latvija'},
            {'kodas': 'ee', 'pavadinimas': 'Estija'},
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
      |   |                   |        |         |              |
      |   |   | countryRes    |        | code    | salis        |
      |   |   |   | code      | string |         | kodas        | open
      |   |   |   | name      | string |         | pavadinimas  | open
    datasets/gov/exampleNoRes |        |         |              |
      |   |   | countryNoRes  |        | code    | salis        |
      |   |   |   | code      | string |         | kodas        | open
      |   |   |   | name      | string |         | pavadinimas  | open
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
    ])
    assert result.exit_code == 0
    assert result.stderr == ""

    remote.app.authmodel('datasets/gov/exampleRes/countryRes', ['getall'])
    resp_res = remote.app.get('/datasets/gov/exampleRes/countryRes')

    remote.app.authmodel('datasets/gov/exampleNoRes/countryNoRes', ['getall'])
    resp_no_res = remote.app.get('/datasets/gov/exampleRes/countryNoRes')

    assert len(listdata(resp_res)) == 1
    assert len(listdata(resp_no_res)) == 0

