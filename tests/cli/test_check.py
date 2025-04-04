import pytest

from spinta.components import Context
from spinta.exceptions import InvalidValue
from spinta.testing.cli import SpintaCliRunner
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.tabular import create_tabular_manifest


def test_check_status(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type   | ref     | source      | prepare | access | status
    datasets/gov/example     |        |         |             |         |        |
      | data                 | sql    |         |             |         |        |
                             |        |         |             |         |        |
      |   |   | Country      |        | code    | salis       |         |        | develop
      |   |   |   | code     | string |         | kodas       |         | public | develop
      |   |   |   | name     | string |         | pavadinimas |         | open   | completed
      |   |   |   | driving  | string |         | vairavimas  |         | open   | discont
                             | enum   |         | l           | 'left'  | open   | completed
                             |        |         | r           | 'right' | open   | discont
                             |        |         |             |         |        |
      |   |   | City         |        | name    | miestas     |         |        | completed
      |   |   |   | name     | string |         | pavadinimas |         | open   | deprecated
      |   |   |   | country  | ref    | Country | salis       |         | open   | withdrawn
    '''))

    cli.invoke(rc, [
        'check',
        tmp_path / 'manifest.csv',
    ])


def test_check_visibility(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type   | ref     | source      | prepare | access | visibility
    datasets/gov/example     |        |         |             |         |        |
      | data                 | sql    |         |             |         |        |
                             |        |         |             |         |        |
      |   |   | Country      |        | code    | salis       |         |        | public
      |   |   |   | code     | string |         | kodas       |         | public | public
      |   |   |   | name     | string |         | pavadinimas |         | open   | package
      |   |   |   | driving  | string |         | vairavimas  |         | open   | protected
                             | enum   |         | l           | 'left'  | open   | public
                             |        |         | r           | 'right' | open   | package
                             |        |         |             |         |        |
      |   |   | City         |        | name    | miestas     |         |        |
      |   |   |   | name     | string |         | pavadinimas |         | open   | private
      |   |   |   | country  | ref    | Country | salis       |         | open   | private
    '''))

    cli.invoke(rc, [
        'check',
        tmp_path / 'manifest.csv',
    ])


def test_check_eli(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type   | ref     | source      | prepare | access | eli
    datasets/gov/example     |        |         |             |         |        |
      | data                 | sql    |         |             |         |        |
                             |        |         |             |         |        |
      |   |   | Country      |        | code    | salis       |         |        | https://example.com/law/1
      |   |   |   | code     | string |         | kodas       |         | public | https://example.com/law/2
      |   |   |   | name     | string |         | pavadinimas |         | open   |
      |   |   |   | driving  | string |         | vairavimas  |         | open   | https://example.com/law/3
                             | enum   |         | l           | 'left'  | open   | https://example.com/law/4
                             |        |         | r           | 'right' | open   | https://example.com/law/5
    '''))

    cli.invoke(rc, [
        'check',
        tmp_path / 'manifest.csv',
    ])


def test_check_count(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type   | ref     | source      | prepare | access | count
    datasets/gov/example     |        |         |             |         |        | 4
      | data                 | sql    |         |             |         |        |
                             |        |         |             |         |        |
      |   |   | Country      |        | code    | salis       |         |        | 5 
      |   |   |   | code     | string |         | kodas       |         | public | 6
      |   |   |   | name     | string |         | pavadinimas |         | open   |
      |   |   |   | driving  | string |         | vairavimas  |         | open   | 7
                             | enum   |         | l           | 'left'  | open   | 8
                             |        |         | r           | 'right' | open   | 9
    '''))

    cli.invoke(rc, [
        'check',
        tmp_path / 'manifest.csv',
    ])


def test_check_origin(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type   | ref     | source      | prepare | access | origin
    datasets/gov/example     |        |         |             |         |        |
      | data                 | sql    |         |             |         |        |
                             |        |         |             |         |        |
      |   |   | Country      |        | code    | salis       |         |        |
      |   |   |   | code     | string |         | kodas       |         | public |
      |   |   |   | name     | string |         | pavadinimas |         | open   |
      |   |   |   | driving  | string |         | vairavimas  |         | open   |
                             | enum   |         | l           | 'left'  | open   |
                             |        |         | r           | 'right' | open   |
    datasets/gov/example2    |        |         |             |         |        |
      | data                 | sql    |         |             |         |        |        
                             |        |         |             |         |        |              
      |   |   | Country      |        | code    | salis       |         |        | datasets/gov/example/Country
      |   |   |   | code     | string |         | kodas       |         | public | code
      |   |   |   | name     | string |         | pavadinimas |         | open   | name
    '''))

    cli.invoke(rc, [
        'check',
        tmp_path / 'manifest.csv',
    ])


def test_check_level(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type   | ref     | source      | prepare | access | level
    datasets/gov/example     |        |         |             |         |        |
      | data                 | sql    |         |             |         |        |
                             |        |         |             |         |        |
      |   |   | Country      |        | code    | salis       |         |        | 
      |   |   |   | code     | string |         | kodas       |         | public | 1
      |   |   |   | name     | string |         | pavadinimas |         | open   | 2
      |   |   |   | driving  | string |         | vairavimas  |         | open   | 3
                             | enum   |         | l           | 'left'  | open   | 9
                             |        |         | r           | 'right' | open   |
    datasets/gov/example2    |        |         |             |         |        |
      | data                 | sql    |         |             |         |        |        
                             |        |         |             |         |        |              
      |   |   | Country      |        | code    | salis       |         |        |
      |   |   |   | code     | string |         | kodas       |         | public |
      |   |   |   | name     | string |         | pavadinimas |         | open   |
    '''))

    result = cli.invoke(rc, [
        'check',
        tmp_path / 'manifest.csv',
    ],
        fail=False)

    assert result.exit_code != 0
    assert result.exc_info[0] is InvalidValue
    assert result.exception.context.get('given') == 9


def test_check_access(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type   | ref     | source      | prepare | access | level
    datasets/gov/example     |        |         |             |         |        |
      | data                 | sql    |         |             |         |        |
                             |        |         |             |         |        |
      |   |   | Country      |        | code    | salis       |         |        | 
      |   |   |   | code     | string |         | kodas       |         | public | 1
      |   |   |   | name     | string |         | pavadinimas |         | open   | 2
      |   |   |   | driving  | string |         | vairavimas  |         | open   | 3
                             | enum   |         | l           | 'left'  | open   | 
                             |        |         | r           | 'right' | test   |
    datasets/gov/example2    |        |         |             |         |        |
      | data                 | sql    |         |             |         |        |        
                             |        |         |             |         |        |              
      |   |   | Country      |        | code    | salis       |         |        |
      |   |   |   | code     | string |         | kodas       |         | public |
      |   |   |   | name     | string |         | pavadinimas |         | open   |
    '''))

    result = cli.invoke(rc, [
        'check',
        tmp_path / 'manifest.csv'
    ],
        fail=False)

    assert result.exit_code != 0
    assert result.exc_info[0] is InvalidValue
    assert result.exception.context.get('given') == "test"


def test_check_namespace_two_manifests(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(context, tmp_path / 'manifest1.csv', striptable('''
id | d | r | b | m | property       | type   | ref  | level | 
   | datasets/gov/test              | ns     |      |       | 
   |                                |        |      |       | 
   | datasets/gov/test/example1     |        |      |       | 
   |                                |        |      |       | 
   |   |   |   | Country            |        | code | 4     | 
   |   |   |   |   | name           | string |      | 4     | 
   |   |   |   |   | code           | string |      | 4     | 

    '''))

    create_tabular_manifest(context, tmp_path / 'manifest2.csv', striptable('''
id | d | r | b | m | property       | type   | ref  | level |
   | datasets/gov/test/example2     |        |      |       |
   |                                |        |      |       |
   |   |   |   | City               |        | code | 4     |
   |   |   |   |   | name           | string |      | 4     |
   |   |   |   |   | code           | string |      | 4     |
'''))

    cli.invoke(rc, [
        'check',
        tmp_path / 'manifest1.csv',
        tmp_path / 'manifest2.csv',

    ])
    cli.invoke(rc, [
        'check',
        tmp_path / 'manifest1.csv',
        tmp_path / 'manifest2.csv',

    ])
