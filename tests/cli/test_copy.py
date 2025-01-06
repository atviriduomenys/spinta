from pathlib import Path

from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.testing.cli import SpintaCliRunner
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.context import create_test_context
from spinta.testing.tabular import create_tabular_manifest
from spinta.testing.manifest import load_manifest


def test_copy(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type   | ref     | source      | prepare | access
    datasets/gov/example     |        |         |             |         |
      | data                 | sql    |         |             |         |
                             |        |         |             |         |
      |   |   | Country      |        | code    | salis       |         |
      |   |   |   | code     | string |         | kodas       |         | public
      |   |   |   | name     | string |         | pavadinimas |         | open
      |   |   |   | driving  | string |         | vairavimas  |         | open
                             | enum   |         | l           | 'left'  | open
                             |        |         | r           | 'right' | open
                             |        |         |             |         |
      |   |   | City         |        | name    | miestas     |         |
      |   |   |   | name     | string |         | pavadinimas |         | open
      |   |   |   | country  | ref    | Country | salis       |         | open
                             |        |         |             |         |
      |   |   | Capital      |        | name    | miestas     |         |
      |   |   |   | name     | string |         | pavadinimas |         |
      |   |   |   | country  | ref    | Country | salis       |         |
    '''))

    cli.invoke(rc, [
        'copy',
        '--no-source',
        '--access', 'open',
        '-o', tmp_path / 'result.csv',
        tmp_path / 'manifest.csv',
    ])

    manifest = load_manifest(rc, tmp_path / 'result.csv')
    assert manifest == '''
    d | r | b | m | property | type   | ref     | source | prepare | access
    datasets/gov/example     |        |         |        |         |
                             |        |         |        |         |
      |   |   | Country      |        |         |        |         |
      |   |   |   | name     | string |         |        |         | open
      |   |   |   | driving  | string |         |        |         | open
                             | enum   |         |        | 'left'  | open
                             |        |         |        | 'right' | open
                             |        |         |        |         |
      |   |   | City         |        |         |        |         |
      |   |   |   | name     | string |         |        |         | open
      |   |   |   | country  | ref    | Country |        |         | open
    '''


def test_copy_enum_0(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type    | ref     | source      | prepare | access
    datasets/gov/example     |         |         |             |         |
      | data                 | sql     |         |             |         |
                             |         |         |             |         |
      |   |   | Country      |         | name    | salis       |         |
      |   |   |   | name     | string  |         | pavadinimas |         | open
      |   |   |   | driving  | integer |         | vairavimas  |         | open
                             | enum    |         | l           | 0       | open
                             |         |         | r           | 1       | open
    '''))

    cli.invoke(rc, [
        'copy',
        '--no-source',
        '--access', 'open',
        '-o', tmp_path / 'result.csv',
        tmp_path / 'manifest.csv',
  ])

    manifest = load_manifest(rc, tmp_path / 'result.csv')
    assert manifest == '''
    d | r | b | m | property | type    | ref     | source | prepare | access
    datasets/gov/example     |         |         |        |         |
                             |         |         |        |         |
      |   |   | Country      |         |         |        |         |
      |   |   |   | name     | string  |         |        |         | open
      |   |   |   | driving  | integer |         |        |         | open
                             | enum    |         |        | 0       | open
                             |         |         |        | 1       | open
    '''


def test_copy_global_enum(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type    | ref       | source      | prepare | access
    datasets/gov/example     |         |           |             |         |
                             | enum    | direction | l           | 0       |
                             |         |           | r           | 1       |
      | data                 | sql     |           |             |         |
                             |         |           |             |         |
      |   |   | Country      |         | name      | salis       |         |
      |   |   |   | name     | string  |           | pavadinimas |         | open
      |   |   |   | driving  | integer | direction | vairavimas  |         | open
    '''))

    cli.invoke(rc, [
        'copy',
        '--no-source',
        '--access', 'open',
        '-o', tmp_path / 'result.csv',
        tmp_path / 'manifest.csv',
    ])

    manifest = load_manifest(rc, tmp_path / 'result.csv')
    assert manifest == '''
    d | r | b | m | property | type    | ref       | source | prepare | access
    datasets/gov/example     |         |           |        |         |
                             | enum    | direction |        | 0       |
                             |         |           |        | 1       |
                             |         |           |        |         |
      |   |   | Country      |         |           |        |         |
      |   |   |   | name     | string  |           |        |         | open
      |   |   |   | driving  | integer | direction |        |         | open
    '''


def test_copy_with_filters_and_externals(context: Context, rc, cli, tmp_path):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type   | ref     | source      | prepare   | access
    datasets/gov/example     |        |         |             |           |
      | data                 | sql    |         |             |           |
                             |        |         |             |           |
      |   |   | Country      |        | code    | salis       | code='lt' |
      |   |   |   | code     | string |         | kodas       |           | private
      |   |   |   | name     | string |         | pavadinimas |           | open
      |   |   |   | driving  | string |         | vairavimas  |           | open
      |   |   |   |          | enum   |         | l           | 'left'    | private
      |   |   |   |          |        |         | r           | 'right'   | open
                             |        |         |             |           |
      |   |   | City         |        | name    | miestas     |           |
      |   |   |   | name     | string |         | pavadinimas |           | open
      |   |   |   | country  | ref    | Country | salis       |           | open
                             |        |         |             |           |
      |   |   | Capital      |        | name    | miestas     |           |
      |   |   |   | name     | string |         | pavadinimas |           |
      |   |   |   | country  | ref    | Country | salis       |           |
    '''))

    cli.invoke(rc, [
        'copy',
        '--access', 'open',
        '-o', tmp_path / 'result.csv',
        tmp_path / 'manifest.csv',
    ])

    manifest = load_manifest(rc, tmp_path / 'result.csv')
    assert manifest == '''
    d | r | b | m | property | type   | ref     | source      | prepare   | level | access
    datasets/gov/example     |        |         |             |           |       |
      | data                 | sql    |         |             |           |       |
                             |        |         |             |           |       |
      |   |   | Country      |        |         | salis       | code='lt' |       |
      |   |   |   | name     | string |         | pavadinimas |           |       | open
      |   |   |   | driving  | string |         | vairavimas  |           |       | open
                             | enum   |         | r           | 'right'   |       | open
                             |        |         |             |           |       |
      |   |   | City         |        | name    | miestas     |           |       |
      |   |   |   | name     | string |         | pavadinimas |           |       | open
      |   |   |   | country  | ref    | Country | salis       |           |       | open
    '''


def test_copy_and_format_names(context: Context, rc, cli, tmp_path):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property    | type    | ref                        | source      | prepare                  | level  | access     | title
    datasets/gov/example        |         |                            |             |                          |        |            | Example dataset
      | data                    | sql     |                            |             |                          |        |            |
                                |         |                            |             |                          |        |            |
      |   |   | COUNTRY         |         | countryCode                | Šalis       | countryCode='lt'         |        |            | Countries
      |   |   |   | countryCode | string  |                            | Kodas       | lower()                  | 3      | private    | Country code
      |   |   |   | name        | string  |                            | Pavadinimas |                          | 3      | open       |
      |   |   |   | population  | integer |                            | Populiacija |                          | 4      | protected  |
      |   |   |   | id          | integer |                            |             | countryCode, name        | 2      | public     |
                                |         |                            |             |                          |        |            |
      |   |   | CITY            |         | name                       | Miestas     |                          |        |            | Cities
      |   |   |   | name        | string  |                            | Pavadinimas |                          | 1      |            |
      |   |   |   | countryName | string  |                            | Šalis       |                          | 4      |            |
      |   |   |   | countryCode | string  |                            | Kodas       |                          | 4      |            |
      |   |   |   | country     | ref     | COUNTRY[countryCode, name] |             | countryCode, countryName | 4      |            |
    '''))

    cli.invoke(rc, [
        'copy',
        '--format-names',
        '-o', tmp_path / 'result.csv',
        tmp_path / 'manifest.csv',
    ])

    manifest = load_manifest(rc, tmp_path / 'result.csv')
    assert manifest == '''
    d | r | b | m | property     | type    | ref                         | source      | prepare                    | level  | access    | title
    datasets/gov/example         |         |                             |             |                            |        |           | Example dataset
      | data                     | sql     |                             |             |                            |        |           |
                                 |         |                             |             |                            |        |           |
      |   |   | Country          |         | country_code                | Šalis       | country_code='lt'          |        |           | Countries
      |   |   |   | country_code | string  |                             | Kodas       | lower()                    | 3      | private   | Country code
      |   |   |   | name         | string  |                             | Pavadinimas |                            | 3      | open      |
      |   |   |   | population   | integer |                             | Populiacija |                            | 4      | protected |
      |   |   |   | id           | integer |                             |             | country_code, name         | 2      | public    |
                                 |         |                             |             |                            |        |           |
      |   |   | City             |         | name                        | Miestas     |                            |        |           | Cities
      |   |   |   | name         | string  |                             | Pavadinimas |                            | 1      |           |
      |   |   |   | country_name | string  |                             | Šalis       |                            | 4      |           |
      |   |   |   | country_code | string  |                             | Kodas       |                            | 4      |           |
      |   |   |   | country      | ref     | Country[country_code, name] |             | country_code, country_name | 4      |           |
    '''


def test_copy_and_format_names_for_ref(context: Context, rc, cli, tmp_path):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property     | type   | ref       | prepare
    datasets/gov/example         |        |           |
      | data                     | sql    |           |
                                 |        |           |
      |   |   | CONTINENT        |        |           | name_id='eu'
      |   |   |   | name_id      | string |           |
                                 |        |           |
      |   |   | COUNTRY          |        |           | continent_id.name_id='eu'
      |   |   |   | name_id      | string |           |
      |   |   |   | continent_id | ref    | CONTINENT |
                                 |        |           |
      |   |   | CITY             |        |           | country_id.continent_id.name_id='eu'
      |   |   |   | name_id      | string |           |
      |   |   |   | country_id   | ref    | COUNTRY   |
    '''))

    cli.invoke(rc, [
        'copy',
        '--format-names',
        '-o', tmp_path / 'result.csv',
        tmp_path / 'manifest.csv',
    ])

    manifest = load_manifest(rc, tmp_path / 'result.csv')
    assert manifest == '''
    d | r | b | m | property     | type   | ref       | prepare
    datasets/gov/example         |        |           |
      | data                     | sql    |           |
                                 |        |           |
      |   |   | Continent        |        |           | name_id='eu'
      |   |   |   | name_id      | string |           |
                                 |        |           |
      |   |   | Country          |        |           | continent.name_id='eu'
      |   |   |   | name_id      | string |           |
      |   |   |   | continent    | ref    | Continent |
                                 |        |           |
      |   |   | City             |        |           | country.continent.name_id='eu'
      |   |   |   | name_id      | string |           |
      |   |   |   | country      | ref    | Country   |
    '''


def test_copy_and_format_names_with_formulas(context: Context, rc, cli, tmp_path):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type   | prepare
    datasets/gov/example     |        |
      | data                 | sql    |
                             |        |
      |   |   | City         |        | NAME!=['a', 'b']\\|NAME='c'
      |   |   |   | NAME     | string |
    '''))

    cli.invoke(rc, [
        'copy',
        '--format-names',
        '-o', tmp_path / 'result.csv',
        tmp_path / 'manifest.csv',
  ])

    manifest = load_manifest(rc, tmp_path / 'result.csv')
    assert manifest == '''
    d | r | b | m | property | type   | prepare
    datasets/gov/example     |        |
      | data                 | sql    |
                             |        |
      |   |   | City         |        | name!=['a', 'b']\\|name='c'
      |   |   |   | name     | string |
    '''


def test_copy_to_stdout(context: Context, rc, cli, tmp_path):
    manifest = striptable('''
    d | r | b | m | property | type
    datasets/gov/example     |
      | data                 | sql
                             |
      |   |   | City         |
      |   |   |   | name     | string
    ''')
    create_tabular_manifest(context, tmp_path / 'manifest.csv', manifest)

    result = cli.invoke(rc, [
        'copy',
        '-c', 'd,r,b,m,p,type',
        tmp_path / 'manifest.csv',
    ])

    assert result.stdout.strip() == manifest


def test_copy_order_by_access(context: Context, rc, cli, tmp_path):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property   | type    | ref        | source | prepare  | access
    datasets/gov/example       |         |            |        |          |
      | data                   | sql     |            |        |          |
                               |         |            |        |          |
      |   |   | Continent      |         |            |        |          |
      |   |   |   | name       | string  |            |        |          | private
      |   |   |   | population | string  |            |        |          | private
                               |         |            |        |          |
      |   |   | Country        |         |            |        |          |
      |   |   |   | name       | string  |            |        |          | protected
      |   |   |   | continent  | ref     | Continent  |        |          | protected
      |   |   |   | population | string  |            |        |          | protected
      |   |   |   | driving    | string  |            |        |          | open
                               | enum    |            | l      | 'left'   | private
                               |         |            | r      | 'right'  | open
                               |         |            | c      | 'center' | public
                               |         |            |        |          |
      |   |   | City           |         |            |        |          |
      |   |   |   | name       | string  |            |        |          | open
      |   |   |   | country    | ref     | Country    |        |          | public
      |   |   |   | population | string  |            |        |          | open
    '''))

    result = cli.invoke(rc, [
        'copy',
        '-o', tmp_path / 'result.csv',
        '--order-by', 'access',
        tmp_path / 'manifest.csv',
    ])

    manifest = load_manifest(rc, tmp_path / 'result.csv')
    assert manifest == '''
    d | r | b | m | property   | type    | ref        | source | prepare  | access
    datasets/gov/example       |         |            |        |          |
      | data                   | sql     |            |        |          |
                               |         |            |        |          |
      |   |   | Country        |         |            |        |          |
      |   |   |   | driving    | string  |            |        |          | open
                               | enum    |            | r      | 'right'  | open
                               |         |            | c      | 'center' | public
                               |         |            | l      | 'left'   | private
      |   |   |   | name       | string  |            |        |          | protected
      |   |   |   | continent  | ref     | Continent  |        |          | protected
      |   |   |   | population | string  |            |        |          | protected
                               |         |            |        |          |
      |   |   | City           |         |            |        |          |
      |   |   |   | name       | string  |            |        |          | open
      |   |   |   | population | string  |            |        |          | open
      |   |   |   | country    | ref     | Country    |        |          | public
                               |         |            |        |          |
      |   |   | Continent      |         |            |        |          |
      |   |   |   | name       | string  |            |        |          | private
      |   |   |   | population | string  |            |        |          | private
    '''


def test_copy_rename_duplicates(context: Context, rc, cli, tmp_path):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property   | type
    datasets/gov/example       |
      | data                   | sql
                               |
      |   |   | City           |
      |   |   |   | name       | string
                               |
      |   |   | City           |
      |   |   |   | name       | string
                               |
      |   |   | City           |
      |   |   |   | name       | string
    '''))

    cli.invoke(rc, [
        'copy',
        '--rename-duplicates',
        tmp_path / 'manifest.csv',
        '-o', tmp_path / 'result.csv',
    ])

    manifest = load_manifest(rc, tmp_path / 'result.csv')
    assert manifest == '''
    d | r | b | m | property   | type
    datasets/gov/example       |
      | data                   | sql
                               |
      |   |   | City           |
      |   |   |   | name       | string
                               |
      |   |   | City_1         |
      |   |   |   | name       | string
                               |
      |   |   | City_2         |
      |   |   |   | name       | string
    '''


def test_enum_ref(context: Context, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', '''
    d | r | b | m | property | type    | ref     | source      | prepare | access | title
                             | enum    | sex     |             | 1       |        | Male
                             |         |         |             | 2       |        | Female
                             |         |         |             |         |        |
    datasets/gov/example     |         |         |             |         |        |
      | data                 | sql     |         |             |         |        |
                             |         |         |             |         |        |
      |   |   | Report1      |         | id      | REPORT1     |         |        |
      |   |   |   | id       | integer |         | ID          |         | open   |
      |   |   |   | sex      | integer | sex     | SEX         |         | open   |
      |   |   |   | name     | string  |         | NAME        |         | open   |
                             |         |         |             |         |        |
      |   |   | Report2      |         | id      | REPORT2     |         |        |
      |   |   |   | id       | integer |         | ID          |         | open   |
      |   |   |   | sex      | integer | sex     | SEX         |         | open   |
      |   |   |   | name     | string  |         | NAME        |         | open   |
    ''')

    cli.invoke(rc, [
        'copy', tmp_path / 'manifest.csv', '-o', tmp_path / 'result.csv',
    ])

    manifest = load_manifest(rc, tmp_path / 'result.csv')
    assert manifest == '''
    d | r | b | m | property | type    | ref     | source      | prepare | access | title
                             | enum    | sex     |             | 1       |        | Male
                             |         |         |             | 2       |        | Female
                             |         |         |             |         |        |
    datasets/gov/example     |         |         |             |         |        |
      | data                 | sql     |         |             |         |        |
                             |         |         |             |         |        |
      |   |   | Report1      |         | id      | REPORT1     |         |        |
      |   |   |   | id       | integer |         | ID          |         | open   |
      |   |   |   | sex      | integer | sex     | SEX         |         | open   |
      |   |   |   | name     | string  |         | NAME        |         | open   |
                             |         |         |             |         |        |
      |   |   | Report2      |         | id      | REPORT2     |         |        |
      |   |   |   | id       | integer |         | ID          |         | open   |
      |   |   |   | sex      | integer | sex     | SEX         |         | open   |
      |   |   |   | name     | string  |         | NAME        |         | open   |
    '''


def test_copy_status(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type   | ref     | source      | prepare | access | status 
    datasets/gov/example     |        |         |             |         |        | 
      | data                 | sql    |         |             |         |        |
                             |        |         |             |         |        |
      |   |   | Country      |        | code    | salis       |         |        |
      |   |   |   | code     | string |         | kodas       |         | public | develop
      |   |   |   | name     | string |         | pavadinimas |         | open   | completed
      |   |   |   | driving  | string |         | vairavimas  |         | open   | discont
                             | enum   |         | l           | 'left'  | open   |
                             |        |         | r           | 'right' | open   |
                             |        |         |             |         |        |
      |   |   | City         |        | name    | miestas     |         |        |
      |   |   |   | name     | string |         | pavadinimas |         | open   | deprecated
      |   |   |   | country  | ref    | Country | salis       |         | open   | withdrawn
                             |        |         |             |         |        |
      |   |   | Capital      |        | name    | miestas     |         |        |
      |   |   |   | name     | string |         | pavadinimas |         |        |
      |   |   |   | country  | ref    | Country | salis       |         |        |
    '''))

    cli.invoke(rc, [
        'copy',
        '-o', tmp_path / 'result.csv',
        tmp_path / 'manifest.csv',
    ])

    manifest = load_manifest(rc, tmp_path / 'result.csv')
    assert manifest == '''
    d | r | b | m | property | type   | ref     | source      | prepare | access | status    
    datasets/gov/example     |        |         |             |         |        | 
      | data                 | sql    |         |             |         |        |
                             |        |         |             |         |        |
      |   |   | Country      |        | code    | salis       |         |        |
      |   |   |   | code     | string |         | kodas       |         | public | develop
      |   |   |   | name     | string |         | pavadinimas |         | open   | completed
      |   |   |   | driving  | string |         | vairavimas  |         | open   | discont
                             | enum   |         | l           | 'left'  | open   |
                             |        |         | r           | 'right' | open   |
                             |        |         |             |         |        |
      |   |   | City         |        | name    | miestas     |         |        |
      |   |   |   | name     | string |         | pavadinimas |         | open   | deprecated
      |   |   |   | country  | ref    | Country | salis       |         | open   | withdrawn
                             |        |         |             |         |        |
      |   |   | Capital      |        | name    | miestas     |         |        |
      |   |   |   | name     | string |         | pavadinimas |         |        |
      |   |   |   | country  | ref    | Country | salis       |         |        |
    '''
