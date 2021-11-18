from pathlib import Path

from spinta.core.config import RawConfig
from spinta.testing.cli import SpintaCliRunner
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.tabular import create_tabular_manifest
from spinta.testing.manifest import load_manifest


def test_copy(rc, cli: SpintaCliRunner, tmpdir):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
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
        '-o', tmpdir / 'result.csv',
        tmpdir / 'manifest.csv',
    ])

    manifest = load_manifest(rc, tmpdir / 'result.csv')
    assert manifest == '''
    d | r | b | m | property | type   | ref     | source | prepare | access
    datasets/gov/example     |        |         |        |         |
      | data                 | sql    |         |        |         |
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


def test_copy_enum_0(rc, cli: SpintaCliRunner, tmpdir):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
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
        '-o', tmpdir / 'result.csv',
        tmpdir / 'manifest.csv',
  ])

    manifest = load_manifest(rc, tmpdir / 'result.csv')
    assert manifest == '''
    d | r | b | m | property | type    | ref     | source | prepare | access
    datasets/gov/example     |         |         |        |         |
      | data                 | sql     |         |        |         |
                             |         |         |        |         |
      |   |   | Country      |         |         |        |         |
      |   |   |   | name     | string  |         |        |         | open
      |   |   |   | driving  | integer |         |        |         | open
                             | enum    |         |        | 0       | open
                             |         |         |        | 1       | open
    '''


def test_copy_global_enum(rc, cli: SpintaCliRunner, tmpdir):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
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
        '-o', tmpdir / 'result.csv',
        tmpdir / 'manifest.csv',
    ])

    manifest = load_manifest(rc, tmpdir / 'result.csv')
    assert manifest == '''
    d | r | b | m | property | type    | ref       | source | prepare | access
    datasets/gov/example     |         |           |        |         |
                             | enum    | direction |        | 0       |
                             |         |           |        | 1       |
      | data                 | sql     |           |        |         |
                             |         |           |        |         |
      |   |   | Country      |         |           |        |         |
      |   |   |   | name     | string  |           |        |         | open
      |   |   |   | driving  | integer | direction |        |         | open
    '''


def test_copy_with_filters_and_externals(rc, cli, tmpdir):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    d | r | b | m | property | type   | ref     | source      | prepare   | access
    datasets/gov/example     |        |         |             |           |
      | data                 | sql    |         |             |           |
                             |        |         |             |           |
      |   |   | country      |        | code    | salis       | code='lt' |
      |   |   |   | code     | string |         | kodas       |           | private
      |   |   |   | name     | string |         | pavadinimas |           | open
      |   |   |   | driving  | string |         | vairavimas  |           | open
      |   |   |   |          | enum   |         | l           | 'left'    | private
      |   |   |   |          |        |         | r           | 'right'   | open
                             |        |         |             |           |
      |   |   | city         |        | name    | miestas     |           |
      |   |   |   | name     | string |         | pavadinimas |           | open
      |   |   |   | country  | ref    | country | salis       |           | open
                             |        |         |             |           |
      |   |   | capital      |        | name    | miestas     |           |
      |   |   |   | name     | string |         | pavadinimas |           |
      |   |   |   | country  | ref    | country | salis       |           |
    '''))

    cli.invoke(rc, [
        'copy',
        '--access', 'open',
        '-o', tmpdir / 'result.csv',
        tmpdir / 'manifest.csv',
    ])

    manifest = load_manifest(rc, tmpdir / 'result.csv')
    assert manifest == '''
    d | r | b | m | property | type   | ref     | source      | prepare   | level | access
    datasets/gov/example     |        |         |             |           |       |
      | data                 | sql    |         |             |           |       |
                             |        |         |             |           |       |
      |   |   | country      |        |         | salis       | code='lt' |       |
      |   |   |   | name     | string |         | pavadinimas |           |       | open
      |   |   |   | driving  | string |         | vairavimas  |           |       | open
                             | enum   |         | r           | 'right'   |       | open
                             |        |         |             |           |       |
      |   |   | city         |        | name    | miestas     |           |       |
      |   |   |   | name     | string |         | pavadinimas |           |       | open
      |   |   |   | country  | ref    | country | salis       |           |       | open
    '''


def test_copy_and_format_names(rc, cli, tmpdir):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
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
        '-o', tmpdir / 'result.csv',
        tmpdir / 'manifest.csv',
    ])

    manifest = load_manifest(rc, tmpdir / 'result.csv')
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


def test_copy_and_format_names_for_ref(rc, cli, tmpdir):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
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
        '-o', tmpdir / 'result.csv',
        tmpdir / 'manifest.csv',
    ])

    manifest = load_manifest(rc, tmpdir / 'result.csv')
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


def test_copy_and_format_names_with_formulas(rc, cli, tmpdir):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
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
        '-o', tmpdir / 'result.csv',
        tmpdir / 'manifest.csv',
  ])

    manifest = load_manifest(rc, tmpdir / 'result.csv')
    assert manifest == '''
    d | r | b | m | property | type   | prepare
    datasets/gov/example     |        |
      | data                 | sql    |
                             |        |
      |   |   | City         |        | name!=['a', 'b']\\|name='c'
      |   |   |   | name     | string |
    '''


def test_copy_to_stdout(rc, cli, tmpdir):
    manifest = striptable('''
    d | r | b | m | property | type
    datasets/gov/example     |
      | data                 | sql
                             |
      |   |   | City         |
      |   |   |   | name     | string
    ''')
    create_tabular_manifest(tmpdir / 'manifest.csv', manifest)

    result = cli.invoke(rc, [
        'copy',
        '-c', 'd,r,b,m,p,type',
        tmpdir / 'manifest.csv',
    ])

    assert result.stdout.strip() == manifest


def test_copy_order_by_access(rc, cli, tmpdir):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
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
        '-o', tmpdir / 'result.csv',
        '--order-by', 'access',
        tmpdir / 'manifest.csv',
    ])

    manifest = load_manifest(rc, tmpdir / 'result.csv')
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


def test_copy_rename_duplicates(rc, cli, tmpdir):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
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
        tmpdir / 'manifest.csv',
        '-o', tmpdir / 'result.csv',
    ])

    manifest = load_manifest(rc, tmpdir / 'result.csv')
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


def test_enum_ref(rc: RawConfig, cli: SpintaCliRunner, tmpdir: Path):
    create_tabular_manifest(tmpdir / 'manifest.csv', '''
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
        'copy', tmpdir / 'manifest.csv', '-o', tmpdir / 'result.csv',
    ])

    manifest = load_manifest(rc, tmpdir / 'result.csv')
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
