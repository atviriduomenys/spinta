from spinta.testing.cli import SpintaCliRunner
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.tabular import create_tabular_manifest
from spinta.testing.tabular import load_tabular_manifest
from spinta.manifests.tabular.helpers import render_tabular_manifest


def test_copy(rc, cli: SpintaCliRunner, tmpdir):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    d | r | b | m | property | type   | ref     | source      | access
    datasets/gov/example     |        |         |             |
      | data                 | sql    |         |             |
                             |        |         |             |
      |   |   | country      |        | code    | salis       |
      |   |   |   | code     | string |         | kodas       | public
      |   |   |   | name     | string |         | pavadinimas | open
      |   |                  |        |         |             |
      |   |   | city         |        | name    | miestas     |
      |   |   |   | name     | string |         | pavadinimas | open
      |   |   |   | country  | ref    | country | salis       | open
                             |        |         |             |
      |   |   | capital      |        | name    | miestas     |
      |   |   |   | name     | string |         | pavadinimas |
      |   |   |   | country  | ref    | country | salis       |
    '''))

    cli.invoke(rc, [
        'copy', '--no-source', '--access', 'open',
        tmpdir / 'manifest.csv',
        tmpdir / 'result.csv',
    ])

    manifest = load_tabular_manifest(rc, tmpdir / 'result.csv')
    cols = [
        'dataset', 'resource', 'base', 'model', 'property',
        'type', 'ref', 'source', 'level', 'access',
    ]
    assert render_tabular_manifest(manifest, cols) == striptable('''
    d | r | b | m | property | type   | ref     | source | level | access
    datasets/gov/example     |        |         |        |       | protected
      | data                 | sql    |         |        |       | protected
                             |        |         |        |       |
      |   |   | country      |        |         |        |       | open
      |   |   |   | name     | string |         |        |       | open
                             |        |         |        |       |
      |   |   | city         |        |         |        |       | open
      |   |   |   | name     | string |         |        |       | open
      |   |   |   | country  | ref    | country |        |       | open
    ''')


def test_copy_with_filters_and_externals(rc, cli, tmpdir):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    d | r | b | m | property | type   | ref     | source      | prepare   | access
    datasets/gov/example     |        |         |             |           |
      | data                 | sql    |         |             |           |
                             |        |         |             |           |
      |   |   | country      |        | code    | salis       | code='lt' |
      |   |   |   | code     | string |         | kodas       |           | private
      |   |   |   | name     | string |         | pavadinimas |           | open
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
        'copy', '--access', 'open',
        tmpdir / 'manifest.csv',
        tmpdir / 'result.csv',
    ])

    manifest = load_tabular_manifest(rc, tmpdir / 'result.csv')
    cols = [
        'dataset', 'resource', 'base', 'model', 'property',
        'type', 'ref', 'source', 'prepare', 'level', 'access',
    ]
    assert render_tabular_manifest(manifest, cols) == striptable('''
    d | r | b | m | property | type   | ref     | source      | prepare   | level | access
    datasets/gov/example     |        |         |             |           |       | protected
      | data                 | sql    |         |             |           |       | protected
                             |        |         |             |           |       |
      |   |   | country      |        |         | salis       | code='lt' |       | open
      |   |   |   | name     | string |         | pavadinimas |           |       | open
                             |        |         |             |           |       |
      |   |   | city         |        | name    | miestas     |           |       | open
      |   |   |   | name     | string |         | pavadinimas |           |       | open
      |   |   |   | country  | ref    | country | salis       |           |       | open
    ''')


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
        'copy', '--format-names',
        tmpdir / 'manifest.csv',
        tmpdir / 'result.csv',
        ])

    manifest = load_tabular_manifest(rc, tmpdir / 'result.csv')
    assert manifest == '''
    d | r | b | m | property     | type    | ref                         | source      | prepare                    | level  | access    | title
    datasets/gov/example         |         |                             |             |                            |        | protected | Example dataset
      | data                     | sql     |                             |             |                            |        | protected |
                                 |         |                             |             |                            |        |           |
      |   |   | Country          |         | country_code                | Šalis       | country_code='lt'          |        | open      | Countries
      |   |   |   | country_code | string  |                             | Kodas       | lower()                    | 3      | private   | Country code
      |   |   |   | name         | string  |                             | Pavadinimas |                            | 3      | open      |
      |   |   |   | population   | integer |                             | Populiacija |                            | 4      | protected |
      |   |   |   | id           | integer |                             |             | country_code, name         | 2      | public    |
                                 |         |                             |             |                            |        |           |
      |   |   | City             |         | name                        | Miestas     |                            |        | protected | Cities
      |   |   |   | name         | string  |                             | Pavadinimas |                            | 1      | protected |
      |   |   |   | country_name | string  |                             | Šalis       |                            | 4      | protected |
      |   |   |   | country_code | string  |                             | Kodas       |                            | 4      | protected |
      |   |   |   | country      | ref     | Country[country_code, name] |             | country_code, country_name | 4      | protected |
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
        'copy', '--format-names',
        tmpdir / 'manifest.csv',
        tmpdir / 'result.csv',
        ])

    manifest = load_tabular_manifest(rc, tmpdir / 'result.csv')
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
