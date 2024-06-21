from spinta.components import Context
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.tabular import create_tabular_manifest


def test_copy_mmd(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
        d | r | b | m | property | type    | ref       | source      | prepare | access
        datasets/gov/example     |         |           |             |         |
          | data                 | sql     |           |             |         |
                                 |         |           |             |         |
          |   |   | Country      |         |           | salis       |         |
          |   |   |   | name     | string  |           | pavadinimas |         | open
          |   |   |   | id       | integer required|           | id          |         | open
        '''))

    cli.invoke(rc, [
        'copy',
        '--no-source',
        '--access', 'open',
        '-o', tmp_path / 'result.mmd',
              tmp_path / 'manifest.csv',
    ])

    with open(tmp_path / 'result.mmd', "r") as file:
        contents = file.read()
        assert contents == """---
datasets/gov/example
---
classDiagram
class Country {
+ name : string [0..1]
+ id : integer [1..1]
}
"""


def test_copy_mmd_array(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
        d | r | b | m | property | type    | ref       | source      | prepare | access
        datasets/gov/example     |         |           |             |         |
          | data                 | sql     |           |             |         |
                                 |         |           |             |         |
          |   |   | Country      |         |           | salis       |         |
          |   |   |   | name[]   | string  |           | pavadinimas |         | open
          |   |   |   | id       | integer required|           | id          |         | open
        '''))

    cli.invoke(rc, [
        'copy',
        '--no-source',
        '--access', 'open',
        '-o', tmp_path / 'result.mmd',
              tmp_path / 'manifest.csv',
    ])

    with open(tmp_path / 'result.mmd', "r") as file:
        contents = file.read()
        assert contents == """---
datasets/gov/example
---
classDiagram
class Country {
+ name : partial_array [0..*]
+ id : integer [1..1]
}
"""


def test_copy_mmd_enum(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
        d | r | b | m | property | type    | ref       | source      | prepare | access
        datasets/gov/example     |         |           |             |         |
          | data                 | sql     |           |             |         |
                                 |         |           |             |         |
          |   |   | Country      |         |           | salis       |         |
          |   |   |   | name     | string  |           | pavadinimas |         | open
          |   |   |   | id       | integer required|           | id          |         | open
          |   |   |   | continent| string  |           |             |         | open
          |   |   |   |          | enum    |           |             | "Africa"|
          |   |   |   |          |         |           |             | "Asia"  |
          |   |   |   |          |         |           |             | "Europe"|
        '''))

    cli.invoke(rc, [
        'copy',
        '--no-source',
        '--access', 'open',
        '-o', tmp_path / 'result.mmd',
              tmp_path / 'manifest.csv',
    ])

    with open(tmp_path / 'result.mmd', "r") as file:
        contents = file.read()
        assert contents == """---
datasets/gov/example
---
classDiagram
class CountryContinent {
<<enumeration>>
Africa
Asia
Europe
}
class Country {
+ name : string [0..1]
+ id : integer [1..1]
}
Country ..> "[1..1]" CountryContinent : continent
"""


def test_copy_mmd_ref(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
        d | r | b | m | property | type    | ref       | source      | prepare | access
        datasets/gov/example     |         |           |             |         |
          | data                 | sql     |           |             |         |
                                 |         |           |             |         |
          |   |   | Country      |         |           | salis       |         |
          |   |   |   | name     | string  |           | pavadinimas |         | open
          |   |   |   | id       | integer required|   | id          |         | open
          |   |   | City         |         |           | salis       |         |
          |   |   |   | name     | string  |           | pavadinimas |         | open
          |   |   |   | country  | ref     | Country   |             |         | open
          |   |   |   | id       | integer required|   | id          |         | open
        '''))

    cli.invoke(rc, [
        'copy',
        '--no-source',
        '--access', 'open',
        '-o', tmp_path / 'result.mmd',
              tmp_path / 'manifest.csv',
    ])

    with open(tmp_path / 'result.mmd', "r") as file:
        contents = file.read()
        assert contents == """---
datasets/gov/example
---
classDiagram
class Country {
+ name : string [0..1]
+ id : integer [1..1]
}
class City {
+ name : string [0..1]
+ id : integer [1..1]
}
City --> "[1..1]" Country : country
"""


def test_copy_mmd_backref(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
        d | r | b | m | property | type    | ref       | source      | prepare | access
        datasets/gov/example     |         |           |             |         |
          | data                 | sql     |           |             |         |
                                 |         |           |             |         |
          |   |   | Country      |         |           | salis       |         |
          |   |   |   | name     | string  |           | pavadinimas |         | open
          |   |   |   | id       | integer required|   | id          |         | open
          |   |   |   | cities[] | backref | City      |             |         | open
          |   |   | City         |         |           | salis       |         |
          |   |   |   | name     | string  |           | pavadinimas |         | open
          |   |   |   | country  | ref     | Country   |             |         | open
          |   |   |   | id       | integer required|   | id          |         | open
        '''))

    cli.invoke(rc, [
        'copy',
        '--no-source',
        '--access', 'open',
        '-o', tmp_path / 'result.mmd',
              tmp_path / 'manifest.csv',
    ])

    with open(tmp_path / 'result.mmd', "r") as file:
        contents = file.read()
        assert contents == """---
datasets/gov/example
---
classDiagram
class Country {
+ name : string [0..1]
+ id : integer [1..1]
}
class City {
+ name : string [0..1]
+ id : integer [1..1]
}
City --> "[1..1]" Country : country
"""
