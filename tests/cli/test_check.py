from spinta.components import Context
from spinta.testing.cli import SpintaCliRunner
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.tabular import create_tabular_manifest


def test_check_nested_Backref(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property                   | type    | ref       | source            | prepare | access | level
    datasets/gov/example                       |         |           |                   |         |        |
      | data                                   | sql     |           |                   |         |        |
                                               |         |           |                   |         |        |
      |   |   | Continent                      |         | code      | salis             |         |        | 
      |   |   |   | code                       | string  |           | kodas             |         | public | 1
      |   |   |   | name                       | string  |           | pavadinimas       |         | open   | 2
      |   |   |   | political                  | ref     | Political | countries         |         | open   | 3      
      |   |   |   | political.countries        | array   |           | countries         |         | open   | 3 
      |   |   |   | political.countries[]      | backref | Country   | .                 |         | open   | 3
      |   |   | Political                      |         | code      | salis             |         |        | 
      |   |   |   | code                       | string  |           | kodas             |         | public | 1
      |   |   |   | name                       | string  |           | pavadinimas       |         | open   | 2 
      |   |   |   | countries[]                | backref | Country   | .                 |         | open   | 3                
      |   |   | Country                        |         | code      | salis             |         |        |    
      |   |   |   | continent                  | ref     | Continent | ./.               |         | public | 1      
      |   |   |   | political                  | ref     | Political | ././.             |         | public | 1
      |   |   |   | code                       | string  |           | kodas             |         | public | 1
      |   |   |   | name                       | string  |           | pavadinimas       |         | open   | 2
    '''))

    cli.invoke(rc, [
        'check',
        tmp_path / 'manifest.csv'
    ])
