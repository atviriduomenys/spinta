from pathlib import Path

from spinta.components import Context
from spinta.exceptions import InvalidValue, InvalidManifestFile, InvalidName
from spinta.testing.cli import SpintaCliRunner
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.tabular import create_tabular_manifest


def test_check_status(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
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
    """),
    )

    cli.invoke(
        rc,
        [
            "check",
            tmp_path / "manifest.csv",
        ],
    )


def test_check_visibility(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
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
    """),
    )

    cli.invoke(
        rc,
        [
            "check",
            tmp_path / "manifest.csv",
        ],
    )


def test_check_eli(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
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
    """),
    )

    cli.invoke(
        rc,
        [
            "check",
            tmp_path / "manifest.csv",
        ],
    )


def test_check_count(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
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
    """),
    )

    cli.invoke(
        rc,
        [
            "check",
            tmp_path / "manifest.csv",
        ],
    )


def test_check_origin(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
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
    """),
    )

    cli.invoke(
        rc,
        [
            "check",
            tmp_path / "manifest.csv",
        ],
    )


def test_check_level(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
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
      |   |   | Country      |        | code    | salis       |         |        | 9
      |   |   |   | code     | string |         | kodas       |         | public |
      |   |   |   | name     | string |         | pavadinimas |         | open   |
    """),
    )

    result = cli.invoke(
        rc,
        [
            "check",
            tmp_path / "manifest.csv",
        ],
        fail=False,
    )
    assert result.exit_code == 0
    assert "Invalid value." in result.stdout
    assert "component: spinta.dimensions.enum.components.EnumItem" in result.stdout
    assert "component: spinta.components.Model" in result.stdout
    assert "model: datasets/gov/example2/Country" in result.stdout
    assert "param: level" in result.stdout
    assert "given: 9" in result.stdout
    assert "Total errors: 2" in result.stdout
    assert "manifest.csv" in result.stdout


def test_check_level_multiple_manifests(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
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
    """),
    )

    create_tabular_manifest(
        context,
        tmp_path / "manifest2.csv",
        striptable("""
        d | r | b | m | property | type   | ref     | source      | prepare | access | level
        datasets/gov/example3    |        |         |             |         |        |
          | data                 | sql    |         |             |         |        |
                                 |        |         |             |         |        |
          |   |   | Salis        |        | code    | salis       |         |        | 
          |   |   |   | code     | string |         | kodas       |         | public | 1
          |   |   |   | name     | string |         | pavadinimas |         | open   | 2
          |   |   |   | driving  | string |         | vairavimas  |         | open   | 3
                                 | enum   |         | l           | 'left'  | open   | 9
                                 |        |         | r           | 'right' | open   |
        datasets/gov/example4    |        |         |             |         |        |
          | data                 | sql    |         |             |         |        |        
                                 |        |         |             |         |        |              
          |   |   | Salis        |        | code    | salis       |         |        | 9
          |   |   |   | code     | string |         | kodas       |         | public |
          |   |   |   | name     | string |         | pavadinimas |         | open   |
        """),
    )

    result = cli.invoke(
        rc,
        [
            "check",
            tmp_path / "manifest.csv",
            tmp_path / "manifest2.csv",
        ],
        fail=False,
    )
    assert result.exit_code == 0
    assert "Invalid value." in result.stdout
    assert "component: spinta.dimensions.enum.components.EnumItem" in result.stdout
    assert "component: spinta.components.Model" in result.stdout
    assert "model: datasets/gov/example4/Salis" in result.stdout
    assert "param: level" in result.stdout
    assert "given: 9" in result.stdout
    assert "Total errors: 3" in result.stdout
    assert "manifest.csv" in result.stdout
    assert "manifest2.csv" in result.stdout


def test_check_access(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
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
    """),
    )

    result = cli.invoke(rc, ["check", tmp_path / "manifest.csv"], fail=False)

    assert result.exit_code != 0
    assert result.exc_info[0] is InvalidValue
    assert result.exception.context.get("given") == "test"


def test_check_existing_generated_manifest(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
 d | r | b | m | property       | type   | ref  | level
 datasets/gov/test/example2     |        |      |
 datasets/gov/test | | | |      | ns     |      |
 datasets/gov/test/example1     |        |      |
                                |        |      |
   |   |   | City               |        | code | 4
   |   |   |   | name           | string |      | 4
   |   |   |   | code           | string |      | 4
"""),
    )

    cli.invoke(
        rc,
        [
            "check",
            tmp_path / "manifest.csv",
        ],
    )


def test_check_existing_declared_manifest(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
 d | r | b | m | property       | type   | ref  | level
 datasets/gov/test/example2     |        |      |
 datasets/gov/test | | | |      | ns     |      |
 datasets/gov/test | | | |      | ns     |      |
 datasets/gov/test/example1     |        |      |
                                |        |      |
   |   |   | City               |        | code | 4
   |   |   |   | name           | string |      | 4
   |   |   |   | code           | string |      | 4
"""),
    )

    result = cli.invoke(
        rc,
        [
            "check",
            tmp_path / "manifest.csv",
        ],
        fail=False,
    )
    assert result.exit_code != 0
    assert result.exc_info[0] is InvalidManifestFile


def test_check_nested_Backref(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
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
    """),
    )

    cli.invoke(rc, ["check", tmp_path / "manifest.csv"])


def test_check_dot_in_ref(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
 d | r | b | m | property   | type    | ref                     | source
 test_dataset               |         |                         |
   | resource1              | xml     |                         |
                            |         |                         |
   |   |   | Area           |         | id                      |
   |   |   |   | name       | string  |                         | name/text()
   |   |   |   | id         | integer |                         | name/text()
   |   |   |   | area       | string  |                         | name/text()
 test_dataset2              |         |                         |
   | resource2              | xml     |                         |
                            |         |                         |
   |   | /test_dataset/Area |         | area                    |
   |   |   | Country        |         | area.id                 | Country    
   |   |   |   | name       | string  |                         | name/text()
   |   |   |   | code       | string  |                         | code/text()
   |   |   |   | area       | ref     | /test_dataset/Area[id]  | area/text()
   |   |   |   | area.id    | integer |                         | area/text()
    """),
    )

    cli.invoke(
        rc,
        [
            "check",
            tmp_path / "manifest.csv",
        ],
    )


def test_check_nested_ref_and_prepare(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
d | r | b | m | property        | type    | ref                              | prepare         | source
test_dataset                    |         |                                  |                 |
  | resource1                   | xml     |                                  |                 |
  |   |   | City                |         |                                  |                 | City
  |   |   |   | name            | string  |                                  |                 | name/text()
  |   |   |   | code            | string  |                                  |                 | code/text()
  |   |   |   | mayor           | ref     | Mayor                            |                 | mayor
  |   |   |   | mayor.name      | string  |                                  |                 | name/text()
  |   |   | Mayor               |         |                                  |                 | Mayor
  |   |   |   | name            | string  |                                  |                 | name/text()
  |   |   |   | city            | ref     | City[mayor.name, code]           | name, city.code | city/text()
  |   |   |   | city.code       | string  |                                  |                 | city/text()
    """),
    )

    cli.invoke(
        rc,
        [
            "check",
            tmp_path / "manifest.csv",
        ],
    )


def test_check_wsdl_unreachable(context: Context, rc, cli: SpintaCliRunner, tmp_path: Path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
d | r | b | m | property        | type    | prepare            | source
test_dataset                    |         |                    |
  | wsdl_resource               | wsdl    |                    | foo.bar
  | soap_resource               | soap    | wsdl(wsdl_resource)| service.port.type.operation
    """),
    )

    cli.invoke(
        rc,
        [
            "check",
            tmp_path / "manifest.csv",
        ],
    )


def test_check_prop_underscore(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property  | type   | ref     | source      | prepare | access | status
    datasets/gov/example      |        |         |             |         |        |
      | data                  | sql    |         |             |         |        |
                              |        |         |             |         |        |
      |   |   | Country       |        | code    | salis       |         |        | develop
      |   |   |   | code      | string |         | kodas       |         | public | develop
      |   |   |   | name      | string |         | pavadinimas |         | open   | completed
      |   |   |   | _label    | string |         | zyme        |         | open   | develop
      |   |   |   | _created  | string |         | sukurta     |         | open   | discont
                              |        |         |             |         |        |
      |   |   | City          |        | name    | miestas     |         |        | completed
      |   |   |   | name      | string |         | pavadinimas |         | open   | deprecated
      |   |   |   | country   | ref    | Country | salis       |         | open   | withdrawn
      |   |   |   | _revision | string |         | versija     |         | open   | discont
      |   |   |   | _updated  | string |         | papildyta   |         | open   | discont
      |   |   |   | _id       | uuid   |         | id          |         | open   | develop
    """),
    )

    result = cli.invoke(
        rc,
        [
            "check",
            "--check-names",
            tmp_path / "manifest.csv",
        ],
    )


def test_check_prop_underscore_error(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type   | ref     | source      | prepare | access | status
    datasets/gov/example     |        |         |             |         |        |
      | data                 | sql    |         |             |         |        |
                             |        |         |             |         |        |
      |   |   | Country      |        | code    | salis       |         |        | develop
      |   |   |   | code     | string |         | kodas       |         | public | develop
      |   |   |   | name     | string |         | pavadinimas |         | open   | completed
      |   |   |   | _created | string |         | sukurta     |         | open   | discont
      |   |   |   | _op      | string |         | veiksmas    |         | open   | develop
    """),
    )

    result = cli.invoke(
        rc,
        [
            "check",
            "--check-names",
            tmp_path / "manifest.csv",
        ],
        fail=False,
    )

    assert result.exit_code != 0
    assert result.exc_info[0] is InvalidName
    assert "_op" in str(result.exception)
