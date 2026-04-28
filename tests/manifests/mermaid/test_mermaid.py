from spinta.components import Context
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.tabular import create_tabular_manifest
from spinta.cli.manifest import _read_and_return_manifest
from spinta.manifests.mermaid.helpers import write_mermaid_manifest
from spinta.manifests.mermaid.helpers import MERMAID_CONFIG, ENTITY_STYLES, CONCEPT_STYLES


def test_copy_mmd(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
        d | r | b | m | property | type            | ref       | source       | visibility
        datasets/gov/example     |                 |           |              | 
          | data                 | sql             |           |              |
                                 |                 |           |              |
          |   |   | Country      |                 |           | salis        | 
          |   |   |   | name     | string          |           | pavadinimas  | public
          |   |   |   | id       | integer required|           | id           | public
        """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "--no-source",
            "--access",
            "open",
            "-o",
            tmp_path / "result.mmd",
            tmp_path / "manifest.csv",
        ],
    )

    with open(tmp_path / "result.mmd", "r") as file:
        contents = file.read()
        assert (
            contents
            == f"""{MERMAID_CONFIG}
classDiagram
namespace `datasets/gov/example` {{
class `datasets/gov/example/Country`["Country"]:::Entity {{
«mandatory»
+ id : integer [1..1]
«optional»
+ name : string [0..1]
}}
}}
classDef Entity {ENTITY_STYLES};
"""
        )


def test_copy_mmd_visibility(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
        d | r | b | m | property   | type            | ref       | source      | prepare | visibility
        datasets/gov/example       |                 |           |             |         |
          | data                   | sql             |           |             |         |
                                   |                 |           |             |         |
          |   |   | Country        |                 |           | salis       |         |
          |   |   |   | name       | string          |           | pavadinimas |         | public
          |   |   |   | id         | integer required|           | id          |         | package
          |   |   |   | continent  | string          |           | pavadinimas |         | protected
          |   |   |   | population | integer         |           | id          |         | private
        """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "--no-source",
            "--access",
            "open",
            "-o",
            tmp_path / "result.mmd",
            tmp_path / "manifest.csv",
        ],
    )

    with open(tmp_path / "result.mmd", "r") as file:
        contents = file.read()
        assert (
            contents
            == f"""{MERMAID_CONFIG}
classDiagram
namespace `datasets/gov/example` {{
class `datasets/gov/example/Country`["Country"]:::Entity {{
«mandatory»
~ id : integer [1..1]
«optional»
+ name : string [0..1]
# continent : string [0..1]
- population : integer [0..1]
}}
}}
classDef Entity {ENTITY_STYLES};
"""
        )


def test_copy_mmd_array(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
        d | r | b | m | property | type            | ref | source      | visibility
        datasets/gov/example     |                 |     |             |
          | data                 | sql             |     |             |
                                 |                 |     |             |
          |   |   | Country      |                 |     | salis       |
          |   |   |   | name[]   | string          |     | pavadinimas | public
          |   |   |   | id       | integer required|     | id          | public
        """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "--no-source",
            "--access",
            "open",
            "-o",
            tmp_path / "result.mmd",
            tmp_path / "manifest.csv",
        ],
    )

    with open(tmp_path / "result.mmd", "r") as file:
        contents = file.read()
        assert (
            contents
            == f"""{MERMAID_CONFIG}
classDiagram
namespace `datasets/gov/example` {{
class `datasets/gov/example/Country`["Country"]:::Entity {{
«mandatory»
+ id : integer [1..1]
«optional»
+ name : string [0..*]
}}
}}
classDef Entity {ENTITY_STYLES};
"""
        )


def test_copy_mmd_enum(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
        d | r | b | m | property  | type             | ref | source      | prepare  | visibility
        datasets/gov/example      |                  |     |             |          |
          | data                  | sql              |     |             |          |
                                  |                  |     |             |          |
          |   |   | Country       |                  |     | salis       |          |
          |   |   |   | name      | string           |     | pavadinimas |          | public
          |   |   |   | id        | integer required |     | id          |          | public
          |   |   |   | continent | string           |     |             |          |
          |   |   |   |           | enum             |     |             | "Africa" |
          |   |   |   |           |                  |     |             | "Asia"   |
          |   |   |   |           |                  |     |             | "Europe" |
        """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "--no-source",
            "--access",
            "open",
            "-o",
            tmp_path / "result.mmd",
            tmp_path / "manifest.csv",
        ],
    )

    with open(tmp_path / "result.mmd", "r") as file:
        contents = file.read()
        assert (
            contents
            == f"""{MERMAID_CONFIG}
classDiagram
namespace `datasets/gov/example` {{
class `datasets/gov/example/CountryContinent`["Continent"]:::Concept {{
<<enumeration>>
«optional»
Africa
Asia
Europe
}}
class `datasets/gov/example/Country`["Country"]:::Entity {{
«mandatory»
+ id : integer [1..1]
«optional»
+ name : string [0..1]
}}
}}
`datasets/gov/example/Country` ..> "[1..1]" `datasets/gov/example/CountryContinent` : continent<br/>«mandatory»
classDef Concept {CONCEPT_STYLES};
classDef Entity {ENTITY_STYLES};
"""
        )


def test_copy_mmd_ref(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
        d | r | b | m | property | type             | ref       | source      
        datasets/gov/example     |                  |           |             
          | data                 | sql              |           |             
                                 |                  |           |             
          |   |   | Country      |                  |           | salis       
          |   |   |   | name     | string           |           | pavadinimas 
          |   |   |   | id       | integer required |           | id          
          |   |   | City         |                  |           | salis       
          |   |   |   | name     | string           |           | pavadinimas 
          |   |   |   | country  | ref              | Country   |             
          |   |   |   | id       | integer required |           | id          
        """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "--no-source",
            "--access",
            "open",
            "-o",
            tmp_path / "result.mmd",
            tmp_path / "manifest.csv",
        ],
    )

    with open(tmp_path / "result.mmd", "r") as file:
        contents = file.read()
        assert (
            contents
            == f"""{MERMAID_CONFIG}
classDiagram
namespace `datasets/gov/example` {{
class `datasets/gov/example/Country`["Country"]:::Entity {{
«mandatory»
id : integer [1..1]
«optional»
name : string [0..1]
}}
class `datasets/gov/example/City`["City"]:::Entity {{
«mandatory»
id : integer [1..1]
«optional»
name : string [0..1]
}}
}}
`datasets/gov/example/City` --> "[0..1]" `datasets/gov/example/Country` : country<br/>«optional»
classDef Entity {ENTITY_STYLES};
"""
        )


def test_copy_mmd_ref_required(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
        d | r | b | m | property | type             | ref     | source      
        datasets/gov/example     |                  |         |             
          | data                 | sql              |         |             
                                 |                  |         |             
          |   |   | Country      |                  |         | salis       
          |   |   |   | name     | string           |         | pavadinimas 
          |   |   |   | id       | integer required |         | id          
          |   |   | City         |                  |         | salis       
          |   |   |   | name     | string           |         | pavadinimas 
          |   |   |   | country  | ref required     | Country |             
          |   |   |   | id       | integer required |         | id          
        """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "--no-source",
            "--access",
            "open",
            "-o",
            tmp_path / "result.mmd",
            tmp_path / "manifest.csv",
        ],
    )

    with open(tmp_path / "result.mmd", "r") as file:
        contents = file.read()
        assert (
            contents
            == f"""{MERMAID_CONFIG}
classDiagram
namespace `datasets/gov/example` {{
class `datasets/gov/example/Country`["Country"]:::Entity {{
«mandatory»
id : integer [1..1]
«optional»
name : string [0..1]
}}
class `datasets/gov/example/City`["City"]:::Entity {{
«mandatory»
id : integer [1..1]
«optional»
name : string [0..1]
}}
}}
`datasets/gov/example/City` --> "[1..1]" `datasets/gov/example/Country` : country<br/>«mandatory»
classDef Entity {ENTITY_STYLES};
"""
        )


def test_copy_mmd_backref(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
        d | r | b | m | property | type             | ref     | source      
        datasets/gov/example     |                  |         |             
          | data                 | sql              |         |             
                                 |                  |         |             
          |   |   | Country      |                  |         | salis       
          |   |   |   | name     | string           |         | pavadinimas 
          |   |   |   | id       | integer required |         | id          
          |   |   |   | cities[] | backref          | City    |             
          |   |   | City         |                  |         | salis       
          |   |   |   | name     | string           |         | pavadinimas 
          |   |   |   | country  | ref              | Country |             
          |   |   |   | id       | integer required |         | id          
        """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "--no-source",
            "--access",
            "open",
            "-o",
            tmp_path / "result.mmd",
            tmp_path / "manifest.csv",
        ],
    )

    with open(tmp_path / "result.mmd", "r") as file:
        contents = file.read()
        assert (
            contents
            == f"""{MERMAID_CONFIG}
classDiagram
namespace `datasets/gov/example` {{
class `datasets/gov/example/Country`["Country"]:::Entity {{
«mandatory»
id : integer [1..1]
«optional»
name : string [0..1]
}}
class `datasets/gov/example/City`["City"]:::Entity {{
«mandatory»
id : integer [1..1]
«optional»
name : string [0..1]
}}
}}
`datasets/gov/example/Country` --> "[0..*]" `datasets/gov/example/City` : cities<br/>«optional»
`datasets/gov/example/City` --> "[0..1]" `datasets/gov/example/Country` : country<br/>«optional»
classDef Entity {ENTITY_STYLES};
"""
        )


def test_copy_mmd_backref_not_array(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
        d | r | b | m | property | type             | ref       | source      
        datasets/gov/example     |                  |           |             
          | data                 | sql              |           |             
                                 |                  |           |             
          |   |   | Country      |                  |           | salis       
          |   |   |   | name     | string           |           | pavadinimas 
          |   |   |   | id       | integer required |           | id          
          |   |   |   | capital  | backref          | City      |             
          |   |   | City         |                  |           | salis       
          |   |   |   | name     | string           |           | pavadinimas 
          |   |   |   | country  | ref              | Country   |             
          |   |   |   | id       | integer required |           | id          
        """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "--no-source",
            "--access",
            "open",
            "-o",
            tmp_path / "result.mmd",
            tmp_path / "manifest.csv",
        ],
    )

    with open(tmp_path / "result.mmd", "r") as file:
        contents = file.read()
        assert (
            contents
            == f"""{MERMAID_CONFIG}
classDiagram
namespace `datasets/gov/example` {{
class `datasets/gov/example/Country`["Country"]:::Entity {{
«mandatory»
id : integer [1..1]
«optional»
name : string [0..1]
}}
class `datasets/gov/example/City`["City"]:::Entity {{
«mandatory»
id : integer [1..1]
«optional»
name : string [0..1]
}}
}}
`datasets/gov/example/Country` --> "[0..1]" `datasets/gov/example/City` : capital<br/>«optional»
`datasets/gov/example/City` --> "[0..1]" `datasets/gov/example/Country` : country<br/>«optional»
classDef Entity {ENTITY_STYLES};
"""
        )


def test_copy_mmd_backref_required(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
        d | r | b | m | property | type             | ref       | source      | prepare | visibility
        datasets/gov/example     |                  |           |             |         |
          | data                 | sql              |           |             |         |
                                 |                  |           |             |         |
          |   |   | Country      |                  |           | salis       |         |
          |   |   |   | name     | string           |           | pavadinimas |         | public
          |   |   |   | id       | integer required |           | id          |         | public
          |   |   |   | cities[] | backref required | City      |             |         | 
          |   |   | City         |                  |           | salis       |         |
          |   |   |   | name     | string           |           | pavadinimas |         | public
          |   |   |   | country  | ref              | Country   |             |         | 
          |   |   |   | id       | integer required |           | id          |         | public
        """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "--no-source",
            "--access",
            "open",
            "-o",
            tmp_path / "result.mmd",
            tmp_path / "manifest.csv",
        ],
    )

    with open(tmp_path / "result.mmd", "r") as file:
        contents = file.read()
        assert (
            contents
            == f"""{MERMAID_CONFIG}
classDiagram
namespace `datasets/gov/example` {{
class `datasets/gov/example/Country`["Country"]:::Entity {{
«mandatory»
+ id : integer [1..1]
«optional»
+ name : string [0..1]
}}
class `datasets/gov/example/City`["City"]:::Entity {{
«mandatory»
+ id : integer [1..1]
«optional»
+ name : string [0..1]
}}
}}
`datasets/gov/example/Country` --> "[1..*]" `datasets/gov/example/City` : cities<br/>«mandatory»
`datasets/gov/example/City` --> "[0..1]" `datasets/gov/example/Country` : country<br/>«optional»
classDef Entity {ENTITY_STYLES};
"""
        )


def test_copy_mmd_base(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
        d | r | b | m | property | type             | ref       | source      | prepare | visibility
        datasets/gov/example     |                  |           |             |         |
          | data                 | sql              |           |             |         |
          |   |   | Settlement   |                  |           |             |         |
          |   |   |   | name     | string           |           |             |         | public
          |   |   |   | id       | integer required |           |             |         | public
          |   | Settlement       |                  |           |             |         |
          |   |   | City         |                  |           | miestas     |         |
          |   |   |   | name     |                  |           | pavadinimas |         | public
          |   |   |   | id       |                  |           | id          |         | public                   
          |   |   |   | council  | string           |           | taryba      |         | public
        """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "--no-source",
            "--access",
            "open",
            "-o",
            tmp_path / "result.mmd",
            tmp_path / "manifest.csv",
        ],
    )

    with open(tmp_path / "result.mmd", "r") as file:
        contents = file.read()
        assert (
            contents
            == f"""{MERMAID_CONFIG}
classDiagram
namespace `datasets/gov/example` {{
class `datasets/gov/example/Settlement`["Settlement"]:::Entity {{
«mandatory»
+ id : integer [1..1]
«optional»
+ name : string [0..1]
}}
class `datasets/gov/example/City`["City"]:::Entity {{
«optional»
+ council : string [0..1]
}}
}}
`datasets/gov/example/City` --|> `datasets/gov/example/Settlement`
classDef Entity {ENTITY_STYLES};
"""
        )


def test_copy_mmd_base_ref(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
        d | r | b | m | property | type             | ref       | source      | prepare | visibility
        datasets/gov/example     |                  |           |             |         |
          | data                 | sql              |           |             |         |
          |   |   | Settlement   |                  | id        |             |         |
          |   |   |   | name     | string           |           |             |         | public
          |   |   |   | id       | integer required |           |             |         | public
          |   | Settlement       |                  | id        |             |         |
          |   |   | City         |                  |           | miestas     |         |
          |   |   |   | name     |                  |           | pavadinimas |         | public
          |   |   |   | id       |                  |           | id          |         | public                   
          |   |   |   | council  | string           |           | taryba      |         | public
        """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "--no-source",
            "--access",
            "open",
            "-o",
            tmp_path / "result.mmd",
            tmp_path / "manifest.csv",
        ],
    )

    with open(tmp_path / "result.mmd", "r") as file:
        contents = file.read()
        assert (
            contents
            == f"""{MERMAID_CONFIG}
classDiagram
namespace `datasets/gov/example` {{
class `datasets/gov/example/Settlement`["Settlement"]:::Entity {{
«mandatory»
+ id : integer [1..1]
«optional»
+ name : string [0..1]
}}
class `datasets/gov/example/City`["City"]:::Entity {{
«optional»
+ council : string [0..1]
}}
}}
`datasets/gov/example/City` --|> `datasets/gov/example/Settlement` : id<br/>«optional»
classDef Entity {ENTITY_STYLES};
"""
        )


def test_copy_with_two_datasets_and_main_specified(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
        d | r | b | m | property | type             | ref       | source      | prepare | visibility
        datasets/gov/example     |                  |           |             |         |
          | data                 | sql              |           |             |         |
          |   |   | City         |                  |           | miestas     |         |
          |   |   |   | name     | string           |           | pavadinimas |         | public
          |   |   |   | id       | string           |           | id          |         | public                   
          |   |   |   | council  | string           |           | taryba      |         | public
        datasets/gov/example2    |                  |           |             |         |
          | data                 | sql              |           |             |         |
          |   |   | Country      |                  |           | miestas     |         |
          |   |   |   | name     | string           |           | pavadinimas |         | public
          |   |   |   | id       | string           |           | id          |         | public                   
        """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "--no-source",
            "--access",
            "open",
            "-d",
            "datasets/gov/example",
            "-o",
            tmp_path / "result.mmd",
            tmp_path / "manifest.csv",
        ],
    )

    with open(tmp_path / "result.mmd", "r") as file:
        contents = file.read()
        assert (
            contents
            == f"""{MERMAID_CONFIG}
classDiagram
class `datasets/gov/example/City`["City"]:::Entity {{
«optional»
+ name : string [0..1]
+ id : string [0..1]
+ council : string [0..1]
}}

namespace `datasets/gov/example2` {{
class `datasets/gov/example2/Country`["Country"]:::Entity {{
«optional»
+ name : string [0..1]
+ id : string [0..1]
}}
}}
classDef Entity {ENTITY_STYLES};
"""
        )


def test_write_mermaid_manifest_output_as_string(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
        d | r | b | m | property | type             | ref       | source      | prepare | visibility
        datasets/gov/example     |                  |           |             |         |
          | data                 | sql              |           |             |         |
          |   |   | City         |                  |           | miestas     |         |
          |   |   |   | name     | string required  |           | pavadinimas |         | public
          |   |   |   | id       | string           |           | id          |         | public                   
          |   |   |   | council  | string           |           | taryba      |         | public
        datasets/gov/example2    |                  |           |             |         |
          | data                 | sql              |           |             |         |
          |   |   | Country      |                  |           | miestas     |         |
          |   |   |   | name     | string           |           | pavadinimas |         | public
          |   |   |   | id       | string           |           | id          |         | public                   
        """),
    )
    manifest = _read_and_return_manifest(context, [str(tmp_path / "manifest.csv")])
    mermaid = write_mermaid_manifest(context, manifest, "datasets/gov/example")

    assert (
        mermaid
        == f"""{MERMAID_CONFIG}
classDiagram
class `datasets/gov/example/City`["City"]:::Entity {{
«mandatory»
+ name : string [1..1]
«optional»
+ id : string [0..1]
+ council : string [0..1]
}}

namespace `datasets/gov/example2` {{
class `datasets/gov/example2/Country`["Country"]:::Entity {{
«optional»
+ name : string [0..1]
+ id : string [0..1]
}}
}}
classDef Entity {ENTITY_STYLES};
"""
    )
