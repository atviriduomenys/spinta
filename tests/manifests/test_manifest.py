import pathlib

import pytest

from spinta.exceptions import InvalidManifestFile, NoRefPropertyForDenormProperty, ReferencedPropertyNotFound, ModelReferenceNotFound, PartialTypeNotFound, DataTypeCannotBeUsedForNesting, NestedDataTypeMissmatch
from spinta.manifests.components import Manifest
from spinta.manifests.internal_sql.helpers import write_internal_sql_manifest
from spinta.testing.datasets import Sqlite
from spinta.testing.tabular import create_tabular_manifest
from spinta.testing.manifest import load_manifest
from spinta.manifests.tabular.helpers import TabularManifestError


def create_sql_manifest(
    manifest: Manifest,
    path: pathlib.Path
):
    db = Sqlite('sqlite:///' + str(path))
    with db.engine.connect():
        write_internal_sql_manifest(db.dsn, manifest)


def setup_tabular_manifest(rc, tmp_path, table):
    create_tabular_manifest(tmp_path / 'manifest.csv', table)
    return load_manifest(rc, tmp_path / 'manifest.csv')


def setup_internal_manifest(rc, tmp_path, manifest):
    create_sql_manifest(manifest, tmp_path / 'db.sqlite')
    return load_manifest(rc, 'sqlite:///' + str(tmp_path / 'db.sqlite'))


def check(tmp_path, rc, table, tabular: bool = True):
    manifest = setup_tabular_manifest(rc, tmp_path, table)
    if not tabular:
        manifest = setup_internal_manifest(rc, tmp_path, manifest)
    assert manifest == table


manifest_type = {
    "tabular": True,
    "internal_sql": False
}


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_loading(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
     d | r | b | m | property | source      | prepare   | type       | ref     | level | access | uri | title   | description
     datasets/gov/example     |             |           |            |         |       | open   |     | Example |
       | data                 |             |           | postgresql | default |       | open   |     | Data    |
                              |             |           |            |         |       |        |     |         |
       |   |   | country      |             | code='lt' |            | code    |       | open   |     | Country |
       |   |   |   | code     | kodas       | lower()   | string     |         | 3     | open   |     | Code    |
       |   |   |   | name     | pavadinimas |           | string     |         | 3     | open   |     | Name    |
                              |             |           |            |         |       |        |     |         |
       |   |   | city         |             |           |            | name    |       | open   |     | City    |
       |   |   |   | name     | pavadinimas |           | string     |         | 3     | open   |     | Name    |
       |   |   |   | country  | šalis       |           | ref        | country | 4     | open   |     | Country |
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_uri(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property | type       | ref     | uri
    datasets/gov/example     |            |         |
                             | prefix     | locn    | http://www.w3.org/ns/locn#
                             |            | ogc     | http://www.opengis.net/rdf#
                             |            |         |
      | data                 | postgresql | default |
                             |            |         |
      |   |   | Country      |            | code    |
      |   |   |   | code     | string     |         |
      |   |   |   | name     | string     |         | locn:geographicName
                             |            |         |
      |   |   | City         |            | name    |
      |   |   |   | name     | string     |         | locn:geographicName
      |   |   |   | country  | ref        | Country |
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_backends(is_tabular, tmp_path, rc):
    check(tmp_path, rc, f'''
    d | r | b | m | property | type | ref | source
      | default              | sql  |     | sqlite:///{tmp_path}/db
                             |      |     |
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_backends_with_models(is_tabular, tmp_path, rc):
    check(tmp_path, rc, f'''
    d | r | b | m | property | type   | ref | source
      | default              | sql    |     | sqlite:///{tmp_path}/db
                             |        |     |
      |   |   | country      |        |     | code
      |   |   |   | code     | string |     |
      |   |   |   | name     | string |     |
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_ns(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property | type | ref                  | title               | description
                             | ns   | datasets             | All datasets        | All external datasets.
                             |      | datasets/gov         | Government datasets | All government datasets.
                             |      | datasets/gov/example | Example             |
                             |      |                      |                     |
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_ns_with_models(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property | type   | ref                  | title               | description
                             | ns     | datasets             | All datasets        | All external datasets.
                             |        | datasets/gov         | Government datasets | All government datasets.
                             |        | datasets/gov/example | Example             |
                             |        |                      |                     |
    datasets/gov/example     |        |                      |                     |
      | data                 |        | default              |                     |
                             |        |                      |                     |
      |   |   | Country      |        |                      |                     |
      |   |   |   | name     | string |                      |                     |
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_enum(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property     | type   | source | prepare | access  | title | description
    datasets/gov/example         |        |        |         |         |       |
      | data                     |        |        |         |         |       |
                                 |        |        |         |         |       |
      |   |   | Country          |        |        |         |         |       |
      |   |   |   | name         | string |        |         |         |       |
      |   |   |   | driving_side | string |        |         |         |       |
                                 | enum   | l      | 'left'  | open    | Left  | Left side.
                                 |        | r      | 'right' | private | Right | Right side.
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_enum_ref(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property     | type   | ref     | source | prepare | access  | title | description
                                 | enum   | side    | l      | 'left'  | open    | Left  | Left side.
                                 |        |         | r      | 'right' | private | Right | Right side.
                                 |        |         |        |         |         |       |
    datasets/gov/example         |        |         |        |         |         |       |
      | data                     |        | default |        |         |         |       |
                                 |        |         |        |         |         |       |
      |   |   | Country          |        |         |        |         |         |       |
      |   |   |   | name         | string |         |        |         |         |       |
      |   |   |   | driving_side | string | side    |        |         |         |       |
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_lang(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property | type   | ref     | prepare | title       | description
    datasets/gov/example     |        |         |         | Example     | Example dataset.
                             | lang   | lt      |         | Pavyzdys    | Pavyzdinis duomenų rinkinys.
      | data                 |        | default |         |             |
                             |        |         |         |             |
      |   |   | Country      |        |         |         | Country     | Country data model.
                             | lang   | lt      |         | Šalis       | Šalies duomenų modelis.
      |   |   |   | name     | string |         |         | Name        | Country name.
                             | lang   | lt      |         | Pavadinimas | Šalies pavadinimas.
      |   |   |   | driving  | string |         |         | Driving     | Driving side.
                             | lang   | lt      |         | Vairavimas  | Eismo pusė kelyje.
                             | enum   |         | 'left'  | Left        | Left side.
                             | lang   | lt      |         | Kairė       | Kairė pusė.
                             | enum   |         | 'right' | Right       | Right side.
                             | lang   | lt      |         | Dešinė      | Dešinė pusė.
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_enum_negative(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property | type    | prepare | title
    datasets/gov/example     |         |         |
                             |         |         |
      |   |   | Data         |         |         |
      |   |   |   | value    | integer |         |
                             | enum    | 1       | Positive
                             |         | -1      | Negative
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_units(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property | type    | ref
    datasets/gov/example     |         |
                             |         |
      |   |   | City         |         |
      |   |   |   | founded  | date    | 1Y
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_boolean_enum(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property | type    | ref   | source | prepare
    datasets/gov/example     |         |       |        |
                             | enum    | bool  |        | null
                             |         |       | no     | false
                             |         |       | yes    | true
                             |         |       |        |
      |   |   | Bool         |         |       |        |
      |   |   |   | value    | boolean | bool  |        |
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_enum_with_unit_name(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property | type    | ref   | source | prepare
    datasets/gov/example     |         |       |        |
                             | enum    | m     | no     | 0
                             |         |       | yes    | 1
                             |         |       |        |
      |   |   | Bool         |         |       |        |
      |   |   |   | value    | integer | m     |        |
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_comment(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property | type    | source | prepare | access  | title      | description
    datasets/gov/example     |         |        |         |         |            |
                             | enum    | no     | 0       |         |            |
                             |         | yes    | 1       |         |            |
      | resource1            | sql     |        |         |         |            |
                             | comment | Name1  |         | private | 2022-01-01 | Comment 1.
                             |         |        |         |         |            |
      |   |   | Bool         |         |        |         |         |            |
                             | comment | Name1  |         | private | 2022-01-01 | Comment 1.
      |   |   |   | value    | integer |        |         |         |            |
                             | comment | Name2  |         |         | 2022-01-02 | Comment 2.
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_prop_type_not_given(is_tabular, tmp_path, rc):
    with pytest.raises(InvalidManifestFile) as e:
        check(tmp_path, rc, '''
        d | r | b | m | property | type
        datasets/gov/example     |
          |   |   | Bool         |
          |   |   |   | value    |
        ''', is_tabular)
    assert e.value.context['error'] == (
        "Type is not given for 'value' property in "
        "'datasets/gov/example/Bool' model."
    )


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_prop_type_required(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property | type
    example                  |
                             |
      |   |   | City         |
      |   |   |   | name     | string required
      |   |   |   | place    | geometry(point) required
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_time_type(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property | type
    example                  |
                             |
      |   |   | Time         |
      |   |   |   | prop     | time
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_explicit_ref(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property | type       | ref
    datasets/gov/example     |            |
      | data                 | postgresql | default
                             |            |
      |   |   | Country      |            | id
      |   |   |   | id       | integer    |
      |   |   |   | code     | string     |
      |   |   |   | name     | string     |
                             |            |
      |   |   | City         |            | name
      |   |   |   | name     | string     |
      |   |   |   | country  | ref        | Country[code]
      ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_property_unique_add(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property            | type
    example                             |
                                        |
      |   |   | City                    |
      |   |   |   | prop_with_unique    | string unique
      |   |   |   | prop_not_unique     | string
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_property_unique_add_wrong_type(is_tabular, tmp_path, rc):
    with pytest.raises(TabularManifestError) as e:
        check(tmp_path, rc, '''
        d | r | b | m | property | type
        datasets/gov/example     |
          |   |   | City         |
          |   |   |   | value    | string unikue
        ''', is_tabular)
    assert 'TabularManifestError' in str(e)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_property_with_ref_unique(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property | type               | ref                  | uri
    datasets/gov/example     |                    |                      |
                             | prefix             | locn                 | http://www.w3.org/ns/locn#
                             |                    | ogc                  | http://www.opengis.net/rdf#
                             |                    |                      |
      | data                 | postgresql         | default              |
                             |                    |                      |
      |   |   | Country      |                    |                      |
      |   |   |   | name     | string unique      |                      | locn:geographicName
                             |                    |                      |
      |   |   | City         |                    |                      |
                             | unique             | name, country        |
      |   |   |   | name     | string             |                      | locn:geographicName
      |   |   |   | country  | ref                | Country              |
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_property_with_multi_ref_unique(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property | type               | ref                  | uri
    datasets/gov/example     |                    |                      |
                             | prefix             | locn                 | http://www.w3.org/ns/locn#
                             |                    | ogc                  | http://www.opengis.net/rdf#
                             |                    |                      |
      | data                 | postgresql         | default              |
                             |                    |                      |
      |   |   | Country      |                    |                      |
      |   |   |   | name     | string unique      |                      | locn:geographicName
                             |                    |                      |
      |   |   | City         |                    |                      |
                             | unique             | name, country        |
                             | unique             | text                 |
                             | unique             | another, text        |
      |   |   |   | name     | string             |                      | locn:geographicName
      |   |   |   | text     | string             |                      | locn:geographicName
      |   |   |   | another  | string             |                      | locn:geographicName
      |   |   |   | country  | ref                | Country              |
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_property_with_ref_with_unique(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property | type               | ref                  | uri
    datasets/gov/example     |                    |                      |
                             | prefix             | locn                 | http://www.w3.org/ns/locn#
                             |                    | ogc                  | http://www.opengis.net/rdf#
                             |                    |                      |
      | data                 | postgresql         | default              |
                             |                    |                      |
      |   |   | Country      |                    |                      |
      |   |   |   | name     | string unique      |                      | locn:geographicName
                             |                    |                      |
      |   |   | City         |                    |                      |
                             | unique             | country              |
      |   |   |   | name     | string             |                      | locn:geographicName
      |   |   |   | country  | ref                | Country              |
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_unique_prop_remove_when_model_ref_single(is_tabular, tmp_path, rc):
    table = '''
    d | r | b | m | property | type               | ref                  | uri
    datasets/gov/example     |                    |                      |
      | data                 | postgresql         | default              |
                             |                    |                      |
      |   |   | Country      |                    | name                 |
      |   |   |   | name     | string unique      |                      |
                             |                    |                      |
      |   |   | City         |                    | name                 |
                             | unique             | name                 |   
                             | unique             | country              |
      |   |   |   | name     | string             |                      |
      |   |   |   | country  | ref                | Country              |
    '''
    manifest = setup_tabular_manifest(rc, tmp_path, table)
    if not is_tabular:
        manifest = setup_internal_manifest(rc, tmp_path, manifest)
    assert manifest == '''
    d | r | b | m | property | type               | ref                  | uri
    datasets/gov/example     |                    |                      |
      | data                 | postgresql         | default              |
                             |                    |                      |
      |   |   | Country      |                    | name                 |
      |   |   |   | name     | string             |                      |
                             |                    |                      |
      |   |   | City         |                    | name                 |
                             | unique             | country              |
      |   |   |   | name     | string             |                      |
      |   |   |   | country  | ref                | Country              |
    '''


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_unique_prop_remove_when_model_ref_multi(is_tabular, tmp_path, rc):
    table = '''
    d | r | b | m | property | type               | ref                  | uri
    datasets/gov/example     |                    |                      |
      | data                 | postgresql         | default              |
                             |                    |                      |
      |   |   | Country      |                    | name, id             |
                             | unique             | name, id             |  
      |   |   |   | name     | string             |                      |
      |   |   |   | id       | string unique      |                      |
                             |                    |                      |
      |   |   | City         |                    | name, id             |
                             | unique             | name, id             |   
                             | unique             | name                 |   
                             | unique             | country              |
      |   |   |   | name     | string             |                      |
      |   |   |   | id       | string             |                      |
      |   |   |   | country  | ref                | Country              |
    '''
    manifest = setup_tabular_manifest(rc, tmp_path, table)
    if not is_tabular:
        manifest = setup_internal_manifest(rc, tmp_path, manifest)
    assert manifest == '''
    d | r | b | m | property | type               | ref                  | uri
    datasets/gov/example     |                    |                      |
      | data                 | postgresql         | default              |
                             |                    |                      |
      |   |   | Country      |                    | name, id             |
      |   |   |   | name     | string             |                      |
      |   |   |   | id       | string unique      |                      |
                             |                    |                      |
      |   |   | City         |                    | name, id             |
                             | unique             | name                 |
                             | unique             | country              |
      |   |   |   | name     | string             |                      |
      |   |   |   | id       | string             |                      |
      |   |   |   | country  | ref                | Country              |
    '''


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_with_denormalized_data(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property               | type   | ref       | access
    example                                |        |           |
                                           |        |           |
      |   |   | Continent                  |        |           |
      |   |   |   | name                   | string |           | open
                                           |        |           |
      |   |   | Country                    |        |           |
      |   |   |   | name                   | string |           | open
      |   |   |   | continent              | ref    | Continent | open
                                           |        |           |
      |   |   | City                       |        |           |
      |   |   |   | name                   | string |           | open
      |   |   |   | country                | ref    | Country   | open
      |   |   |   | country.name           |        |           | open
      |   |   |   | country.continent.name |        |           | open
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_with_denormalized_data_ref_error(is_tabular, tmp_path, rc):
    with pytest.raises(PartialTypeNotFound) as e:
        check(tmp_path, rc, '''
        d | r | b | m | property               | type   | ref       | access
        example                                |        |           |
                                               |        |           |
          |   |   | Country                    |        |           |
          |   |   |   | name                   | string |           | open
                                               |        |           |
          |   |   | City                       |        |           |
          |   |   |   | name                   | string |           | open
          |   |   |   | country.name           |        |           | open
        ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_with_denormalized_data_undefined_error(is_tabular, tmp_path, rc):
    with pytest.raises(ReferencedPropertyNotFound) as e:
        check(tmp_path, rc, '''
        d | r | b | m | property               | type   | ref       | access
        example                                |        |           |
                                               |        |           |
          |   |   | Continent                  |        |           |
          |   |   |   | name                   | string |           | open
                                               |        |           |
          |   |   | Country                    |        |           |
          |   |   |   | name                   | string |           | open
          |   |   |   | continent              | ref    | Continent | open
                                               |        |           |
          |   |   | City                       |        |           |
          |   |   |   | name                   | string |           | open
          |   |   |   | country                | ref    | Country   | open
          |   |   |   | country.name           |        |           | open
          |   |   |   | country.continent.size |        |           | open
        ''', is_tabular)
    assert e.value.message == (
        "Property 'country.continent.size' not found."
    )
    assert e.value.context['ref'] == "{'property': 'size', 'model': 'example/Continent'}"


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_with_base(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property   | type    | ref
    datasets/gov/example       |         |
                               |         |
      |   |   | Base           |         |
      |   |   |   | id         | integer |
                               |         |
      |   | Base               |         |
      |   |   | Location       |         |
      |   |   |   | id         |         |
      |   |   |   | name       | string  |
      |   |   |   | population | integer |
                               |         |
      |   | Location           |         | name
      |   |   | City           |         | name
      |   |   |   | id         |         |
      |   |   |   | name       |         |
      |   |   |   | population |         |
                               |         |
      |   |   | Village        |         | name
      |   |   |   | id         |         |
      |   |   |   | name       |         |
      |   |   |   | population |         |
      |   |   |   | region     | ref     | Location
                               |         |
      |   | /                  |         |
      |   |   | Country        |         |
      |   |   |   | id         | integer |
      |   |   |   | name       | string  |
      |   |   |   | population | integer |
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_end_marker(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property   | type    | ref
    datasets/gov/example       |         |
      | resource1              | sql     |
                               |         |
      |   |   | Location       |         |
      |   |   |   | id         | integer |
      |   |   |   | name       | string  |
      |   |   |   | population | integer |
                               |         |
      |   | Location           |         |
      |   |   | City           |         |
      |   |   |   | id         |         |
      |   |   |   | name       |         |
      |   |   |   | population |         |
      | /                      |         |
                               |         |
      |   |   | Village        |         |
      |   |   |   | id         | integer |
      |   |   |   | name       | string  |
      |   |   |   | population | integer |
      |   |   |   | region     | ref     | Location
    /                          |         |
                               |         |
      |   |   | Country        |         |
      |   |   |   | id         | integer |
      |   |   |   | name       | string  |
      |   |   |   | population | integer |
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_with_same_base(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property   | type    | ref      | level
    datasets/gov/example       |         |          |
                               |         |          |
      |   |   | Base           |         |          |
      |   |   |   | id         | integer |          |
                               |         |          |
      |   | Base               |         |          |
      |   |   | Location       |         |          |
      |   |   |   | id         |         |          |
      |   |   |   | name       | string  |          |
      |   |   |   | population | integer |          |
                               |         |          |
      |   | Location           |         | name     | 4
      |   |   | City           |         | name     |
      |   |   |   | id         |         |          |
      |   |   |   | name       |         |          |
      |   |   |   | population |         |          |
                               |         |          |
      |   | Location           |         | name     | 3
      |   |   | Village        |         | name     |
      |   |   |   | id         |         |          |
      |   |   |   | name       |         |          |
      |   |   |   | population |         |          |
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_model_param_list(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property   | type    | ref     | source | prepare
    datasets/gov/example       |         |         |        |
                               |         |         |        |
      |   |   | Location       |         |         |        |
                               | param   | country |        | 'lt'
                               |         |         |        | 'lv'
                               |         |         |        | 'ee'
      |   |   |   | id         | integer |         |        |
      |   |   |   | name       | string  |         |        |
      |   |   |   | population | integer |         |        |
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_model_param_list_with_source(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property   | type    | ref     | source | prepare
    datasets/gov/example       |         |         |        |
                               |         |         |        |
      |   |   | Location       |         |         |        |
                               | param   | country |        | 'lt'
                               |         |         |        | 'lv'
                               |         |         |        | 'ee'
                               |         |         | es     |
      |   |   |   | id         | integer |         |        |
      |   |   |   | name       | string  |         |        |
      |   |   |   | population | integer |         |        |
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_model_param_multiple(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property   | type    | ref     | source   | prepare
    datasets/gov/example       |         |         |          |
                               |         |         |          |
      |   |   | Location       |         |         |          |
                               | param   | country |          | 'lt'
                               |         |         |          | 'lv'
                               |         |         |          | 'ee'
                               |         |         | es       |
                               | param   | test    | Location | select(name)
      |   |   |   | id         | integer |         |          |
      |   |   |   | name       | string  |         |          |
      |   |   |   | population | integer |         |          |
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_resource_param(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property   | type    | ref     | source   | prepare
    datasets/gov/example       |         |         |          |
      | resource1              |         | default |          | sql
                               | param   | country |          | 'lt'
                               |         |         |          |
      |   |   | Location       |         |         |          |
                               | param   | country |          | 'lt'
                               |         |         |          | 'lv'
                               |         |         |          | 'ee'
                               |         |         | es       |
                               | param   | test    | Location | select(name)
      |   |   |   | id         | integer |         |          |
      |   |   |   | name       | string  |         |          |
      |   |   |   | population | integer |         |          |
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_resource_param_multiple(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property   | type    | ref     | source   | prepare
    datasets/gov/example       |         |         |          |
      | resource1              |         | default |          | sql
                               | param   | country |          | 'lt'
                               | param   | name    | Location | select(name)
                               | param   | date    |          | range(station.start_time, station.end_time, freq: 'd')
                               |         |         |          |
      |   |   | Location       |         |         |          |
                               | param   | country |          | 'lt'
                               |         |         |          | 'lv'
                               |         |         |          | 'ee'
                               |         |         | es       |
                               | param   | test    | Location | select(name)
      |   |   |   | id         | integer |         |          |
      |   |   |   | name       | string  |         |          |
      |   |   |   | population | integer |         |          |
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_multiline_prepare(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property   | type    | ref     | source   | prepare
    datasets/gov/example       |         |         |          |
                               |         |         |          |
      |   |   | Location       |         |         |          |
      |   |   |   | id         | integer |         |          |
      |   |   |   | name       | string  |         |          | cast()
                               |         |         | 'namas'  | swap('Namas')
                               |         |         |          | swap('kiemas', 'Kiemas')
      |   |   |   | population | integer |         |          |
    ''')


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_multiline_prepare_without_given_prepare(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property   | type    | ref     | source   | prepare
    datasets/gov/example       |         |         |          |
                               |         |         |          |
      |   |   | Location       |         |         |          |
      |   |   |   | id         | integer |         |          |
      |   |   |   | name       | string  |         |          |
                               |         |         | 'namas'  | swap('Namas')
                               |         |         |          | swap('kiemas', 'Kiemas')
      |   |   |   | population | integer |         |          |
    ''', is_tabular)


@pytest.mark.skip('backref not implemented yet #96')
def test_prop_array_backref(tmp_path, rc):
    check(tmp_path, rc, '''
        d | r | b | m | property    | type    | ref      | access
        example                     |         |          |
                                    |         |          |
          |   |   | Language        |         |          |
          |   |   |   | name        | string  |          | open
          |   |   |   | countries[] | backref | Country  | open
                                    |         |          |
          |   |   | Country         |         |          |
          |   |   |   | name        | string  |          | open
          |   |   |   | languages[] | ref     | Language | open
    ''')


@pytest.mark.skip('backref not implemented yet #96')
def test_prop_array_with_custom_backref(rc, tmp_path):
    check(tmp_path, rc, '''
        d | r | b | m | property    | type                                       | ref
        example                     |                                            |
                                    |                                            |
          |   |   | Language        |                                            |
          |   |   |   | name        | string                                     |
          |   |   |   | countries[] | backref                                    | Country
                                    |                                            |
          |   |   | Country         |                                            |
          |   |   |   | name        | string                                     |
          |   |   |   | languages   | array                                      | CountryLanguage[country, language]
          |   |   |   | languages[] | ref                                        | Language
                                    |                                            |
          |   |   | CountryLanguage |                                            |
          |   |   |   | language    | ref                                        | Language
          |   |   |   | country     | ref                                        | Country
    ''')


@pytest.mark.skip('backref not implemented yet #96')
def test_prop_array_with_custom_without_properties_backref(rc, tmp_path):
    check(tmp_path, rc, '''
        d | r | b | m | property    | type                                       | ref
        example                     |                                            |
                                    |                                            |
          |   |   | Language        |                                            |
          |   |   |   | name        | string                                     |
          |   |   |   | countries[] | backref                                    | Country
                                    |                                            |
          |   |   | Country         |                                            |
          |   |   |   | name        | string                                     |
          |   |   |   | languages   | array                                      | CountryLanguage
          |   |   |   | languages[] | ref                                        | Language
                                    |                                            |
          |   |   | CountryLanguage |                                            |
          |   |   |   | language    | ref                                        | Language
          |   |   |   | country     | ref                                        | Country
    ''')


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_prop_array_simple_type(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
        d | r | b | m | property    | type    | ref      | access
        example                     |         |          |
                                    |         |          |
          |   |   | Country         |         |          |
          |   |   |   | name        | string  |          | open
          |   |   |   | languages[] | string  |          | open
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_prop_array_ref_type(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
        d | r | b | m | property    | type    | ref      | access
        example                     |         |          |
                                    |         |          |
          |   |   | Language        |         |          |
          |   |   |   | name        | string  |          | open
                                    |         |          |
          |   |   | Country         |         |          |
          |   |   |   | name        | string  |          | open
          |   |   |   | languages[] | ref     | Language | open
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_prop_array_customize_type(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
        d | r | b | m | property    | type    | ref      | access | title
        example                     |         |          |        |
                                    |         |          |        |
          |   |   | Country         |         |          |        |
          |   |   |   | name        | string  |          | open   |
          |   |   |   | languages   | array   |          | open   | Array of languages
          |   |   |   | languages[] | string  |          | open   | Correction
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_prop_multi_array(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
        d | r | b | m | property        | type    | ref      | access | title
        example                         |         |          |        |
                                        |         |          |        |
          |   |   | Country             |         |          |        |
          |   |   |   | name            | string  |          | open   |
          |   |   |   | languages[][][] | string  |          | open   | Correction
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_prop_multi_array_specific(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
        d | r | b | m | property        | type    | ref      | access | title
        example                         |         |          |        |
                                        |         |          |        |
          |   |   | Country             |         |          |        |
          |   |   |   | name            | string  |          | open   |
          |   |   |   | languages       | array   |          | open   | Correction T0
          |   |   |   | languages[]     | array   |          | open   | Correction T1
          |   |   |   | languages[][]   | array   |          | open   | Correction T2
          |   |   |   | languages[][][] | string  |          | open   | Correction T3
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_prop_nested_denorm(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
        d | r | b | m | property        | type    | ref      | access | title
        example                         |         |          |        |
                                        |         |          |        |
          |   |   | Language            |         |          |        |
          |   |   |   | dialect         | string  |          | open   |
                                        |         |          |        |
          |   |   | Country             |         |          |        |
          |   |   |   | name            | string  |          | open   |
          |   |   |   | langs[]         | ref     | Language | open   |
          |   |   |   | langs[].dialect |         |          | open   | Denorm
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_prop_multi_nested_denorm(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
        d | r | b | m | property          | type    | ref      | access | title
        example                           |         |          |        |
                                          |         |          |        |
          |   |   | Language              |         |          |        |
          |   |   |   | dialect           | string  |          | open   |
                                          |         |          |        |
          |   |   | Country               |         |          |        |
          |   |   |   | name              | string  |          | open   |
          |   |   |   | langs             | array   |          | open   |
          |   |   |   | langs[]           | array   |          | open   |
          |   |   |   | langs[][]         | ref     | Language | open   |
          |   |   |   | langs[][].dialect |         |          | open   |
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_prop_multi_nested_error_partial(is_tabular, tmp_path, rc):
    with pytest.raises(PartialTypeNotFound) as e:
        check(tmp_path, rc, '''
            d | r | b | m | property          | type    | ref      | access | title
            example                           |         |          |        |
                                              |         |          |        |
              |   |   | Language              |         |          |        |
              |   |   |   | dialect           | string  |          | open   |
                                              |         |          |        |
              |   |   | Country               |         |          |        |
              |   |   |   | name              | string  |          | open   |
              |   |   |   | langs             | array   |          | open   |
              |   |   |   | langs[][].dialect |         |          | open   |
        ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_prop_multi_nested_multi_models(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
        d | r | b | m | property               | type    | ref       | access | title
        example                                |         |           |        |
                                               |         |           |        |
          |   |   | Continent                  |         | id        |        |
          |   |   |   | id                     | integer |           | open   |
          |   |   |   | name                   | string  |           | open   |
                                               |         |           |        |
          |   |   | Country                    |         | id        |        |
          |   |   |   | id                     | integer |           | open   |
          |   |   |   | name                   | string  |           | open   |
          |   |   |   | continent              | ref     | Continent | open   |
                                               |         |           |        |
          |   |   | City                       |         | id        |        |
          |   |   |   | id                     | integer |           | open   |
          |   |   |   | country                | ref     | Country   | open   |
          |   |   |   | country.code           | string  |           | open   |
          |   |   |   | country.name           |         |           | open   |
          |   |   |   | country.continent.code | string  |           | open   |
          |   |   |   | country.continent.name |         |           | open   |
    ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_prop_multi_nested(is_tabular, tmp_path, rc):
    check(tmp_path, rc, '''
            d | r | b | m | property                       | type    | ref      | access | title
            example                                        |         |          |        |
                                                           |         |          |        |
              |   |   | Language                           |         |          |        |
              |   |   |   | dialect                        | string  |          | open   |
              |   |   |   | meta                           | object  |          | open   |
              |   |   |   | meta.version                   | integer |          | open   |
                                                           |         |          |        |
              |   |   | Country                            |         |          |        |
              |   |   |   | name                           | string  |          | open   |
              |   |   |   | meta                           | object  |          | open   |
              |   |   |   | meta.version                   | integer |          | open   |
              |   |   |   | meta.old                       | object  |          | open   |
              |   |   |   | meta.old.language              | ref     | Language | open   |
              |   |   |   | meta.old.language.dialect      |         |          | open   |
              |   |   |   | meta.old.language.meta.version | integer |          | open   |
              |   |   |   | meta.langs                     | array   |          | open   |
              |   |   |   | meta.langs[]                   | array   |          | open   |
              |   |   |   | meta.langs[][]                 | ref     | Language | open   |
              |   |   |   | meta.langs[][].dialect         |         |          | open   |
        ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_multi_nested_incorrect(is_tabular, tmp_path, rc):
    with pytest.raises(DataTypeCannotBeUsedForNesting) as e:
        check(tmp_path, rc, '''
                d | r | b | m | property                       | type    | ref      | access | title
                example                                        |         |          |        |
                                                               |         |          |        |
                  |   |   | Language                           |         |          |        |
                  |   |   |   | dialect                        | string  |          | open   |
                  |   |   |   | meta.version                   | string  |          | open   |
                  |   |   |   | meta                           | integer |          | open   |
            ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_multi_nested_incorrect_reversed_order(is_tabular, tmp_path, rc):
    with pytest.raises(DataTypeCannotBeUsedForNesting) as e:
        check(tmp_path, rc, '''
                d | r | b | m | property                       | type    | ref      | access | title
                example                                        |         |          |        |
                                                               |         |          |        |
                  |   |   | Language                           |         |          |        |
                  |   |   |   | dialect                        | string  |          | open   |
                  |   |   |   | meta                           | integer |          | open   |
                  |   |   |   | meta.version                   | string  |          | open   |
            ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_multi_nested_incorrect_deep(is_tabular, tmp_path, rc):
    with pytest.raises(DataTypeCannotBeUsedForNesting) as e:
        check(tmp_path, rc, '''
                d | r | b | m | property                       | type    | ref      | access | title
                example                                        |         |          |        |
                                                               |         |          |        |
                  |   |   | Language                           |         |          |        |
                  |   |   |   | dialect                        | string  |          | open   |
                  |   |   |   | meta.version.id                | integer |          | open   |
                  |   |   |   | meta.version                   | string  |          | open   |
                  |   |   |   | meta                           | object  |          | open   |
            ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_multi_nested_incorrect_with_array(is_tabular, tmp_path, rc):
    with pytest.raises(DataTypeCannotBeUsedForNesting) as e:
        check(tmp_path, rc, '''
                d | r | b | m | property                       | type    | ref      | access | title
                example                                        |         |          |        |
                                                               |         |          |        |
                  |   |   | Language                           |         |          |        |
                  |   |   |   | dialect                        | string  |          | open   |
                  |   |   |   | meta.version[].id              | integer |          | open   |
                  |   |   |   | meta.version[]                 | string  |          | open   |
                  |   |   |   | meta                           | object  |          | open   |
            ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_multi_nested_type_missmatch_with_array(is_tabular, tmp_path, rc):
    with pytest.raises(NestedDataTypeMissmatch) as e:
        check(tmp_path, rc, '''
                d | r | b | m | property                       | type    | ref      | access | title
                example                                        |         |          |        |
                                                               |         |          |        |
                  |   |   | Language                           |         |          |        |
                  |   |   |   | dialect                        | string  |          | open   |
                  |   |   |   | meta.version.id                | integer |          | open   |
                  |   |   |   | meta.version[]                 | string  |          | open   |
                  |   |   |   | meta                           | object  |          | open   |
            ''', is_tabular)


@pytest.mark.parametrize("is_tabular", manifest_type.values(), ids=manifest_type.keys())
def test_multi_nested_type_missmatch_with_partial(is_tabular, tmp_path, rc):
    with pytest.raises(NestedDataTypeMissmatch) as e:
        check(tmp_path, rc, '''
                d | r | b | m | property                       | type    | ref      | access | title
                example                                        |         |          |        |
                                                               |         |          |        |
                  |   |   | Language                           |         |          |        |
                  |   |   |   | dialect                        | string  |          | open   |
                  |   |   |   | meta.version[]                 | string  |          | open   |
                  |   |   |   | meta.version.id                | integer |          | open   |
                  |   |   |   | meta                           | object  |          | open   |
            ''', is_tabular)
