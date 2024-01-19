import pytest

from spinta.exceptions import InvalidManifestFile, ModelReferenceNotFound, ReferencedPropertyNotFound, \
    PartialTypeNotFound, DataTypeCannotBeUsedForNesting, NestedDataTypeMissmatch
from spinta.testing.tabular import create_tabular_manifest
from spinta.testing.manifest import load_manifest
from spinta.manifests.tabular.helpers import TabularManifestError


def check(tmp_path, rc, table):
    create_tabular_manifest(tmp_path / 'manifest.csv', table)
    manifest = load_manifest(rc, tmp_path / 'manifest.csv')
    assert manifest == table


def test_loading(tmp_path, rc):
    check(tmp_path, rc, '''
    id | d | r | b | m | property | source      | prepare   | type       | ref     | level | access | uri | title   | description
       | datasets/gov/example     |             |           |            |         |       | open   |     | Example |
       |   | data                 |             |           | postgresql | default |       | open   |     | Data    |
       |                          |             |           |            |         |       |        |     |         |
       |   |   |   | country      |             | code='lt' |            | code    |       | open   |     | Country |
       |   |   |   |   | code     | kodas       | lower()   | string     |         | 3     | open   |     | Code    |
       |   |   |   |   | name     | pavadinimas |           | string     |         | 3     | open   |     | Name    |
       |                          |             |           |            |         |       |        |     |         |
       |   |   |   | city         |             |           |            | name    |       | open   |     | City    |
       |   |   |   |   | name     | pavadinimas |           | string     |         | 3     | open   |     | Name    |
       |   |   |   |   | country  | šalis       |           | ref        | country | 4     | open   |     | Country |
    ''')


def test_uri(tmp_path, rc):
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
    ''')


def test_backends(tmp_path, rc):
    check(tmp_path, rc, f'''
    d | r | b | m | property | type | ref | source
      | default              | sql  |     | sqlite:///{tmp_path}/db
                             |      |     |
    ''')


def test_backends_with_models(tmp_path, rc):
    check(tmp_path, rc, f'''
    d | r | b | m | property | type   | ref | source
      | default              | sql    |     | sqlite:///{tmp_path}/db
                             |        |     |
      |   |   | country      |        |     | code
      |   |   |   | code     | string |     |
      |   |   |   | name     | string |     |
    ''')


def test_ns(tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property | type | ref                  | title               | description
                             | ns   | datasets             | All datasets        | All external datasets.
                             |      | datasets/gov         | Government datasets | All government datasets.
                             |      | datasets/gov/example | Example             |
                             |      |                      |                     |
    ''')


def test_ns_with_models(tmp_path, rc):
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
    ''')


def test_enum(tmp_path, rc):
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
    ''')


def test_enum_ref(tmp_path, rc):
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
    ''')


def test_lang(tmp_path, rc):
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
    ''')


def test_enum_negative(tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property | type    | prepare | title
    datasets/gov/example     |         |         |
                             |         |         |
      |   |   | Data         |         |         |
      |   |   |   | value    | integer |         |
                             | enum    | 1       | Positive
                             |         | -1      | Negative
    ''')


def test_units(tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property | type    | ref
    datasets/gov/example     |         |
                             |         |
      |   |   | City         |         |
      |   |   |   | founded  | date    | 1Y
    ''')


def test_boolean_enum(tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property | type    | ref   | source | prepare
    datasets/gov/example     |         |       |        |
                             | enum    | bool  |        | null
                             |         |       | no     | false
                             |         |       | yes    | true
                             |         |       |        |
      |   |   | Bool         |         |       |        |
      |   |   |   | value    | boolean | bool  |        |
    ''')


def test_enum_with_unit_name(tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property | type    | ref   | source | prepare
    datasets/gov/example     |         |       |        |
                             | enum    | m     | no     | 0
                             |         |       | yes    | 1
                             |         |       |        |
      |   |   | Bool         |         |       |        |
      |   |   |   | value    | integer | m     |        |
    ''')


def test_comment(tmp_path, rc):
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
    ''')


def test_prop_type_not_given(tmp_path, rc):
    with pytest.raises(InvalidManifestFile) as e:
        check(tmp_path, rc, '''
        d | r | b | m | property | type
        datasets/gov/example     |
          |   |   | Bool         |
          |   |   |   | value    |
        ''')
    assert e.value.context['error'] == (
        "Type is not given for 'value' property in "
        "'datasets/gov/example/Bool' model."
    )


def test_prop_type_required(tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property | type
    example                  |
                             |
      |   |   | City         |
      |   |   |   | name     | string required
      |   |   |   | place    | geometry(point) required
    ''')


def test_time_type(tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property | type
    example                  |
                             |
      |   |   | Time         |
      |   |   |   | prop     | time
    ''')


def test_explicit_ref(tmp_path, rc):
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
      ''')

def test_property_unique_add(tmp_path, rc):
    check(tmp_path, rc, '''
    d | r | b | m | property            | type
    example                             |
                                        |
      |   |   | City                    |
      |   |   |   | prop_with_unique    | string unique
      |   |   |   | prop_not_unique     | string
    ''')


def test_property_unique_add_wrong_type(tmp_path, rc):
    with pytest.raises(TabularManifestError) as e:
        check(tmp_path, rc, '''
        d | r | b | m | property | type
        datasets/gov/example     |
          |   |   | City         |
          |   |   |   | value    | string unikue
        ''')
    assert 'TabularManifestError' in str(e)


def test_property_with_ref_unique(tmp_path, rc):
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
    ''')


def test_property_with_multi_ref_unique(tmp_path, rc):
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
    ''')


def test_property_with_ref_with_unique(tmp_path, rc):
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
    ''')


def test_unique_prop_remove_when_model_ref_single(tmp_path, rc):
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
    create_tabular_manifest(tmp_path / 'manifest.csv', table)
    manifest = load_manifest(rc, tmp_path / 'manifest.csv')
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


def test_unique_prop_remove_when_model_ref_multi(tmp_path, rc):
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
    create_tabular_manifest(tmp_path / 'manifest.csv', table)
    manifest = load_manifest(rc, tmp_path / 'manifest.csv')
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


def test_with_denormalized_data(tmp_path, rc):
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
    ''')
    
    
def test_with_denormalized_data_ref_error(tmp_path, rc):
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
        ''')


def test_with_denormalized_data_undefined_error(tmp_path, rc):
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
        ''')
    assert e.value.message == (
        "Property 'country.continent.size' not found."
    )
    assert e.value.context['ref'] == "{'property': 'size', 'model': 'example/Continent'}"


def test_with_base(tmp_path, rc):
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
    ''')


def test_end_marker(tmp_path, rc):
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
    ''')


def test_with_same_base(tmp_path, rc):
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
    ''')


def test_model_param_list(tmp_path, rc):
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
    ''')


def test_model_param_list_with_source(tmp_path, rc):
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
    ''')


def test_model_param_multiple(tmp_path, rc):
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
    ''')


def test_resource_param(tmp_path, rc):
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
    ''')


def test_resource_param_multiple(tmp_path, rc):
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
    ''')


def test_multiline_prepare(tmp_path, rc):
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


def test_multiline_prepare_without_given_prepare(tmp_path, rc):
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
    ''')


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


def test_prop_array_simple_type(tmp_path, rc):
    check(tmp_path, rc, '''
        d | r | b | m | property    | type    | ref      | access
        example                     |         |          |
                                    |         |          |
          |   |   | Country         |         |          |
          |   |   |   | name        | string  |          | open
          |   |   |   | languages[] | string  |          | open
    ''')


def test_prop_array_ref_type(tmp_path, rc):
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
    ''')


def test_prop_array_customize_type(tmp_path, rc):
    check(tmp_path, rc, '''
        d | r | b | m | property    | type    | ref      | access | title
        example                     |         |          |        |
                                    |         |          |        |
          |   |   | Country         |         |          |        |
          |   |   |   | name        | string  |          | open   |
          |   |   |   | languages   | array   |          | open   | Array of languages
          |   |   |   | languages[] | string  |          | open   | Correction
    ''')


def test_prop_multi_array(tmp_path, rc):
    check(tmp_path, rc, '''
        d | r | b | m | property        | type    | ref      | access | title
        example                         |         |          |        |
                                        |         |          |        |
          |   |   | Country             |         |          |        |
          |   |   |   | name            | string  |          | open   |
          |   |   |   | languages[][][] | string  |          | open   | Correction
    ''')


def test_prop_multi_array_specific(tmp_path, rc):
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
    ''')


def test_prop_nested_denorm(tmp_path, rc):
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
    ''')


def test_prop_multi_nested_denorm(tmp_path, rc):
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
    ''')


def test_prop_multi_nested_error_partial(tmp_path, rc):
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
        ''')


def test_prop_multi_nested_multi_models(tmp_path, rc):
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
    ''')


def test_prop_multi_nested(tmp_path, rc):
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
        ''')


def test_multi_nested_incorrect(tmp_path, rc):
    with pytest.raises(DataTypeCannotBeUsedForNesting) as e:
        check(tmp_path, rc, '''
                d | r | b | m | property                       | type    | ref      | access | title
                example                                        |         |          |        |
                                                               |         |          |        |
                  |   |   | Language                           |         |          |        |
                  |   |   |   | dialect                        | string  |          | open   |
                  |   |   |   | meta.version                   | string  |          | open   |
                  |   |   |   | meta                           | integer |          | open   |
            ''')


def test_multi_nested_incorrect_reversed_order(tmp_path, rc):
    with pytest.raises(DataTypeCannotBeUsedForNesting) as e:
        check(tmp_path, rc, '''
                d | r | b | m | property                       | type    | ref      | access | title
                example                                        |         |          |        |
                                                               |         |          |        |
                  |   |   | Language                           |         |          |        |
                  |   |   |   | dialect                        | string  |          | open   |
                  |   |   |   | meta                           | integer |          | open   |
                  |   |   |   | meta.version                   | string  |          | open   |
            ''')


def test_multi_nested_incorrect_deep(tmp_path, rc):
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
            ''')


def test_multi_nested_incorrect_with_array(tmp_path, rc):
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
            ''')


def test_multi_nested_type_missmatch_with_array(tmp_path, rc):
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
            ''')


def test_multi_nested_type_missmatch_with_partial(tmp_path, rc):
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
            ''')
