from pathlib import Path

import pytest

from spinta.core.config import RawConfig
from spinta.exceptions import MissingRefModel, PropertyNotFound, FieldNotInResource, LangNotDeclared
from spinta.testing.manifest import load_manifest, load_manifest_get_context
from spinta.types.datatype import Object


@pytest.mark.manifests("internal_sql", "csv")
def test_raises_missing_ref_model(manifest_type, tmp_path, rc):
    manifest = """
    d | r | b | m | property | type       | ref     | uri
    datasets/gov/example     |            |         |
                             |            |         |
      | data                 | postgresql | default |
                             |            |         |
      |   |   | Country      |            | code    |
      |   |   |   | code     | string     |         |
      |   |   |   | name     | string     |         | locn:geographicName
                             |            |         |
      |   |   | City         |            | name    |
      |   |   |   | name     | string     |         | locn:geographicName
      |   |   |   | country  | ref        |         |
    """

    with pytest.raises(MissingRefModel) as e:
        load_manifest(rc, manifest=manifest, manifest_type=manifest_type, tmp_path=tmp_path)
    assert (
        str(e.value)
        == """Property "country" of type "ref" in the model "City" should have a model name in the `ref` column, to which it refers.
  Context:
    component: spinta.types.datatype.Ref
    manifest: default
    schema: 10
    dataset: datasets/gov/example
    resource: data
    model: datasets/gov/example/City
    entity: 
    property: country
    attribute: 
    type: ref
    resource.backend: default
    model_name: City
    property_name: country
    property_type: ref
"""
    )


@pytest.mark.manifests("internal_sql", "csv")
def test_undeclared_ref_model_transforms_to_object(manifest_type: str, tmp_path: Path, rc: RawConfig):
    """When a ref points to a model not declared in the manifest, the property
    is silently downgraded to object and a restore comment is appended."""
    context = load_manifest_get_context(
        rc,
        manifest="""
    d | r | b | m | property | type   | ref              | source | access  | level
    example                  |        |                  |        |         |
                             |        |                  |        |         |
      |   |   | City         |        |                  |        |         |
      |   |   |   | name     | string |                  |        | private |
      |   |   |   | country  | ref    | example2/Country | A      | private |
    """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )
    store = context.get("store")
    city = store.manifest.get_objects()["model"]["example/City"]
    country_property = city.properties["country"]

    assert isinstance(country_property.dtype, Object)
    assert country_property.level.value == 2
    assert len(country_property.comments) == 1

    comment = country_property.comments[0]
    assert comment.parent == "type"
    assert comment.level == 4
    # internal-sql round-trips through spyna which normalises spacing and quote style
    prepare = comment.prepare.replace(" ", "").replace('"', "").replace("'", "")
    assert "type:ref" in prepare
    assert "ref:example2/Country" in prepare


@pytest.mark.manifests("internal_sql", "csv")
def test_undeclared_ref_with_refprops_transforms_to_object(manifest_type: str, tmp_path: Path, rc: RawConfig):
    """When a ref with explicit bracket refprops points to an undeclared model,
    the restore expression includes the bracket syntax: ref:Model[prop]."""
    context = load_manifest_get_context(
        rc,
        manifest="""
    d | r | b | m | property | type   | ref                    | access
    example                  |        |                        |
                             |        |                        |
      |   |   | City         |        |                        |
      |   |   |   | name     | string |                        | private
      |   |   |   | country  | ref    | example2/Country[code] | private
    """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )
    store = context.get("store")
    city = store.manifest.get_objects()["model"]["example/City"]
    country_property = city.properties["country"]

    assert isinstance(country_property.dtype, Object)
    assert country_property.level.value == 2
    assert len(country_property.comments) == 1

    comment = country_property.comments[0]
    assert comment.parent == "type"
    assert comment.level == 4
    prepare = comment.prepare.replace(" ", "").replace('"', "").replace("'", "")
    assert "type:ref" in prepare
    assert "ref:example2/Country[code]" in prepare


@pytest.mark.manifests("internal_sql", "csv")
def test_undeclared_backref_model_transforms_to_object(manifest_type: str, tmp_path: Path, rc: RawConfig):
    """When a backref points to an undeclared model, the property is downgraded
    to object and a restore comment with type:backref is appended."""
    context = load_manifest_get_context(
        rc,
        manifest="""
    d | r | b | m | property    | type    | ref              | access
    example                     |         |                  |
                                |         |                  |
      |   |   | City            |         |                  |
      |   |   |   | name        | string  |                  | private
      |   |   |   | countries   | backref | example2/Country | private
    """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )
    store = context.get("store")
    city = store.manifest.get_objects()["model"]["example/City"]
    countries_property = city.properties["countries"]

    assert isinstance(countries_property.dtype, Object)
    assert countries_property.level.value == 2
    assert len(countries_property.comments) == 1

    comment = countries_property.comments[0]
    assert comment.parent == "type"
    assert comment.level == 4
    prepare = comment.prepare.replace(" ", "").replace('"', "").replace("'", "")
    assert "type:backref" in prepare
    assert "ref:example2/Country" in prepare


@pytest.mark.manifests("internal_sql", "csv")
def test_undeclared_array_model_transforms_to_object(manifest_type, tmp_path, rc):
    """When an array property references an undeclared intermediate table model,
    the property is downgraded to object and a restore comment with type:array is appended."""
    context = load_manifest_get_context(
        rc,
        manifest="""
    d | r | b | m | property  | type   | ref                               | access
    example                   |        |                                   |
                              |        |                                   |
      |   |   | City          |        |                                   |
      |   |   |   | name      | string |                                   | private
      |   |   |   | countries | array  | example2/CityCountry[city,country]| private
    """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )
    store = context.get("store")
    city = store.manifest.get_objects()["model"]["example/City"]
    countries_property = city.properties["countries"]

    assert isinstance(countries_property.dtype, Object)
    assert countries_property.level.value == 2
    assert len(countries_property.comments) == 1

    comment = countries_property.comments[0]
    assert comment.parent == "type"
    assert comment.level == 4
    prepare = comment.prepare.replace(" ", "").replace('"', "").replace("'", "")
    assert "type:array" in prepare
    assert "ref:example2/CityCountry[city,country]" in prepare


@pytest.mark.manifests("csv")
def test_scope_with_valid_property_loads(manifest_type: str, tmp_path: Path, rc: RawConfig):
    context = load_manifest_get_context(
        rc,
        manifest="""
    d | r | b | m | property | type   | ref | prepare       | access
    example                  |        |     |               |
      |   |   | City         |        | id  |               |
      |   |   |              | scope  | ltu | code = 'lt'   | private
      |   |   |   | id       | string |     |               | private
      |   |   |   | code     | string |     |               | private
        """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )
    store = context.get("store")
    city = store.manifest.get_objects()["model"]["example/City"]

    assert "ltu" in city.scopes
    assert city.scopes["ltu"].model is city
    assert city.scopes["ltu"].prepare is not None


@pytest.mark.manifests("csv")
def test_scope_with_unknown_property_raises(manifest_type, tmp_path, rc):
    with pytest.raises(PropertyNotFound):
        load_manifest_get_context(
            rc,
            manifest="""
    d | r | b | m | property | type   | ref | prepare           | access
    example                  |        |     |                   |
      |   |   | City         |        | id  |                   |
      |   |   |              | scope  | bad | foo = 'lt'        | private
      |   |   |   | id       | string |     |                   | private
      |   |   |   | code     | string |     |                   | private
            """,
            manifest_type=manifest_type,
            tmp_path=tmp_path,
        )


@pytest.mark.manifests("csv")
def test_scope_with_select_bare_identifier_raises(manifest_type: str, tmp_path: Path, rc: RawConfig):
    with pytest.raises(PropertyNotFound):
        load_manifest_get_context(
            rc,
            manifest="""
    d | r | b | m | property | type   | ref | prepare         | access
    example                  |        |     |                 |
      |   |   | City         |        | id  |                 |
      |   |   |              | scope  | bad | select(country) | private
      |   |   |   | id       | string |     |                 | private
      |   |   |   | code     | string |     |                 | private
            """,
            manifest_type=manifest_type,
            tmp_path=tmp_path,
        )


@pytest.mark.manifests("csv")
def test_scope_with_valid_denorm_property_loads(manifest_type: str, tmp_path: Path, rc: RawConfig):
    context = load_manifest_get_context(
        rc,
        manifest="""
    d | r | b | m | property     | type   | ref     | prepare              | access
    example                      |        |         |                      |
      |   |   | City             |        | id      |                      |
      |   |   |                  | scope  | denorm  | country.code = 'lt'  | private
      |   |   |   | id           | string |         |                      | private
      |   |   |   | country      | ref    | Country |                      | private
      |   |   |   | country.code | string |         |                      | private
      |   |   | Country          |        | id      |                      |
      |   |   |   | id           | string |         |                      | private
      |   |   |   | code         | string |         |                      | private
        """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )
    city = context.get("store").manifest.get_objects()["model"]["example/City"]
    assert "denorm" in city.scopes
    assert city.scopes["denorm"].model is city


@pytest.mark.manifests("csv")
def test_scope_with_invalid_denorm_subproperty_raises(manifest_type: str, tmp_path: Path, rc: RawConfig):
    with pytest.raises((PropertyNotFound, FieldNotInResource)):
        load_manifest_get_context(
            rc,
            manifest="""
    d | r | b | m | property     | type   | ref     | prepare               | access
    example                      |        |         |                       |
      |   |   | City             |        | id      |                       |
      |   |   |                  | scope  | bad     | country.foo = 'x'     | private
      |   |   |   | id           | string |         |                       | private
      |   |   |   | country      | ref    | Country |                       | private
      |   |   |   | country.code | string |         |                       | private
      |   |   | Country          |        | id      |                       |
      |   |   |   | id           | string |         |                       | private
      |   |   |   | code         | string |         |                       | private
            """,
            manifest_type=manifest_type,
            tmp_path=tmp_path,
        )


@pytest.mark.manifests("csv")
def test_scope_with_valid_object_subproperty_loads(manifest_type: str, tmp_path: Path, rc: RawConfig):
    context = load_manifest_get_context(
        rc,
        manifest="""
    d | r | b | m | property       | type   | ref | prepare                  | access
    example                        |        |     |                          |
      |   |   | City               |        | id  |                          |
      |   |   |                    | scope  | obj | address.street = 'Main'  | private
      |   |   |   | id             | string |     |                          | private
      |   |   |   | address        | object |     |                          | private
      |   |   |   | address.street | string |     |                          | private
      |   |   |   | address.city   | string |     |                          | private
        """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )
    city = context.get("store").manifest.get_objects()["model"]["example/City"]
    assert "obj" in city.scopes


@pytest.mark.manifests("csv")
def test_scope_with_invalid_object_subproperty_raises(manifest_type: str, tmp_path: Path, rc: RawConfig):
    with pytest.raises((PropertyNotFound, FieldNotInResource)):
        load_manifest_get_context(
            rc,
            manifest="""
    d | r | b | m | property       | type   | ref | prepare              | access
    example                        |        |     |                      |
      |   |   | City               |        | id  |                      |
      |   |   |                    | scope  | bad | address.foo = 'x'    | private
      |   |   |   | id             | string |     |                      | private
      |   |   |   | address        | object |     |                      | private
      |   |   |   | address.street | string |     |                      | private
            """,
            manifest_type=manifest_type,
            tmp_path=tmp_path,
        )


@pytest.mark.manifests("csv")
def test_scope_with_valid_text_property_loads(manifest_type: str, tmp_path: Path, rc: RawConfig):
    context = load_manifest_get_context(
        rc,
        manifest="""
    d | r | b | m | property | type   | ref  | prepare             | access
    example                  |        |      |                     |
      |   |   | City         |        | id   |                     | 
      |   |   |              | scope  | text | name@lt = 'Vilnius' | private
      |   |   |   | id       | string |      |                     | private
      |   |   |   | name@lt  | string |      |                     | private
      |   |   |   | name@en  | string |      |                     | private
        """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )
    city = context.get("store").manifest.get_objects()["model"]["example/City"]
    assert "text" in city.scopes


@pytest.mark.manifests("csv")
def test_scope_with_invalid_text_property_raises(manifest_type: str, tmp_path: Path, rc: RawConfig):
    with pytest.raises((PropertyNotFound, FieldNotInResource)):
        load_manifest_get_context(
            rc,
            manifest="""
    d | r | b | m | property | type   | ref | prepare         | access
    example                  |        |     |                 |
      |   |   | City         |        | id  |                 |
      |   |   |              | scope  | bad | foo = 'x'       | private
      |   |   |   | id       | string |     |                 | private
      |   |   |   | name@lt  | string |     |                 | private
            """,
            manifest_type=manifest_type,
            tmp_path=tmp_path,
        )


@pytest.mark.manifests("csv")
def test_multiple_scopes_on_same_model_load(manifest_type, tmp_path, rc):
    context = load_manifest_get_context(
        rc,
        manifest="""
    d | r | b | m | property | type   | ref  | prepare         | access
    example                  |        |      |                 |
      |   |   | City         |        | id   |                 |
      |   |   |              | scope  | ids  | select(id)      | private
      |   |   |              | scope  | code | code = 'lt'     | private
      |   |   |              | scope  | name | select(name)    | private
      |   |   |   | id       | string |      |                 | private
      |   |   |   | code     | string |      |                 | private
      |   |   |   | name     | string |      |                 | private
        """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )
    city = context.get("store").manifest.get_objects()["model"]["example/City"]
    assert set(city.scopes.keys()) == {"ids", "code", "name"}


@pytest.mark.manifests("csv")
def test_cross_model_scope_3_level_chain_loads(manifest_type, tmp_path, rc):
    context = load_manifest_get_context(
        rc,
        manifest="""
    d | r | b | m | property | type   | ref       | prepare                        | access
    example                  |        |           |                                |
      |   |   | City         |        | id        |                                |
      |   |   |              | scope  | continent | select(country.continent.name) | private
      |   |   |   | id       | string |           |                                | private
      |   |   |   | country  | ref    | Country   |                                | private
      |   |   | Country      |        | id        |                                |
      |   |   |   | id       | string |           |                                | private
      |   |   |   | continent| ref    | Continent |                                | private
      |   |   | Continent    |        | id        |                                |
      |   |   |   | id       | string |           |                                | private
      |   |   |   | name     | string |           |                                | private
        """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )
    city = context.get("store").manifest.get_objects()["model"]["example/City"]
    assert "continent" in city.scopes


@pytest.mark.manifests("csv")
def test_scope_with_invalid_lang_tag_raises(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
):
    with pytest.raises(LangNotDeclared):
        load_manifest_get_context(
            rc,
            manifest="""
    d | r | b | m | property | type   | ref  | prepare              | access
    example                  |        |      |                      |
      |   |   | City         |        | id   |                      |
      |   |   |              | scope  | text | name@fr = 'Vilnius'  | private
      |   |   |   | id       | string |      |                      | private
      |   |   |   | name@lt  | string |      |                      | private
      |   |   |   | name@en  | string |      |                      | private
            """,
            manifest_type=manifest_type,
            tmp_path=tmp_path,
        )


@pytest.mark.manifests("csv")
def test_scope_with_or_invalid_property_raises(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
):
    with pytest.raises((PropertyNotFound, FieldNotInResource)):
        load_manifest_get_context(
            rc,
            manifest="""
    d | r | b | m | property | type   | ref  | prepare                            | access
    example                  |        |      |                                    |
      |   |   | City         |        | id   |                                    |
      |   |   |              | scope  | text | or(name@lt = 'Vilnius', foo = 'x') | private
      |   |   |   | id       | string |      |                                    | private
      |   |   |   | name@lt  | string |      |                                    | private
      |   |   |   | name@en  | string |      |                                    | private
            """,
            manifest_type=manifest_type,
            tmp_path=tmp_path,
        )


@pytest.mark.manifests("csv")
def test_scope_with_and_invalid_lang_tag_raises(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
):
    with pytest.raises(LangNotDeclared):
        load_manifest_get_context(
            rc,
            manifest="""
    d | r | b | m | property | type   | ref  | prepare                                       | access
    example                  |        |      |                                               |
      |   |   | City         |        | id   |                                               |
      |   |   |              | scope  | text | and(name@lt = 'Vilnius', name@xx = 'Vilnius') | private
      |   |   |   | id       | string |      |                                               | private
      |   |   |   | name@lt  | string |      |                                               | private
      |   |   |   | name@en  | string |      |                                               | private
            """,
            manifest_type=manifest_type,
            tmp_path=tmp_path,
        )
