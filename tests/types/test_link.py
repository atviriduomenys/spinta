from pathlib import Path

import pytest

from spinta.core.config import RawConfig
from spinta.exceptions import MissingRefModel
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
