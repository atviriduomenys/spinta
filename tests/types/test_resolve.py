from spinta import commands
from spinta.core.config import RawConfig
from spinta.testing.manifest import load_manifest_and_context


def test_resolve_property_basic(rc: RawConfig):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property | type    | ref | source | prepare | access | level
    example                  |         |     |        |         |        |
      |   |   | City         |         | id  |        |         | open   |
      |   |   |   | id       | integer |     |        |         |        |
      |   |   |   | name     | string  |     |        |         |        |
    """,
    )
    model = commands.get_model(context, manifest, "example/City")
    target_prop = model.flatprops["name"]
    prop = commands.resolve_property(model, "name")
    assert prop == target_prop


def test_resolve_property_object_basic(rc: RawConfig):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property | type    | ref | source | prepare | access | level
    example                  |         |     |        |         |        |
      |   |   | City         |         | id  |        |         | open   |
      |   |   |   | id       | integer |     |        |         |        |
      |   |   |   | obj      | object  |     |        |         |        |
      |   |   |   | obj.name | string  |     |        |         |        |
    """,
    )
    model = commands.get_model(context, manifest, "example/City")
    target_prop = model.flatprops["obj.name"]
    prop = commands.resolve_property(model, "obj.name")
    assert prop == target_prop


def test_resolve_property_ref_overwrite_basic(rc: RawConfig):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property     | type    | ref     | source | prepare | access | level
    example                      |         |         |        |         |        |
      |   |   | Country          |         | id      |        |         | open   |
      |   |   |   | id           | integer |         |        |         |        |
      |   |   |   | name         | string  |         |        |         |        |
                                 |         |         |        |         |        |
      |   |   | City             |         | id      |        |         | open   |
      |   |   |   | id           | integer |         |        |         |        |
      |   |   |   | country      | ref     | Country |        |         |        |
      |   |   |   | country.name | string  |         |        |         |        |
    """,
    )
    model = commands.get_model(context, manifest, "example/City")
    target_prop = model.flatprops["country.name"]
    prop = commands.resolve_property(model, "country.name")
    assert prop == target_prop


def test_resolve_property_ref_denorm_basic(rc: RawConfig):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property     | type    | ref     | source | prepare | access | level
    example                      |         |         |        |         |        |
      |   |   | Country          |         | id      |        |         | open   |
      |   |   |   | id           | integer |         |        |         |        |
      |   |   |   | name         | string  |         |        |         |        |
                                 |         |         |        |         |        |
      |   |   | City             |         | id      |        |         | open   |
      |   |   |   | id           | integer |         |        |         |        |
      |   |   |   | country      | ref     | Country |        |         |        |
      |   |   |   | country.name |         |         |        |         |        |
    """,
    )
    model = commands.get_model(context, manifest, "example/City")
    target_prop = model.flatprops["country.name"]
    prop = commands.resolve_property(model, "country.name")
    assert prop == target_prop


def test_resolve_property_ref_denorm_from_ref(rc: RawConfig):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property     | type    | ref     | source | prepare | access | level
    example                      |         |         |        |         |        |
      |   |   | Country          |         | id      |        |         | open   |
      |   |   |   | id           | integer |         |        |         |        |
      |   |   |   | name         | string  |         |        |         |        |
                                 |         |         |        |         |        |
      |   |   | City             |         | id      |        |         | open   |
      |   |   |   | id           | integer |         |        |         |        |
      |   |   |   | country      | ref     | Country |        |         |        |
    """,
    )
    country_model = commands.get_model(context, manifest, "example/Country")
    city_model = commands.get_model(context, manifest, "example/City")
    target_prop = country_model.flatprops["name"]
    prop = commands.resolve_property(city_model, "country.name")
    assert prop == target_prop


def test_resolve_property_nested_ref_denorm(rc: RawConfig):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property            | type    | ref     | source | prepare | access | level
    example                             |         |         |        |         |        |
      |   |   | Planet                  |         | id      |        |         | open   |
      |   |   |   | id                  | integer |         |        |         |        |
      |   |   |   | name                | string  |         |        |         |        |
                                        |         |         |        |         |        |
      |   |   | Country                 |         | id      |        |         | open   |
      |   |   |   | id                  | integer |         |        |         |        |
      |   |   |   | name                | string  |         |        |         |        |
      |   |   |   | planet              | ref     | Planet  |        |         |        |
      |   |   |   | planet.name         | string  |         |        |         |        |
      |   |   |   | planet.new          | string  |         |        |         |        |
                                        |         |         |        |         |        |
      |   |   | City                    |         | id      |        |         | open   |
      |   |   |   | id                  | integer |         |        |         |        |
      |   |   |   | country             | ref     | Country |        |         |        |
      |   |   |   | country.name        |         |         |        |         |        |
      |   |   |   | country.planet      | ref     | Planet  |        |         |        |
      |   |   |   | country.planet.name | string  |         |        |         |        |
    """,
    )
    planet_model = commands.get_model(context, manifest, "example/Planet")
    country_model = commands.get_model(context, manifest, "example/Country")
    city_model = commands.get_model(context, manifest, "example/City")

    # Assert country.name
    # Should be from City
    target_prop = city_model.flatprops["country.name"]
    prop = commands.resolve_property(city_model, "country.name")
    assert prop == target_prop

    # Assert country.id
    # Should be from Country
    target_prop = country_model.flatprops["id"]
    prop = commands.resolve_property(city_model, "country.id")
    assert prop == target_prop

    # Assert country.planet.id
    # Should be from Planet
    target_prop = planet_model.flatprops["id"]
    prop = commands.resolve_property(city_model, "country.planet.id")
    assert prop == target_prop

    # Assert country.planet.name
    # Should be from City
    target_prop = city_model.flatprops["country.planet.name"]
    prop = commands.resolve_property(city_model, "country.planet.name")
    assert prop == target_prop

    # Assert country.planet.new
    # Should be from Country
    target_prop = country_model.flatprops["planet.new"]
    prop = commands.resolve_property(city_model, "country.planet.new")
    assert prop == target_prop


def test_resolve_property_base_basic(rc: RawConfig):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property | type    | ref | source | prepare | access | level
    example                  |         |     |        |         |        |
      |   |   | Node         |         | id  |        |         | open   |
      |   |   |   | id       | integer |     |        |         |        |
      |   |   |   | name     | string  |     |        |         |        |
                             |         |     |        |         |        |
      |   | Node             |         |     |        |         |        |
      |   |   | City         |         | id  |        |         | open   |
      |   |   |   | id       | integer |     |        |         |        |
    """,
    )
    node_model = commands.get_model(context, manifest, "example/Node")
    city_model = commands.get_model(context, manifest, "example/City")

    # Assert id
    # Should come from City, since it declared it
    target_prop = city_model.flatprops["id"]
    prop = commands.resolve_property(city_model, "id")
    assert prop == target_prop

    # Assert name
    # Should come from Node, since City does not have it, but its base does
    target_prop = node_model.flatprops["name"]
    prop = commands.resolve_property(city_model, "name")
    assert prop == target_prop


def test_resolve_property_base_ref_overwrite(rc: RawConfig):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property     | type    | ref     | source | prepare | access | level
    example                      |         |         |        |         |        |
      |   |   | Country          |         | id      |        |         | open   |
      |   |   |   | id           | integer |         |        |         |        |
      |   |   |   | name         | string  |         |        |         |        |
      |   |   |   | uuid         | string  |         |        |         |        |
                                 |         |         |        |         |        |
      |   |   | Node             |         | id      |        |         | open   |
      |   |   |   | id           | integer |         |        |         |        |
      |   |   |   | name         | string  |         |        |         |        |
      |   |   |   | country      | ref     | Country |        |         |        |
      |   |   |   | country.id   |         |         |        |         |        |
      |   |   |   | country.name | string  |         |        |         |        |
                                 |         |         |        |         |        |
      |   | Node                 |         |         |        |         |        |
      |   |   | City             |         | id      |        |         | open   |
      |   |   |   | id           | integer |         |        |         |        |
      |   |   |   | country      | ref     | Country |        |         |        |
      |   |   |   | country.name | string  |         |        |         |        |
    """,
    )
    node_model = commands.get_model(context, manifest, "example/Node")
    city_model = commands.get_model(context, manifest, "example/City")
    country_model = commands.get_model(context, manifest, "example/Country")

    # Assert country.id
    # Should come from Node, since it declared it as Denorm
    target_prop = node_model.flatprops["country.id"]
    prop = commands.resolve_property(city_model, "country.id")
    assert prop == target_prop

    # Assert country.name
    # Should come from City, since it overwrote it
    target_prop = city_model.flatprops["country.name"]
    prop = commands.resolve_property(city_model, "country.name")
    assert prop == target_prop

    # Assert country.uuid
    # Should come from Country, since noone declared or overwrote it, but it still exists
    target_prop = country_model.flatprops["uuid"]
    prop = commands.resolve_property(city_model, "country.uuid")
    assert prop == target_prop
