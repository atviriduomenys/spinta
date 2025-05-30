from spinta.auth import AdminToken
from spinta.backends.helpers import load_query_builder_class
from spinta.commands import get_model
from spinta.core.config import RawConfig
from spinta.core.enums import Mode
from spinta.dimensions.param.components import ResolvedResourceParam
from spinta.testing.manifest import prepare_manifest
from spinta.ufuncs.querybuilder.components import QueryParams


def test_soap_query_builder_has_all_attributes_only_after_init_is_called(rc: RawConfig) -> None:
    context, manifest = prepare_manifest(rc, """
        d | r | b | m | property | type    | ref | source                                          | access | prepare
        example                  | dataset |     |                                                 |        |
          | wsdl_resource        | wsdl    |     | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
          | soap_resource        | soap    |     | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
          |   |   | City         |         | id  | /                                               | open   |
          |   |   |   | id       | integer |     | id                                              |        |
        """, mode=Mode.external)

    config = context.get("config")
    model = get_model(context, manifest, 'example/City')
    load_query_builder_class(config, model.backend)
    query_builder = model.backend.query_builder_class(context)

    query_params = QueryParams()
    new_query_builder = query_builder.init(model.backend, model, {"param1": "1"}, query_params)

    assert query_builder is not new_query_builder
    assert new_query_builder.soap_request_body == {}
    assert new_query_builder.property_values == {}
    assert new_query_builder.params == {"param1": "1"}
    assert new_query_builder.query_params == query_params


def test_soap_query_builder_build_populates_soap_request_body_and_property_values(rc: RawConfig) -> None:
    context, manifest = prepare_manifest(rc, """
        d | r | b | m | property | type    | ref        | source                                          | access | prepare
        example                  | dataset |            |                                                 |        |
          | wsdl_resource        | wsdl    |            | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
          | soap_resource        | soap    |            | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
          |   |   |   |          | param   | parameter1 | request_model/param1                            | open   | input('default_val')
          |   |   | City         |         | id         | /                                               | open   |
          |   |   |   | id       | integer |            | id                                              |        |
          |   |   |   | p1       | integer |            |                                                 |        | param(parameter1)
        """, mode=Mode.external)
    context.set('auth.token', AdminToken())

    config = context.get("config")
    model = get_model(context, manifest, 'example/City')
    load_query_builder_class(config, model.backend)
    query_builder = model.backend.query_builder_class(context)

    resource_params = {
        "parameter1": ResolvedResourceParam(target="parameter1", source="request_model/param1", value="default_value")
    }
    new_query_builder = query_builder.init(model.backend, model, resource_params, QueryParams())

    assert new_query_builder.soap_request_body == {}
    assert new_query_builder.property_values == {}
    new_query_builder.build()
    assert new_query_builder.soap_request_body == {"request_model/param1": "default_value"}
    assert new_query_builder.property_values == {"parameter1": "default_value"}
