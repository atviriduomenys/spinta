from pathlib import Path

import pytest

from spinta.auth import AdminToken
from spinta.backends.helpers import load_query_builder_class
from spinta.commands import get_model
from spinta.core.config import RawConfig
from spinta.core.enums import Mode
from spinta.testing.manifest import prepare_manifest
from spinta.ufuncs.querybuilder.components import QueryParams

import spinta.adapters.rc.signature_adapter as rc_adapter


def _rc_adapter_path() -> Path:
    return Path(rc_adapter.__file__).resolve()


def _rc_test_private_key_path() -> Path:
    return Path(__file__).resolve().parent / "helpers" / "rc_test_private_key.pem"


@pytest.fixture()
def rc_with_rc_signature_adapter(rc: RawConfig) -> RawConfig:
    """Fork session config and register the RC adapter module + key path (like ``config.yml``)."""
    key_path = _rc_test_private_key_path()
    if not key_path.is_file():
        pytest.skip(f"Test private key not found at {key_path}")
    return rc.fork(
        {
            "soap_adapter_modules": [str(_rc_adapter_path())],
            "rc_signature": {"private_key_path": str(key_path)},
        }
    )


def test_dsa_soap_prepare_rc_signature_deferred_resolves_after_body_is_built(
    rc_with_rc_signature_adapter: RawConfig,
) -> None:
    """Manifest param ``prepare`` uses ``rc_signature()``; builder leaves Expr until body is complete, then signs."""
    action_type = "17"
    caller_code = "TEST"
    parameters = "<p/>"
    time_value = "2026-01-01 00:00:00"
    expected_args = f"{action_type}{caller_code}{parameters}{time_value}"
    expected_signature = rc_adapter._compute_rc_signature(
        expected_args,
        str(_rc_test_private_key_path()),
    )

    context, manifest = prepare_manifest(
        rc_with_rc_signature_adapter,
        """
        d | r | b | m | property | type    | ref        | source                                          | access | prepare
        example                  | dataset |            |                                                 |        |
          | wsdl_resource        | wsdl    |            | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
          | soap_resource        | soap    |            | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
          |   |   |   |          | param   | action_type | input/ActionType                               | open   | input('17')
          |   |   |   |          | param   | caller_code | input/CallerCode                               | open   | input('TEST')
          |   |   |   |          | param   | end_user    | input/EndUserInfo                              | open   | input('')
          |   |   |   |          | param   | parameters  | input/Parameters                               | open   | input('<p/>')
          |   |   |   |          | param   | time_val    | input/Time                                     | open   | input('2026-01-01 00:00:00')
          |   |   |   |          | param   | signature   | input/Signature                                | open   | rc_signature()
          |   |   | City         |         | id         | /                                               | open   |
          |   |   |   | id       | integer |            | id                                              |        |
          |   |   |   | sig      | string  |            |                                                 |        | param(signature)
        """,
        mode=Mode.external,
    )
    context.set("auth.token", AdminToken())

    model = get_model(context, manifest, "example/City")
    load_query_builder_class(context.get("config"), model.backend)
    query_builder = model.backend.query_builder_class(context)
    builder = query_builder.init(model.backend, model, QueryParams())

    builder.build()

    assert builder.soap_request_body["input/ActionType"] == action_type
    assert builder.soap_request_body["input/CallerCode"] == caller_code
    assert builder.soap_request_body["input/EndUserInfo"] == ""
    assert builder.soap_request_body["input/Parameters"] == parameters
    assert builder.soap_request_body["input/Time"] == time_value
    assert rc_adapter.build_rc_string_to_sign(dict(builder.soap_request_body)) == expected_args
    assert builder.soap_request_body["input/Signature"] == expected_signature
    assert builder.property_values.get("signature") == expected_signature
