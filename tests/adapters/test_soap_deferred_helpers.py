from types import SimpleNamespace

import pytest

from spinta.core.ufuncs import Expr
from spinta.datasets.components import Param
from spinta.datasets.backends.dataframe.backends.soap.ufuncs.ufuncs import (
    _param_for_soap_body_key,
    _property_value_key_for_deferred,
)

# Shared Expr so parametrized ``soap_body`` dicts and ``expr=`` refer to the same object (expr-identity path).
_DEFERRED_RC_SIGNATURE = Expr("rc_signature")


def _param(name: str, soap_body: dict, soap_body_value_type: str = "string") -> Param:
    param = Param()
    param.name = name
    param.soap_body = soap_body
    param.soap_body_value_type = soap_body_value_type
    return param


class TestPropertyValueKeyForDeferred:
    def test_uses_manifest_param_name_when_param_matched(self) -> None:
        matched = _param("signature", {"input/Signature": Expr("rc_signature")})
        assert _property_value_key_for_deferred(matched, "input/Signature") == "signature"

    def test_fallback_to_last_path_segment_lower_when_param_unknown(self) -> None:
        assert _property_value_key_for_deferred(None, "input/Signature") == "signature"

    def test_fallback_to_whole_source_lower_when_no_slash(self) -> None:
        assert _property_value_key_for_deferred(None, "Signature") == "signature"


class TestParamForSoapBodyKey:
    """`_param_for_soap_body_key` maps a flat SOAP body key (e.g. ``input/Signature``) to the manifest ``Param``.

    Deferred resolution needs that ``Param`` for ``soap_body_value_type`` (CDATA) and for ``property_values`` keys.
    The helper tries, in order: exact key in ``param.soap_body``, same key ignoring case, then the same ``Expr``
    instance as ``expr=`` when keys do not line up.
    """

    @pytest.mark.parametrize(
        "soap_body, lookup, expr",
        [
            ({"input/Signature": _DEFERRED_RC_SIGNATURE}, "input/Signature", None),
            ({"INPUT/SIGNATURE": _DEFERRED_RC_SIGNATURE}, "input/signature", None),
            ({"other/path": _DEFERRED_RC_SIGNATURE}, "input/Signature", _DEFERRED_RC_SIGNATURE),
        ],
    )
    def test_finds_param_for_soap_body_key(
        self,
        soap_body: dict,
        lookup: str,
        expr: Expr | None,
    ) -> None:
        signature_param = _param("signature", soap_body)
        env = SimpleNamespace(params={"signature": signature_param})
        assert _param_for_soap_body_key(env, lookup, expr=expr) is signature_param

    def test_returns_none_when_no_param_defines_slot(self) -> None:
        other = _param("other", {"input/Other": "x"})
        env = SimpleNamespace(params={"other": other})
        assert _param_for_soap_body_key(env, "input/Missing") is None

    def test_skips_params_with_empty_soap_body(self) -> None:
        empty = Param()
        empty.name = "empty"
        empty.soap_body = {}
        other = _param("x", {"input/X": "v"})
        env = SimpleNamespace(params={"empty": empty, "x": other})
        assert _param_for_soap_body_key(env, "input/X") is other
