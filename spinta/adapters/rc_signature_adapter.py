from __future__ import annotations

import base64
import subprocess
from typing import TYPE_CHECKING, Dict

from lxml import etree

from spinta.components import Context
from spinta.datasets.backends.dataframe.backends.soap.ufuncs.ufuncs import MakeCDATA

if TYPE_CHECKING:
    from spinta.core.config import RawConfig


def validate_soap_adapter_config(raw_config: RawConfig | None) -> None:
    """Invoked by soap plugin loader when this module is listed in ``soap_adapter_modules``."""
    if raw_config is None:
        raise RuntimeError(
            "RC signature adapter was loaded but application configuration is missing."
        )
    path = raw_config.get("rc_signature", "private_key_path", default=None)
    if not path or not str(path).strip():
        raise RuntimeError(
            "When using the RC signature SOAP adapter, set `rc_signature.private_key_path` in configuration."
        )


def _get_private_key_path(context: Context) -> str:
    rc = context.get("rc")
    path = rc.get("rc_signature", "private_key_path", default=None)
    if not path or not str(path).strip():
        raise RuntimeError(
            "`rc_signature.private_key_path` must be set in configuration for RC request signing."
        )
    return str(path)


def _compute_rc_signature(args: str, key_path: str) -> str:
    """Compute RC-style signature using openssl CLI.
    This mirrors the legacy shell example:
        echo -n "$ARGS" | openssl dgst -sha256 -sign $PRIVATE_KEY_FILE | base64 -w0
    """
    proc = subprocess.run(
        ["openssl", "dgst", "-sha256", "-sign", key_path],
        input=args.encode("utf-8"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )
    sig_b64 = base64.b64encode(proc.stdout).decode("ascii")

    return sig_b64.replace("\r", "").replace("\n", "")


def build_rc_string_to_sign(soap_body: Dict[str, object]) -> str:
    """Build the RC string-to-sign from soap_body (input/ActionType, etc.)."""

    def _get(name: str, default: str = "") -> str:
        value = soap_body.get(f"input/{name}")

        if value is None and isinstance(soap_body.get("input"), dict):
            value = soap_body["input"].get(name)

        if value is None:
            value = soap_body.get(name, default)

        if isinstance(value, MakeCDATA):
            value = value.data

        if isinstance(value, etree.CDATA):
            value = str(value)

        if name == "Parameters" and isinstance(value, str):
            if value.startswith("'") and value.endswith("'") and len(value) >= 2:
                value = value[1:-1]

        return "" if value is None else str(value)

    action_type = _get("ActionType")
    caller_code = _get("CallerCode")
    end_user_info = _get("EndUserInfo", "")
    parameters = _get("Parameters", "")
    time_value = _get("Time")

    return f"{action_type}{caller_code}{end_user_info}{parameters}{time_value}"


def compute_rc_signature_from_body(soap_body: Dict[str, object], context: Context) -> str:
    """Build string-to-sign from soap_body and return base64 signature."""
    key_path = _get_private_key_path(context)
    args = build_rc_string_to_sign(soap_body)
    
    return _compute_rc_signature(args, key_path)


def get_deferred_prepare_names() -> list[str]:
    return ["rc_signature"]


def get_body_resolvers() -> Dict[str, object]:
    def rc_signature_resolver(env, expr=None) -> str:
        return compute_rc_signature_from_body(env.soap_request_body, env.context)

    return {"rc_signature": rc_signature_resolver}
