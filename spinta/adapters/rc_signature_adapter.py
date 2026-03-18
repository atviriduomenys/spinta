from __future__ import annotations

import base64
import logging
import os
import subprocess
from typing import Dict, Optional

from lxml import etree

from spinta.components import Context
from spinta.datasets.backends.dataframe.backends.soap.ufuncs.ufuncs import MakeCDATA

log = logging.getLogger(__name__)


def _get_poc_private_key_path(context: Optional[Context] = None) -> str | None:
    """Return path to private key used for POC signing.

    Lookup order:
    1. config.yml: rc_signature.private_key_path
    2. Environment variable: RC_POC_PRIVATE_KEY_PATH

    If neither is set, the adapter becomes a no-op.
    """
    if context is not None:
        try:
            rc = context.get("rc")
            config_path = rc.get("rc_signature", "private_key_path", default=None)
            if config_path:
                return config_path
        except Exception:
            pass
    return os.getenv("RC_POC_PRIVATE_KEY_PATH")


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
    # Strip any CR/LF characters as per RC contract examples.
    return sig_b64.replace("\r", "").replace("\n", "")


def build_rc_poc_string_to_sign(soap_body: Dict[str, object]) -> Optional[str]:
    """Build the RC POC string-to-sign from soap_body (input/ActionType, etc.)."""

    def _get(name: str, default: str = "") -> str:
        # Prefer the "input/Name" key if present.
        value = soap_body.get(f"input/{name}")

        # Support nested body: soap_body["input"] = {"ActionType": "...", ...}
        if value is None and isinstance(soap_body.get("input"), dict):
            value = soap_body["input"].get(name)

        # Fallback to top-level key, then default.
        if value is None:
            value = soap_body.get(name, default)

        # For signing we must use the original string, not wrappers/objects.
        # 1) Unwrap MakeCDATA helper (used before zeep serialization).
        if isinstance(value, MakeCDATA):
            value = value.data

        # 2) Unwrap lxml CDATA objects.
        if isinstance(value, etree.CDATA):
            value = str(value)

        # 3) Parameters may be quoted RQL literals; strip surrounding single quotes.
        if name == "Parameters" and isinstance(value, str):
            if value.startswith("'") and value.endswith("'") and len(value) >= 2:
                value = value[1:-1]

        return "" if value is None else str(value)

    action_type = _get("ActionType")
    caller_code = _get("CallerCode")
    end_user_info = _get("EndUserInfo", "")
    parameters = _get("Parameters", "")
    time_value = _get("Time")

    # Even if some components are empty, return the concatenation; validation is handled elsewhere.
    return f"{action_type}{caller_code}{end_user_info}{parameters}{time_value}"


def compute_rc_signature_from_body(
    soap_body: Dict[str, object],
    context: Optional[Context] = None,
) -> str | None:
    """Build string-to-sign from soap_body and return signature, or None if disabled/failed.

    Used by both the DSA prepare ufunc rc_signature() and the legacy adapter.
    """
    key_path = _get_poc_private_key_path(context)
    if not key_path:
        log.warning(
            "RC POC: private key path is not configured; signature will be empty. "
            "Set rc_signature.private_key_path in config.yml or RC_POC_PRIVATE_KEY_PATH env var."
        )
        return None

    args = build_rc_poc_string_to_sign(soap_body)
    if args is None:
        return None

    try:
        return _compute_rc_signature(args, key_path)
    except Exception as exc:  # pragma: no cover - POC logging only
        log.warning("RC POC signature generation failed: %s", exc)
        return None


def get_deferred_prepare_names() -> list[str]:
    """Entry point: names of prepare expressions resolved at SOAP body build time."""
    return ["rc_signature"]


def get_body_resolvers() -> Dict[str, object]:
    """Entry point: name -> (env, expr) -> value for SOAP body Expr resolution."""

    def rc_signature_resolver(env, expr=None) -> str:
        result = compute_rc_signature_from_body(env.soap_request_body, env.context)
        return result if result else ""

    return {"rc_signature": rc_signature_resolver}
