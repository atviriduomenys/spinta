from __future__ import annotations

# TODO(adapter): Move this module to a separate adapter repo
# (e.g. spinta-rc-broker-adapter) so Spinta core stays generic.

import base64
import logging
import os
import subprocess
from typing import Tuple, Dict, Optional

from spinta.components import Context, Model
from spinta.datasets.components import Resource

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


def compute_rc_signature_from_body(
    soap_body: Dict[str, object],
    context: Optional[Context] = None,
) -> str | None:
    """Build string-to-sign from soap_body (input/ActionType, etc.) and return signature, or None if disabled/failed.

    Used by both the DSA prepare ufunc rc_signature() and the legacy adapter.
    """
    key_path = _get_poc_private_key_path(context)
    if not key_path:
        log.warning(
            "RC POC: private key path is not configured; signature will be empty. "
            "Set rc_signature.private_key_path in config.yml or RC_POC_PRIVATE_KEY_PATH env var."
        )
        return None

    def _get(name: str, default: str = "") -> str:
        value = soap_body.get(f"input/{name}")
        if value is None:
            value = soap_body.get(name, default)
        # Support nested body: soap_body["input"] = {"ActionType": "...", ...}
        if value is None and isinstance(soap_body.get("input"), dict):
            value = soap_body["input"].get(name, default)
        return "" if value is None else str(value)

    action_type = _get("ActionType")
    caller_code = _get("CallerCode")
    end_user_info = _get("EndUserInfo", "")
    parameters = _get("Parameters", "")
    time_value = _get("Time")

    if not action_type or not caller_code or not time_value:
        return None

    args = f"{action_type}{caller_code}{end_user_info}{parameters}{time_value}"
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


def rc_signature_poc_adapter(
    *,
    context: Context,
    model: Model,
    resource: Resource,
    soap_body: Dict[str, object],
    http_headers: Dict[str, str],
) -> Tuple[Dict[str, object], Dict[str, str]]:
    """POC adapter that generates RC-style Signature for GetData-like calls.

    For now this is intentionally minimal and driven by environment:
    - If RC_POC_PRIVATE_KEY_PATH is not set, adapter is a no-op.
    - It expects ActionType, CallerCode, Parameters, Time to be present in soap_body.
    - EndUserInfo is optional (Mode 1 vs Mode 2).
    """
    signature = compute_rc_signature_from_body(soap_body)
    if signature is None:
        return soap_body, http_headers

    action_type = str(soap_body.get("input/ActionType") or soap_body.get("ActionType") or "")
    caller_code = str(soap_body.get("input/CallerCode") or soap_body.get("CallerCode") or "")
    end_user_info = str(soap_body.get("input/EndUserInfo") or soap_body.get("EndUserInfo") or "")
    parameters = str(soap_body.get("input/Parameters") or soap_body.get("Parameters") or "")
    time_value = str(soap_body.get("input/Time") or soap_body.get("Time") or "")

    log.warning(
        "RC POC signature generated | action_type=%r caller_code=%r end_user_info=%r parameters=%r time=%r signature=%s",
        action_type,
        caller_code,
        end_user_info,
        parameters,
        time_value,
        signature,
    )

    # Write Signature back into soap_body; prefer nested key if present.
    if "input/Signature" in soap_body:
        soap_body["input/Signature"] = signature
    else:
        soap_body["Signature"] = signature

    return soap_body, http_headers


def apply_soap_adapters(
    *,
    context: Context,
    model: Model,
    resource: Resource,
    soap_body: Dict[str, object],
    http_headers: Dict[str, str],
) -> Tuple[Dict[str, object], Dict[str, str]]:
    """Apply SOAP request adapters.

    POC implementation: always run the RC signature POC adapter when a
    private key path is configured. Later this can be extended to load
    adapters from config/manifest.
    """
    return rc_signature_poc_adapter(
        context=context,
        model=model,
        resource=resource,
        soap_body=soap_body,
        http_headers=http_headers,
    )
