import base64
import subprocess

from lxml import etree

from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.core.ufuncs import Expr
from spinta.datasets.backends.dataframe.backends.soap.ufuncs.components import SoapQueryBuilder
from spinta.datasets.backends.dataframe.backends.soap.ufuncs.ufuncs import MakeCDATA

RC_ACTION_TYPE_FIELD = "ActionType"
RC_CALLER_CODE_FIELD = "CallerCode"
RC_END_USER_INFO_FIELD = "EndUserInfo"
RC_PARAMETERS_FIELD = "Parameters"
RC_TIME_FIELD = "Time"


def is_quoted(value: str) -> bool:
    """True if ``value`` has matching outer ``'`` or ``"`` quotes (Katalogas ``is_quoted``)."""
    return len(value) > 1 and value[0] == value[-1] and value[0] in ('"', "'")


def _require_rc_private_key_path(raw_config: RawConfig | None) -> str:
    """Return the configured PEM path, or raise if raw config / key path is missing."""
    if raw_config is None:
        raise RuntimeError("RC signature adapter was loaded but application configuration is missing.")
    path = raw_config.get("rc_signature", "private_key_path", default=None)
    if not path or not str(path).strip():
        raise RuntimeError(
            "When using the RC signature SOAP adapter, set `rc_signature.private_key_path` in configuration."
        )
    return str(path)


def validate_soap_adapter_config(raw_config: RawConfig | None) -> None:
    """Invoked by soap plugin loader when this module is listed in ``soap_adapter_modules``."""
    _require_rc_private_key_path(raw_config)


def _get_private_key_path(context: Context) -> str:
    return _require_rc_private_key_path(context.get("rc"))


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


def build_rc_string_to_sign(soap_body: dict[str, object]) -> str:
    """Return the plaintext that RC signs and verifies for SOAP calls.

    The broker does not sign the whole XML; it expects an RSA-SHA256 signature over one
    concatenated string, in this exact order with no delimiters: ActionType, CallerCode,
    EndUserInfo, Parameters, Time. The server rebuilds the same string and checks the
    signature, so we must assemble identical pieces from `soap_body`.
    """

    input_block = soap_body.get("input")
    input_dict = input_block if isinstance(input_block, dict) else None

    def _get(name: str, default: str = "") -> str:
        value = soap_body.get(f"input/{name}")
        if value is None and input_dict is not None:
            value = input_dict.get(name)

        if value is None:
            value = soap_body.get(name, default)

        if isinstance(value, MakeCDATA):
            value = value.data

        if isinstance(value, etree.CDATA):
            value = str(value)

        if name == RC_PARAMETERS_FIELD and isinstance(value, str) and is_quoted(value):
            value = value[1:-1]

        return "" if value is None else str(value)

    action_type = _get(RC_ACTION_TYPE_FIELD)
    caller_code = _get(RC_CALLER_CODE_FIELD)
    end_user_info = _get(RC_END_USER_INFO_FIELD, "")
    parameters = _get(RC_PARAMETERS_FIELD, "")
    time_value = _get(RC_TIME_FIELD)

    return f"{action_type}{caller_code}{end_user_info}{parameters}{time_value}"


def compute_rc_signature_from_body(soap_body: dict[str, object], context: Context) -> str:
    """Build string-to-sign from soap_body and return base64 signature."""
    key_path = _get_private_key_path(context)
    args = build_rc_string_to_sign(soap_body)

    return _compute_rc_signature(args, key_path)


def get_deferred_prepare_names() -> list[str]:
    return ["rc_signature"]


def get_body_resolvers() -> dict[str, object]:
    def rc_signature_resolver(env: SoapQueryBuilder, expr: Expr | None = None) -> str:
        return compute_rc_signature_from_body(env.soap_request_body, env.context)

    return {"rc_signature": rc_signature_resolver}
