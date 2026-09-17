import base64
import json
from typing import Any

from cbor2 import dumps as cbor_dumps

from spinta.utils.json import fix_data_for_json


def encode_base32(value: Any) -> str:
    """Identifier of a model keyed by `base32`, as its data is read.

    A composite key is written as CBOR first, so that the parts stay apart, and
    the padding is dropped, because the value is given in a path.
    """
    if isinstance(value, (list, tuple)):
        data = cbor_dumps(list(value))
    else:
        data = str(value).encode("utf-8")
    return base64.b32encode(data).rstrip(b"=").decode("utf-8")


def is_url_safe_base64(s):
    try:
        return base64.urlsafe_b64encode(base64.urlsafe_b64decode(s)) == s
    except Exception:
        return False


def encode_page_values(values: list):
    return base64.urlsafe_b64encode(json.dumps(fix_data_for_json(values)).encode("ascii"))
