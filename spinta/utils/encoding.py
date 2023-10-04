import base64
import json

from spinta.utils.json import fix_data_for_json


def is_url_safe_base64(s):
    try:
        return base64.urlsafe_b64encode(base64.urlsafe_b64decode(s)) == s
    except Exception:
        return False


def encode_page_values(values: list):
    return base64.urlsafe_b64encode(json.dumps(fix_data_for_json(values)).encode('ascii'))
