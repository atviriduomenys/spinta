import base64
import json


def is_url_safe_base64(s):
    try:
        return base64.urlsafe_b64encode(base64.urlsafe_b64decode(s)) == s
    except Exception:
        return False


def encode_page_values(values: list):
    return base64.urlsafe_b64encode(json.dumps(values).encode('ascii'))
