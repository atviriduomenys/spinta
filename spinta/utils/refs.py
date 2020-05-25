import datetime
import hashlib

import msgpack


def get_ref_id(value):
    # XXX: deprecated use spinta.datasets.helpers.encode_primary_key
    if isinstance(value, list):
        value = list(value)
    else:
        value = [value]

    if any(v is None for v in value):
        return

    for i, v in enumerate(value):
        if isinstance(v, (datetime.datetime, datetime.date)):
            value[i] = v.isoformat()

    key = msgpack.dumps(value, use_bin_type=True)
    key = hashlib.sha1(key).hexdigest()

    return key
