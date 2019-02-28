import hashlib
import msgpack


def get_ref_id(value):
    key = None
    if isinstance(value, list):
        if all(v is not None for v in value):
            key = msgpack.dumps(value, use_bin_type=True)
            key = hashlib.sha1(key).hexdigest()
    else:
        key = value
    return key
