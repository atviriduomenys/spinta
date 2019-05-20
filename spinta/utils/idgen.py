import hashlib
import os


def get_new_id(resource_type, random_field=None):
    type_field = hashlib.sha512(resource_type.encode('UTF-8')).hexdigest()[:4]
    random_field = random_field or os.urandom(16).hex()
    checksum_field = hashlib.sha512((type_field + random_field).encode()).hexdigest()[:8]
    return '{}-{}-{}'.format(type_field, random_field, checksum_field)
