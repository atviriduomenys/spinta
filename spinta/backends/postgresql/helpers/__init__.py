import hashlib

from spinta.components import Property
from spinta.backends.postgresql.constants import NAMEDATALEN


def get_column_name(prop: Property, replace: bool = False) -> str:
    if prop.list:
        if prop.place == prop.list.place:
            return prop.list.name
        else:
            return prop.place[len(prop.list.place) + 1:]
    else:
        parent = prop.parent
        if replace and parent and isinstance(parent, Property):
            if prop.place.startswith(parent.place):
                name = prop.place.replace(parent.place, '')
                if name.startswith('.'):
                    name = name[1:]
                return name
        return prop.place


def get_pg_name(name: str) -> str:
    if len(name) > NAMEDATALEN:
        hs = 8
        h = hashlib.sha1(name.encode()).hexdigest()[:hs]
        i = int(NAMEDATALEN / 100 * 60)
        j = NAMEDATALEN - i - hs - 2
        name = name[:i] + '_' + h + '_' + name[-j:]
    return name


def get_pg_sequence_name(name: str) -> str:
    suffix = '__id_seq'
    slen = len(suffix)
    nlen = len(name)
    if nlen + slen > NAMEDATALEN:
        trim = (nlen + slen) - NAMEDATALEN
        name = name[:-trim]
    return name + suffix
