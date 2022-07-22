import hashlib

from spinta.components import Property
from spinta.backends.postgresql.constants import NAMEDATALEN


def get_column_name(prop: Property) -> str:
    if prop.list:
        if prop.place == prop.list.place:
            return prop.list.name
        else:
            return prop.place[len(prop.list.place) + 1:]
    else:
        return prop.place


def get_pg_name(name: str) -> str:
    if len(name) > NAMEDATALEN:
        hs = 8
        h = hashlib.sha1(name.encode()).hexdigest()[:hs]
        i = int(NAMEDATALEN / 100 * 60)
        j = NAMEDATALEN - i - hs - 2
        name = name[:i] + '_' + h + '_' + name[-j:]
    return name
