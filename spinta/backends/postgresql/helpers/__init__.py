from typing import Union

import hashlib

from spinta.components import Model, Property
from spinta.backends.postgresql.constants import TableType
from spinta.backends.postgresql.constants import NAMEDATALEN


def get_table_name(
    node: Union[Model, Property],
    ttype: TableType = TableType.MAIN,
) -> str:
    if isinstance(node, Model):
        model = node
    else:
        model = node.model
    if ttype in (TableType.LIST, TableType.FILE):
        name = model.model_type() + ttype.value + '/' + node.place
    else:
        name = model.model_type() + ttype.value
    return name


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


def flat_dicts_to_nested(value):
    res = {}
    for k, v in dict(value).items():
        names = k.split('.')
        vref = res
        for name in names[:-1]:
            if name not in vref:
                vref[name] = {}
            vref = vref[name]
        vref[names[-1]] = v
    return res
