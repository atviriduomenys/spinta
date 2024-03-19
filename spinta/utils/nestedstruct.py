from typing import Dict, Union
from typing import Iterator
from typing import List
from typing import Optional
from typing import Set

import itertools

from spinta.components import Model, Property
from spinta.types.datatype import DataType


def get_separated_name(parent: DataType, parent_name: str, child_name: str):
    return f'{parent_name}{parent.tabular_separator}{child_name}'


def flatten(value, parent: Union[Model, Property] = None, separator: str = '.', omit_none: bool = True):
    value, lists = _flatten(value, parent, separator, omit_none=omit_none)

    if value is None:
        for k, vals in lists:
            for v in vals:
                if v is not None or not omit_none:
                    yield from flatten(v, parent, separator, omit_none=omit_none)

    elif lists:
        keys, lists = zip(*lists)
        for vals in itertools.product(*lists):
            val = {
                k: v
                for k, v in zip(keys, vals) if v is not None or not omit_none
            }
            val.update(value)
            yield from flatten(val, parent, separator, omit_none=omit_none)

    else:
        yield value


def _combine_with_separator(parent: Union[Model, Property], parent_name: str, child_name: str, seperator: str):
    return_name = parent_name
    return_parent = parent

    if not return_parent:
        return_name = seperator.join([parent_name, child_name] if parent_name else [child_name])
        return return_name, return_parent

    if not parent_name:
        return_name = child_name
        if isinstance(parent, Model) and return_name in parent.properties:
            return_parent = parent.properties[return_name]

        return return_name, return_parent

    if isinstance(parent, Property):
        parent_dtype = parent.dtype
        return_name = get_separated_name(parent_dtype, parent_name, child_name)
        return_parent = parent_dtype.get_child(child_name)
    return return_name, return_parent


def _flatten(value, parent: Union[Model, Property], separator: str, key: str = '', omit_none: bool = True):
    if isinstance(value, dict):
        data = {}
        lists = []
        for k, v in value.items():
            if v is not None or not omit_none:
                new_name, new_parent = _combine_with_separator(parent, key, k, separator)
                v, more = _flatten(v, new_parent, separator, new_name, omit_none=omit_none)
                data.update(v or {})
                lists += more
        return data, lists

    elif isinstance(value, (list, Iterator)):
        if value:
            if key:
                key = f'{key}[]'
            return None, [(key, value)] if value is not None or not omit_none else []
        else:
            return None, []

    else:
        return {key: value} if value is not None or not omit_none else {}, []


def build_select_tree(select: List[str]) -> Dict[str, Set[Optional[str]]]:
    tree: Dict[str, Set[Optional[str]]] = {}
    for name in select:
        split = name, None
        while len(split) == 2:
            name, node = split
            if name not in tree:
                tree[name] = set()
            if node:
                tree[name].add(node)
            split = name.rsplit('.', 1)
    return tree


def flat_dicts_to_nested(value):
    res = {}
    for k, v in dict(value).items():
        names = k.split('.')
        vref = res
        for name in names[:-1]:
            if name not in vref:
                vref[name] = {}
            vref = vref[name]
        if names[-1] in vref:
            target_dict = vref[names[-1]]
            target_dict.update(v)
        else:
            vref[names[-1]] = v
    return res


def flatten_value(value, sep=".", key=()):
    value, _ = _flatten(value, sep, key)
    return value
