from multipledispatch import dispatch
from typing import Dict, Union, Any, Callable, Tuple
from typing import Iterator
from typing import List
from typing import Optional
from typing import Set

import itertools

from spinta import commands
from spinta.components import Model, Property
from spinta.types.datatype import DataType, Ref, Partial, Object
from spinta.types.text.components import Text


SEP_GETTER_TYPE = Callable[[str], Tuple[Callable, str]]


@dispatch(type(None))
def _get_seperator(target):
    return None


@dispatch((Model, DataType))
def _get_seperator(target):
    return '.'


@dispatch(Property)
def _get_seperator(target: Property):
    return _get_seperator(target.dtype)


@dispatch(Text)
def _get_seperator(target: Text):
    return '@'


@dispatch((Model, Object, Partial), str)
def _get_child(parent: Union[Model, Object, Partial], target: str):
    if target in parent.properties:
        return parent.properties[target]


@dispatch(Property, str)
def _get_child(parent: Property, target: str):
    return _get_child(parent.dtype, target)


@dispatch(DataType, str)
def _get_child(parent: DataType, target: str):
    return None


@dispatch(type(None), str)
def _get_child(parent, target):
    return None


@dispatch(Ref, str)
def _get_child(parent: Ref, name: str):
    for refprop in parent.refprops:
        if refprop.name == name:
            return refprop

    if name in parent.properties:
        return parent.properties[name]

    if commands.identifiable(parent.prop) and name == '_id':
        return parent.model.properties['_id']

    if parent.model.external and parent.model.external.unknown_primary_key:
        return parent.model.properties['_id']


@dispatch(Text, str)
def _get_child(parent: Text, name: str):
    if name in parent.langs:
        return parent.langs[name]


def sepgetter(parent: Any = None, default_seperator='.') -> SEP_GETTER_TYPE:
    def _sepgetter(target: str):
        child = _get_child(parent, target)
        seperator = _get_seperator(parent)
        if seperator is None:
            seperator = default_seperator
        return sepgetter(child, default_seperator=default_seperator), seperator

    return _sepgetter


@dispatch(DataType, str, str)
def get_separated_name(parent: DataType, parent_name: str, child_name: str):
    seperator = _get_seperator(parent)
    return get_separated_name(seperator, parent_name, child_name)


@dispatch(str, str, str)
def get_separated_name(seperator: str, parent_name: str, child_name: str):
    if parent_name:
        return f'{parent_name}{seperator}{child_name}'
    return child_name


def flatten(value, sep_getter: SEP_GETTER_TYPE = sepgetter(), omit_none: bool = True):
    value, lists = _flatten(value, sep_getter, omit_none=omit_none)

    if value is None:
        for k, vals in lists:
            for v in vals:
                if v is not None or not omit_none:
                    yield from flatten(v, sep_getter, omit_none=omit_none)

    elif lists:
        keys, lists = zip(*lists)
        for vals in itertools.product(*lists):
            val = {
                k: v
                for k, v in zip(keys, vals) if v is not None or not omit_none
            }
            val.update(value)
            yield from flatten(val, sep_getter, omit_none=omit_none)

    else:
        yield value


def _flatten(value, sep_getter: SEP_GETTER_TYPE, key: str = '', omit_none: bool = True):
    if isinstance(value, dict):
        data = {}
        lists = []
        for k, v in value.items():
            if v is not None or not omit_none:
                new_sep_getter, seperator = sep_getter(k)
                new_name = get_separated_name(seperator, key, k)
                v, more = _flatten(v, new_sep_getter, new_name, omit_none=omit_none)
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


def flat_dicts_to_nested(value, list_keys: list = None):
    if list_keys is None:
        list_keys = []
    res = {}

    def recursive_nesting(data, res_, keys: list, depth: int):
        if depth >= len(keys):
            return

        key = keys[depth]
        place = '.'.join(keys[:depth + 1])

        is_array = place in list_keys
        is_last = len(keys) - 1 == depth

        if is_array:
            if key not in res_:
                res_[key] = []

            data_ = data
            if not isinstance(data_, list):
                data_ = [data_]
            for item in data_:
                new_dict = {}
                recursive_nesting(item, new_dict, keys, depth + 1)
                res_[key].append(new_dict)
        else:
            if key not in res_:
                res_[key] = {}
            recursive_nesting(data, res_[key], keys, depth + 1)

        if is_last:
            if key in res_ and res_[key] and isinstance(res_[key], dict):
                res_[key].update(data)
            else:
                res_[key] = data

    for k, v in dict(value).items():
        names = k.split('.')
        recursive_nesting(v, res, names, 0)
    return res


def flatten_value(value, parent: Property, sep=".", key=""):
    value, _ = _flatten(value, sepgetter(parent, default_seperator=sep), key)
    return value


def get_root_attr(value: str, initial_root: str = "") -> str:
    value = value.split('@')[0]
    if not initial_root:
        return value.split('.')[0]

    contains_prefix = False
    value = value.replace(initial_root, '', 1)

    if value.startswith('.'):
        contains_prefix = True
        value = value.removeprefix('.')

    value = value.split('.')[0]
    root = f'{initial_root}.' if contains_prefix else initial_root

    return root + value


def get_last_attr(value: str):
    return value.split("@")[-1].split(".")[-1]


def extract_list_property_names(
    model: Model,
    properties: List[str],
) -> List[str]:
    list_keys = []
    for key in properties:
        if key in model.flatprops:
            prop = model.flatprops[key]
            if prop.list is not None and prop.list.place not in list_keys:
                list_keys.append(prop.list.place)
    return list_keys
