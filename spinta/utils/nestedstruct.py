from typing import List


def flatten(data, sep='.', scan=100):
    if isinstance(data, dict):
        return _flatten_dict(data, sep)
    else:
        return _flatten_list(data, sep, scan)


def _flatten_dict(data, sep='.'):
    for item in flatten_nested_lists(data):
        return {sep.join(k): v for k, v in item}


def _flatten_list(data, sep='.', scan=100):
    for row in data:
        for item in flatten_nested_lists(row):
            yield {sep.join(k): v for k, v in item}


def flatten_nested_lists(nested, field=(), context=None):
    data, lists = separate_dicts_from_lists(nested, field)
    data += (context or [])
    if lists:
        for key, values in lists:
            for value in values:
                yield from flatten_nested_lists(value, key, data)
    else:
        yield data


def separate_dicts_from_lists(nested, field=()):
    data = []
    lists = []
    for key, value in flatten_nested_dicts(nested, field):
        if isinstance(value, (tuple, list)):
            lists.append((key, value))
        else:
            data.append((key, value))
    return data, lists


def flatten_nested_dicts(nested, field=()):
    if isinstance(nested, dict):
        for k, v in nested.items():
            yield from flatten_nested_dicts(v, field + (k,))
    else:
        yield (field, nested)


def build_select_tree(select: List[str]):
    tree = {}
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
