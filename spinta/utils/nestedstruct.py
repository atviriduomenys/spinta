def flatten(rows, sep='.', scan=100):
    for row in rows:
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
