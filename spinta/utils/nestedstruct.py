from spinta.types.type import Array, Object


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


def get_nested_property_type(properties, prop_name):
    prop_names = prop_name.split('.')

    if len(prop_names) > 1:
        prop = properties.get(prop_names[0])

        if prop is not None:
            if isinstance(prop.type, Array):
                # FIXME: There can be cases, when Array is a list of scalars,
                # in that case there would be no such thing as
                # prop.type.itmes.type.priprieties.
                props = prop.type.items.type.properties
            elif isinstance(prop.type, Object):
                props = prop.type.properties
            return get_nested_property_type(props, '.'.join(prop_names[1:]))
        else:
            return None
    else:
        prop = properties.get(prop_names[0])
        return prop.type if prop else None
