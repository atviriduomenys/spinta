def sort_key(value):
    if value.startswith('-'):
        return {
            'name': value[1:],
            'ascending': False,
        }
    else:
        return {
            'name': value,
            'ascending': True,
        }


RULES = {
    'path': {
        'reduce': '/'.join,
    },
    'id': {
        'maxargs': 1,
        'cast': int,
    },
    'source': {
        'reduce': '/'.join,
    },
    'sort': {
        'cast': sort_key,
    },
    'limit': {
        'maxargs': 1,
        'cast': int,
    },
    'offset': {
        'maxargs': 1,
        'cast': int,
    },
}


def parse_url_path(path):
    data = []
    name = 'path'
    value = []
    for part in path.split('/'):
        if part.startswith(':'):
            data.append((name, value))
            name = part[1:]
            value = []
        elif not data and part.isdigit():
            data.append((name, value))
            name = 'id'
            value = [part]
        else:
            value.append(part)
    data.append((name, value))

    params = {}
    for name, value in data:

        rules = RULES.get(name)
        if rules is None:
            raise Exception(f"Unknown URl parameter {name!r}.")

        minargs = rules.get('minargs', 1)
        if len(value) < minargs:
            raise Exception(f"At least {minargs} argument is required for {name!r} URL parameter.")

        maxargs = rules.get('maxargs')
        if maxargs is not None and len(value) > maxargs:
            raise Exception(f"URL parameter {name!r} can only have {maxargs} arguments.")

        if 'cast' in rules:
            value = list(map(rules['cast'], value))

        if minargs == 1 and maxargs == 1:
            value = value[0]

        if 'reduce' in rules:
            value = rules['reduce'](value)

        multiple = rules.get('multiple', False)
        if multiple:
            if name not in params:
                params[name] = []
            params[name].append(value)
        elif name in params:
            raise Exception(f"Multiple values for {name!r} URL parameter are not allowed.")
        else:
            params[name] = value

    return params
