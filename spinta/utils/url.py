import re


def sort_key_to_native(value):
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


def sort_key_to_path(value):
    if value['ascending']:
        return value['name']
    else:
        return '-' + value['name']


RULES = {
    'path': {
        'reduce': '/'.join,
    },
    'id': {
        'maxargs': 1,
        'cast': (int, int),
    },
    'key': {
        'maxargs': 1,
    },
    'source': {
        'reduce': '/'.join,
    },
    'sort': {
        'cast': (sort_key_to_native, sort_key_to_path),
    },
    'limit': {
        'maxargs': 1,
        'cast': (int, str),
    },
    'offset': {
        'maxargs': 1,
        'cast': (int, str),
    },
    'format': {
        'maxargs': 1,
    },
}

key_re = re.compile(r'^[0-9a-f]{40}$')


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
        elif not data and key_re.match(part):
            data.append((name, value))
            name = 'key'
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
            value = list(map(rules['cast'][0], value))

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


def build_url_path(params):
    params = dict(params)
    parts = []
    if 'path' in params:
        parts.append(params.pop('path'))
    if 'id' in params:
        parts.append(str(params.pop('id')))
    if 'key' in params:
        parts.append(params.pop('key'))
    if 'source' in params:
        parts.append(':source')
        parts.append(params.pop('source'))
    for k, v in params.items():
        rules = RULES.get(k)
        if 'cast' in rules:
            v = rules['cast'][1](v)
        parts.extend([f':{k}', v])
    return '/'.join(parts)
