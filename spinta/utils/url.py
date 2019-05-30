import re


class Cast:

    def __init__(self, to_params=None, to_string=None, each=False):
        self._to_params = to_params
        self._to_string = to_string
        self._each = each

    def to_params(self, value):
        if self._each:
            return list(map(self.to_params_item, value))
        else:
            return self.to_params_item(value)

    def to_string(self, value):
        if self._each:
            return list(map(self.to_string_item, value))
        else:
            return [] if value is None else [self.to_string_item(value)]

    def to_params_item(self, value):
        return self._to_params(value) if self._to_params else value

    def to_string_item(self, value):
        return self._to_string(value) if self._to_string else value


class SortKey(Cast):

    def to_params_item(self, value):
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

    def to_string_item(self, value):
        if value['ascending']:
            return value['name']
        else:
            return '-' + value['name']


class Id(Cast):

    key_re = re.compile(r'^[0-9a-f]{40}$')
    mongo_id_re = re.compile(r'^[0-9a-f]{24}$')

    def match(self, value):
        if value.isdigit():
            return {'value': value, 'type': 'integer'}
        if self.key_re.match(value):
            return {'value': value, 'type': 'sha1'}
        if self.mongo_id_re.match(value):
            return {'value': value, 'type': 'mongo'}

    def to_params(self, value):
        return value

    def to_string(self, value):
        return [] if value['value'] is None else [str(value['value'])]


class Path(Cast):

    def to_params(self, value):
        return '/'.join(value)


class QueryParams(Cast):

    def __init__(self, operator, **kwargs):
        self.operator = operator

        super().__init__(**kwargs)

    def to_params(self, value):
        return {
            'operator': self.operator,
            'key': value[0],
            'value': value[1],
        }


RULES = {
    'path': {
        'cast': Path(),
        'name': False,
    },
    'id': {
        'maxargs': 1,
        'cast': Id(),
        'name': False,
    },
    'source': {
        'cast': Path(),
    },
    'changes': {
        'minargs': 0,
        'maxargs': 1,
        'cast': Cast(int, str),
    },
    'sort': {
        'cast': SortKey(each=True),
    },
    'limit': {
        'maxargs': 1,
        'cast': Cast(int, str),
    },
    'offset': {
        'maxargs': 1,
        'cast': Cast(int, str),
    },
    'format': {
        'maxargs': 1,
    },
    'count': {
        'maxargs': 0,
    },
    'exact': {
        'cast': QueryParams('exact'),
        'minargs': 2,
        'maxargs': 2,
        'multiple': True,
        'change_name': 'query_params',
    },
}


def parse_url_path(path):
    data = []
    name = 'path'
    value = []
    params = {}

    for part in path.split('/'):
        if part.startswith(':'):
            data.append((name, value))
            name = part[1:]
            value = []
            continue

        if not data:
            id = RULES['id']['cast'].match(part)
            if id is not None:
                data.append((name, value))
                name = 'id'
                value = [id]
                continue

        value.append(part)

    data.append((name, value))

    for name, value in data:

        rules = RULES.get(name)
        if rules is None:
            raise Exception(f"Unknown URl parameter {name!r}.")

        maxargs = rules.get('maxargs')
        minargs = rules.get('minargs', 0 if maxargs == 0 else 1)
        if len(value) < minargs:
            raise Exception(f"At least {minargs} argument is required for {name!r} URL parameter.")

        if maxargs is not None and len(value) > maxargs:
            raise Exception(f"URL parameter {name!r} can only have {maxargs} arguments.")

        if minargs == 1 and maxargs == 1:
            value = value[0]
        elif minargs == 0 and maxargs == 1:
            value = value[0] if value else None

        if 'cast' in rules and value is not None:
            value = rules['cast'].to_params(value)

        if 'change_name' in rules:
            name = rules['change_name']

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
    sort_by = list(RULES.keys())

    def sort_key(value):
        if value not in sort_by:
            sort_by.append(value)
        return sort_by.index(value)

    for k, v in sorted(params.items(), key=sort_key):
        rules = RULES.get(k)
        if rules is None:
            raise Exception(f"Unknown URl parameter {k!r}.")

        if 'cast' in rules:
            v = rules['cast'].to_string(v)
        else:
            v = [] if v is None else v
            v = v if isinstance(v, list) else [v]

        assert isinstance(v, list), v

        if rules.get('name', True):
            parts.extend([f':{k}'] + v)
        else:
            parts.extend(v)

    return '/'.join(parts) if parts else None
