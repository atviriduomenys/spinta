from enum import Enum

from spinta.commands import is_object_id


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


class Path(Cast):

    def to_params(self, value):
        return '/'.join(value)


class Property(Cast):
    pass


class QueryParams(Cast):

    def __init__(self, name, operator, **kwargs):
        self.name = name
        self.operator = operator

        super().__init__(**kwargs)

    def to_params(self, value):
        return {
            'name': self.name,
            'operator': self.operator,
            'key': value[0],
            'value': value[1],
        }


class Operator(Enum):
    EXACT = 'exact'
    GT = 'gt'
    GTE = 'gte'
    LT = 'lt'
    LTE = 'lte'
    NE = 'ne'
    CONTAINS = 'contains'
    STARTSWITH = 'startswith'


RULES = {
    'path': {
        'cast': Path(),
        'name': False,
    },
    'id': {
        'maxargs': 1,
        'name': False,
    },
    'properties': {
        'cast': Property(),
        'name': False,
        'minargs': 0,
    },
    'contents': {
        'maxargs': 0,
    },
    'dataset': {
        'cast': Path(),
    },
    'resource': {
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
    'show': {
        'minargs': 1,
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
        'cast': QueryParams('exact', Operator.EXACT),
        'minargs': 2,
        'maxargs': 2,
        'multiple': True,
        'change_name': 'query_params',
    },
    'gt': {
        'cast': QueryParams('gt', Operator.GT),
        'minargs': 2,
        'maxargs': 2,
        'multiple': True,
        'change_name': 'query_params',
    },
    'gte': {
        'cast': QueryParams('gte', Operator.GTE),
        'minargs': 2,
        'maxargs': 2,
        'multiple': True,
        'change_name': 'query_params',
    },
    'lt': {
        'cast': QueryParams('lt', Operator.LT),
        'minargs': 2,
        'maxargs': 2,
        'multiple': True,
        'change_name': 'query_params',
    },
    'lte': {
        'cast': QueryParams('lte', Operator.LTE),
        'minargs': 2,
        'maxargs': 2,
        'multiple': True,
        'change_name': 'query_params',
    },
    'ne': {
        'cast': QueryParams('ne', Operator.NE),
        'minargs': 2,
        'maxargs': 2,
        'multiple': True,
        'change_name': 'query_params',
    },
    'contains': {
        'cast': QueryParams('contains', Operator.CONTAINS),
        'minargs': 2,
        'maxargs': 2,
        'multiple': True,
        'change_name': 'query_params',
    },
    'startswith': {
        'cast': QueryParams('startswith', Operator.STARTSWITH),
        'minargs': 2,
        'maxargs': 2,
        'multiple': True,
        'change_name': 'query_params',
    },
}


def parse_url_path(context, path):
    data = []
    name = 'path'
    value = []
    params = {}

    parts = path.split('/')
    last = len(parts) - 1
    for i, part in enumerate(parts):
        if part.startswith(':'):
            data.append((name, value))
            name = part[1:]
            value = []
            continue

        if not data:
            if is_object_id(context, part):
                data.append((name, value))
                name = 'id'
                value = [part]
                if i < last and not parts[i + 1].startswith(':'):
                    data.append((name, value))
                    name = 'properties'
                    value = []
                continue

        value.append(part)

    data.append((name, value))

    for name, value in data:

        rules = RULES.get(name)
        if rules is None:
            raise Exception(f"Unknown URL parameter {name!r}.")

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
