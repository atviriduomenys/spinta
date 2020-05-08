
class QueryBuilder:
    compops = (
        'eq',
        'ge',
        'gt',
        'le',
        'lt',
        'ne',
        'contains',
        'startswith',
    )

    def __init__(
        self,
        context: Context,
        model: Model,
        backend: Mongo,
        table,
    ):
        self.context = context
        self.model = model
        self.backend = backend
        self.table = table
        self.select = []
        self.where = []

    def build(
        self,
        select: typing.List[str] = None,
        sort: typing.Dict[str, dict] = None,
        offset: int = None,
        limit: int = None,
        query: Optional[List[dict]] = None,
    ) -> dict:
        keys = select if select and '*' not in select else self.model.flatprops
        keys = {k: 1 for k in keys}
        keys['_id'] = 0
        keys['__id'] = 1
        keys['_revision'] = 1
        query = self.op_and(*(query or []))
        cursor = self.table.find(query, keys)

        if limit is not None:
            cursor = cursor.limit(limit)

        if offset is not None:
            cursor = cursor.skip(offset)

        if sort:
            direction = {
                'positive': pymongo.ASCENDING,
                'negative': pymongo.DESCENDING,
            }
            nsort = []
            for k in sort:
                # Optional sort direction: sort(+key) or sort(key)
                if isinstance(k, dict) and k['name'] in direction:
                    d = direction[k['name']]
                    k = k['args'][0]
                else:
                    d = direction['positive']

                if not is_valid_sort_key(k, self.model):
                    raise exceptions.FieldNotInResource(self.model, property=k)

                if k == '_id':
                    k = '__id'

                nsort.append((k, d))
            cursor = cursor.sort(nsort)

        return cursor

    def resolve_recurse(self, arg):
        name = arg['name']
        if name in self.compops:
            return _replace_recurse(self.model, arg, 0)
        if name == 'any':
            return _replace_recurse(self.model, arg, 1)
        return arg

    def resolve(self, args: Optional[List[dict]]) -> None:
        for arg in (args or []):
            arg = self.resolve_recurse(arg)
            name = arg['name']
            opargs = arg.get('args', ())
            method = getattr(self, f'op_{name}', None)
            if method is None:
                raise exceptions.UnknownOperator(self.model, operator=name)
            if name in self.compops:
                yield self.comparison(name, method, *opargs)
            else:
                yield method(*opargs)

    def resolve_property(self, key: Union[str, tuple]) -> Property:
        if key not in self.model.flatprops:
            raise exceptions.FieldNotInResource(self.model, property=key)
        return self.model.flatprops[key]

    def resolve_value(self, op, prop: Property, value: Union[str, dict]) -> object:
        return commands.load_search_params(self.context, prop.dtype, self.backend, {
            'name': op,
            'args': [prop.place, value]
        })

    def comparison(self, op, method, key, value):
        lower = False
        if isinstance(key, dict) and key['name'] == 'lower':
            lower = True
            key = key['args'][0]

        if isinstance(key, tuple):
            key = '.'.join(key)

        prop = self.resolve_property(key)
        value = self.resolve_value(op, prop, value)

        if key == '_id':
            key = '__id'
        elif key != '_revision' and key.startswith('_'):
            raise exceptions.FieldNotInResource(self.model, property=key)

        return method(key, value, lower)

    def op_group(self, *args: List[dict]):
        args = list(self.resolve(args))
        assert len(args) == 1, "Group with multiple args are not supported here."
        return args[0]

    def op_and(self, *args: List[dict]):
        args = list(self.resolve(args))
        if len(args) > 1:
            return {'$and': args}
        if len(args) == 1:
            return args[0]
        else:
            return {}

    def op_or(self, *args: List[dict]):
        args = list(self.resolve(args))
        if len(args) > 1:
            return {'$or': args}
        if len(args) == 1:
            return args[0]
        else:
            return {}

    def op_eq(self, key, value, lower=False):
        if lower:
            # TODO: I don't know how to lower case values in mongo.
            value = re.compile('^' + value + '$', re.IGNORECASE)
        return {key: value}

    def op_ge(self, key, value, lower=False):
        return {key: {'$gte': value}}

    def op_gt(self, key, value, lower=False):
        return {key: {'$gt': value}}

    def op_le(self, key, value, lower=False):
        return {key: {'$lte': value}}

    def op_lt(self, key, value, lower=False):
        return {key: {'$lt': value}}

    def op_ne(self, key, value, lower=False):
        # MongoDB's $ne operator does not consume regular expresions for values,
        # whereas `$not` requires an expression.
        # Thus if our search value is regular expression - search with $not, if
        # not - use $ne
        if lower:
            # TODO: I don't know how to lower case values in mongo.
            value = re.escape(value)
            value = re.compile('^' + value + '$', re.IGNORECASE)
            return {
                '$and': [
                    {key: {'$not': value, '$exists': True}},
                    {key: {'$ne': None, '$exists': True}},
                ],
            }
        else:
            return {
                '$and': [
                    {key: {'$ne': value, '$exists': True}},
                    {key: {'$ne': None, '$exists': True}},
                ]
            }

    def op_contains(self, key, value, lower=False):
        try:
            value = re.escape(value)
            value = re.compile(value, re.IGNORECASE)
        except TypeError:
            # in case value is not a string - then just search for that value directly
            # XXX: Let's not guess, but check schema instead.
            pass
        return {key: value}

    def op_startswith(self, key, value, lower=False):
        # https://stackoverflow.com/a/3483399
        try:
            value = re.escape(value)
            value = re.compile('^' + value + '.*', re.IGNORECASE)
        except TypeError:
            # in case value is not a string - then just search for that value directly
            # XXX: Let's not guess, but check schema instead.
            pass
        return {key: value}
