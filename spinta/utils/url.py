RULES = {
    'path': {
        'minargs': 0,
    },
    'ns': {
        'maxargs': 0,
    },
    'dataset': {
        'minargs': 1,
    },
    'resource': {
        'minargs': 1,
    },
    'origin': {
        'minargs': 1,
    },
    'changelog': {
        'maxargs': 1,
    },
    'format': {
        'maxargs': 1,
    },
}


def apply_query_rules(RULES, params):
    for param in params:
        name = param['name']
        args = param['args']

        if name not in RULES:
            raise Exception(f"Unknown URL parameter {name!r}.")

        rules = RULES[name]
        maxargs = rules.get('maxargs')
        minargs = rules.get('minargs', 0 if maxargs == 0 else 1)
        if len(args) < minargs:
            raise Exception(f"At least {minargs} argument is required for {name!r} URL parameter.")

        if maxargs is not None and len(args) > maxargs:
            raise Exception(f"URL parameter {name!r} can only have {maxargs} arguments.")
    return params


def parse_url_path(path):
    query = []
    name = 'path'
    args = []
    parts = path.split('/') if path else []
    for part in parts:
        if part.startswith(':'):
            query.append({
                'name': name,
                'args': args,
            })
            name = part[1:]
            args = []
        else:
            args.append(part)
    query.append({
        'name': name,
        'args': args,
    })
    apply_query_rules(RULES, query)
    return query


def build_url_path(query):
    parts = []
    for param in query:
        name = param['name']
        args = param['args']
        if name == 'path':
            parts.extend(args)
        else:
            parts.extend([f':{name}'] + args)
    return '/'.join(parts)
