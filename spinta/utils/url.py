SCHEMA = {
    'path': {
        'multiple': False,
        'args': None,
    },
    'id': {
        'multiple': False,
        'args': 1,
    },
    'source': {
        'multiple': False,
        'args': 1,
    },
}


def parse_url_path(path):
    result = {'path': []}
    name = 'path'
    params = SCHEMA[name]
    for part in path.split('/'):
        if part.startswith(':'):
            name = part[1:]
            if name not in SCHEMA:
                raise Exception(f"Unknown URl parameter {name!r}.")
            params = SCHEMA[name]
            if params['args'] == 1:
                if params['multiple']:
                    result[name] = []
                elif name in result:
                    raise Exception(f"URL parameter {name!r} can't be used more than once.")
            else:
                if name not in result:
                    result[name] = []
                elif params['multiple'] is False:
                    raise Exception(f"URL parameter {name!r} can't be used more than once.")
                if params['multiple']:
                    result[name].append([])
        elif name is None:
            raise Exception(f"Expected URL path /:parameter, got {part!r}.")
        elif name == 'path' and part.isdigit():
            result['id'] = int(part)
            name = None
        else:
            if params['args'] == 1:
                if params['multiple']:
                    result[name].append(part)
                else:
                    result[name] = part
            else:
                if params['multiple']:
                    args = result[name][-1]
                else:
                    args = result[name]
                args.append(part)
                if params['args'] is not None and len(args) > params['args']:
                    raise Exception(f"URL parameter {name!r} can only have {params['args']} arguments.")
    result['path'] = '/'.join(result['path'])
    return result
