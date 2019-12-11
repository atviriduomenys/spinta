from spinta import exceptions


COMPOPS = (
    'eq',
    'ge',
    'gt',
    'le',
    'lt',
    'ne',
    'contains',
    'startswith',
)


def _replace_recurse(model, arg):
    name = arg['name']
    opargs = arg.get('args', ())
    if name in COMPOPS:
        if len(opargs) == 2 and isinstance(opargs[0], dict) and opargs[0]['name'] == 'recurse':
            if len(opargs[0]['args']) == 1:
                rkey = opargs[0]['args'][0]
                props = model.leafprops.get(rkey, [])
                if len(props) == 1:
                    return {
                        'name': name,
                        'args': [
                            props[0].place,
                            opargs[1],
                        ]
                    }
                elif len(props) > 1:
                    return {
                        'name': 'or',
                        'args': [
                            {
                                'name': name,
                                'args': [
                                    prop.place,
                                    opargs[1],
                                ]
                            }
                            for prop in props
                        ],
                    }
                else:
                    raise exceptions.FieldNotInResource(model, property=rkey)
    return arg
