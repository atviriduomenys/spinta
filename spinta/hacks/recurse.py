from spinta import exceptions


def _replace_recurse(model, arg, ntharg):
    name = arg['name']
    args = arg.get('args', [])
    if len(args) <= ntharg:
        return arg
    if not isinstance(args[ntharg], dict):
        return arg
    if (
        args[ntharg]['name'] == 'lower' and
        isinstance(args[ntharg]['args'][0], dict) and
        args[ntharg]['args'][0]['name'] == 'recurse'
    ):
        if len(args[ntharg]['args'][0]['args']) == 1:
            rkey = args[ntharg]['args'][0]['args'][0]
            props = model.leafprops.get(rkey, [])
            if len(props) == 1:
                return {
                    'name': name,
                    'args': [
                        {
                            'name': 'lower',
                            'args': [props[0].place],
                        }
                        if i == ntharg else a
                        for i, a in enumerate(args)
                    ]
                }
            elif len(props) > 1:
                return {
                    'name': 'or',
                    'args': [
                        {
                            'name': name,
                            'args': [
                                {
                                    'name': 'lower',
                                    'args': [prop.place],
                                }
                                if i == ntharg else a
                                for i, a in enumerate(args)
                            ]
                        }
                        for prop in props
                    ],
                }
            else:
                raise exceptions.FieldNotInResource(model, property=rkey)
    elif args[ntharg]['name'] == 'recurse':
        if len(args[ntharg]['args']) == 1:
            rkey = args[ntharg]['args'][0]
            props = model.leafprops.get(rkey, [])
            if len(props) == 1:
                return {
                    'name': name,
                    'args': [
                        props[0].place if i == ntharg else a
                        for i, a in enumerate(args)
                    ]
                }
            elif len(props) > 1:
                return {
                    'name': 'or',
                    'args': [
                        {
                            'name': name,
                            'args': [
                                prop.place if i == ntharg else a
                                for i, a in enumerate(args)
                            ]
                        }
                        for prop in props
                    ],
                }
            else:
                raise exceptions.FieldNotInResource(model, property=rkey)
    return arg
