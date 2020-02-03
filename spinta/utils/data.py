from spinta.utils.schema import NA


def take(keys=None, *args, reserved=False):
    if len(args) == 0:
        args = [keys]
        keys = None

    args = [a for a in args if a]

    key = False
    if keys is None:
        keys = {
            k
            for a in args
            for k in a
            if reserved or not k.startswith('_')
        }

    elif not isinstance(keys, list):
        key = True
        keys = [keys]

    data = {}
    for k in keys:
        for a in args:
            if k in a:
                v = a[k]
                if v is not NA:
                    if key:
                        return v
                    data[k] = v
                    break

    if key:
        return NA
    else:
        return data
