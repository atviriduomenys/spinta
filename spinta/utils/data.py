from spinta.utils.schema import NA


def take(keys, *args):
    """Ultimate data extraction function

    Examples:

    +---------------------+----------------------------------------------------+
    | With `take`         | Without `take`                                     |
    +---------------------+----------------------------------------------------+
    | take(data)          | strip_metadata(data) if data else NA               |
    +---------------------+----------------------------------------------------+
    | take(data)          | {k: v for k, v in data.items()                     |
    |                     |       if not k.startswith('_')}                    |
    +---------------------+----------------------------------------------------+
    | take('a.b', data)   | v = data                                           |
    |                     | for k in 'a.b'.split('.'):                         |
    |                     |     v = v[k]                                       |
    +---------------------+----------------------------------------------------+
    | take('a', d1, d2)   | d1 and d1.get('a', NA) or d2 and d2.get('a', NA)   |
    +---------------------+----------------------------------------------------+

    """
    reserved = False
    if keys is all:
        keys = []
        reserved = True

    key = False
    if isinstance(keys, str):
        key = True
        keys = [keys]

    if not isinstance(keys, list):
        args = (keys,) + args
        keys = []

    args = [a for a in args if a]

    if len(keys) == 0:
        return {
            k: v
            for a in reversed(args)
            for k, v in a.items()
            if v is not NA and (reserved or not k.startswith('_'))
        }
    else:
        keys = [k.split('.') for k in keys]

    data = {}
    for k in keys:
        for v in args:
            for x in k:
                if v and x in v:
                    v = v[x]
                else:
                    break
            else:
                if v is not NA:
                    if key:
                        return v
                    data['.'.join(k)] = v
                    break

    if key:
        return NA
    else:
        return data
