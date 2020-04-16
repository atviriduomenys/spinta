from itertools import islice


def consume(generator, n=None):
    generator = islice(generator, n) if n else generator
    return sum((1 for _ in generator), 0)


def chunks(it, n=100):
    yield from iter(lambda: list(islice(it, n)), [])


def recursive_keys(dct, dot_notation=False, prefix=None):
    # yields all keys from a given nested dictionaries
    for k, v in dct.items():
        if prefix:
            k = f"{prefix}{k}"
        yield k

        if isinstance(v, dict):
            if dot_notation:
                prefix = f"{k}."
            yield from recursive_keys(v, dot_notation=dot_notation, prefix=prefix)
            prefix = None


def last(it, default=None):
    res = default
    for res in it:
        pass
    return res
