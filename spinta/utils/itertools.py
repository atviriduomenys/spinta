from itertools import islice
from itertools import chain
from typing import Iterable
from typing import Iterator
from typing import List
from typing import TypeVar
from typing import Union


def consume(generator, n=None):
    generator = islice(generator, n) if n else generator
    return sum((1 for _ in generator), 0)


def drain(g):
    for x in g:
        pass


def chunks(it, n=100):
    yield from iter(lambda: list(islice(it, n)), [])


def schunks(it, s=100):
    blen = 0
    buff = []
    for v in it:
        vlen = len(v)
        if blen + vlen > s and buff:
            yield buff
            blen = 0
            buff = []
        buff.append(v)
        blen += vlen
    if buff:
        yield buff


def peek(it):
    peek = list(islice(it, 1))
    return chain(peek, it)


def recursive_keys(dct, dot_notation=False, prefix=None):
    # yields all keys from a given nested dictionaries
    # e.g.:
    # d = dict(a=1, b=dict(c=2))
    # list(recursive_keys(dct, dot_notation=True))
    # >>> ['a', 'b.c']
    for k, v in dct.items():
        if isinstance(v, dict):
            if dot_notation:
                if prefix:
                    k = f"{prefix}{k}"
                prefix = f"{k}."
            yield from recursive_keys(v, dot_notation=dot_notation, prefix=prefix)
            prefix = None
        elif isinstance(v, list):
            # if there are lists - take first value from the list
            # assume that schema for all list elements are the same
            if dot_notation:
                if prefix:
                    k = f"{prefix}{k}"
                prefix = f"{k}."
            if v and isinstance(v[0], dict):
                yield from recursive_keys(v[0], dot_notation=dot_notation, prefix=prefix)
            else:
                yield k
            prefix = None
        else:  # do not yield dict's root key as it's redundant
            if prefix:
                k = f"{prefix}{k}"
            yield k


def last(it, default=None):
    res = default
    for res in it:
        pass
    return res


T = TypeVar('T')


def flatten(it: Iterable[Union[T, List[T]]]) -> Iterator[T]:
    for x in it:
        if isinstance(x, list):
            yield from x
        else:
            yield x
