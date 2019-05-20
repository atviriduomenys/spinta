from itertools import islice


def consume(generator, n=None):
    generator = islice(generator, n) if n else generator
    return sum((1 for _ in generator), 0)
