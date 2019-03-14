def consume(generator):
    return sum((1 for _ in generator), 0)
