from tabulate import tabulate


def draw(buffer, name, tnum, width):
    if tnum > 1:
        if name:
            yield f"\n\nTable {name} #{tnum}:\n"
        else:
            yield f"\n\nTable #{tnum}:\n"

    yield tabulate(buffer, missingval='∅')
