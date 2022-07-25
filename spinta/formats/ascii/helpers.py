from texttable import Texttable


def draw(buffer, name, tnum, width):
    if tnum > 1:
        if name:
            yield f"\n\nTable {name} #{tnum}:\n"
        else:
            yield f"\n\nTable #{tnum}:\n"

    table = Texttable(width)
    table.set_deco(Texttable.HEADER)
    table.add_rows(buffer)
    yield table.draw()
