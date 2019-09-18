from itertools import islice, chain

import xlrd

from spinta.components import Context
from spinta.types.dataset import Model
from spinta.fetcher import fetch
from spinta.commands import pull
from spinta.commands.sources import Source


class Xlsx(Source):
    schema = {
        'skip': {},
        'limit': {},
    }


@pull.register()
def pull(context: Context, source: Xlsx, node: Model, *, params: dict):
    dataset = node.parent
    path = fetch(context, dataset.source.name.format(**params))
    rows = _read_xlsx(str(path))
    if source.skip:
        if isinstance(source.skip, dict):
            value = set(source.skip['value']) if isinstance(source.skip['value'], list) else {source.skip['value']}
            for row in rows:
                if len(row) > source.skip['column'] and row[source.skip['column']] in source.skip['value']:
                    break
            else:
                raise exceptions.InvalidSource(node, source=source, error=f"Can't find header line: {source.skip!r}.")
            rows = chain([row], rows)
        else:
            rows = islice(rows, source.skip, None)
    cols = {i: x.strip() for i, x in enumerate(next(rows, []))}
    if source.limit:
        rows = islice(rows, 0, source.limit)
    for row in rows:
        data = {}
        for i, value in enumerate(row):
            if i in cols:
                data[cols[i]] = value
        yield data


def _read_xlsx(filename):
    wb = xlrd.open_workbook(filename)
    ws = wb.sheet_by_index(0)

    # Barrowed this code from https://github.com/pandas-dev/pandas/blob/6359bbc4c9ce6dd05bc8b422641cda74871cde43/pandas/io/excel/_xlrd.py
    epoch1904 = wb.datemode

    def _parse_cell(cell_contents, cell_typ):
        """converts the contents of the cell into a pandas
        appropriate object"""

        if cell_typ == xlrd.XL_CELL_DATE:

            # Use the newer xlrd datetime handling.
            try:
                cell_contents = xlrd.xldate.xldate_as_datetime(cell_contents, epoch1904)
            except OverflowError:
                return cell_contents

            # Excel doesn't distinguish between dates and time,
            # so we treat dates on the epoch as times only.
            # Also, Excel supports 1900 and 1904 epochs.
            year = (cell_contents.timetuple())[0:3]
            if ((not epoch1904 and year == (1899, 12, 31)) or
                    (epoch1904 and year == (1904, 1, 1))):
                cell_contents = f'{cell_contents.hour}:{cell_contents.minute}:{cell_contents.second}:{cell_contents.microsecond}'

        elif cell_typ == xlrd.XL_CELL_ERROR:
            cell_contents = None
        elif cell_typ == xlrd.XL_CELL_BOOLEAN:
            cell_contents = bool(cell_contents)
        elif cell_typ == xlrd.XL_CELL_NUMBER:
            # GH5394 - Excel 'numbers' are always floats
            # it's a minimal perf hit and less surprising
            val = int(cell_contents)
            if val == cell_contents:
                cell_contents = val

        return cell_contents

    for i in range(ws.nrows):
        yield [_parse_cell(value, typ) for value, typ in zip(ws.row_values(i), ws.row_types(i))]
