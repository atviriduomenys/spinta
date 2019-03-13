from tempfile import NamedTemporaryFile
from itertools import islice, chain

import requests
import xlrd

from spinta.commands import Command


class XlsxDataset(Command):
    metadata = {
        'name': 'xlsx',
        'type': 'dataset.model',
    }

    def execute(self):
        skip = self.args.args.get('skip', None)
        limit = self.args.args.get('limit', None)

        with NamedTemporaryFile() as f:

            # We can't read data directly from stream, because openpyxl uses
            # seek on file-like object, so we need to download data to a
            # temporary file.
            with requests.get(self.args.url, stream=True) as r:
                for chunk in filter(None, r.iter_content(chunk_size=8192)):
                    f.write(chunk)
            f.seek(0)

            rows = read_excel(f.name)
            if skip:
                if isinstance(skip, dict):
                    value = set(skip['value']) if isinstance(skip['value'], list) else {skip['value']}
                    for row in rows:
                        if len(row) > skip['column'] and row[skip['column']] in skip['value']:
                            break
                    else:
                        self.error(f"Can't find header line: {skip!r}")
                    rows = chain([row], rows)
                else:
                    rows = islice(rows, skip, None)
            cols = {i: x.strip() for i, x in enumerate(next(rows, []))}
            if limit:
                rows = islice(rows, 0, limit)
            for row in rows:
                data = {}
                for i, value in enumerate(row):
                    if i in cols:
                        data[cols[i]] = value
                yield data


class XlsxDatasetProperty(Command):
    metadata = {
        'name': 'xlsx',
        'type': 'dataset.property',
    }

    def execute(self):
        return self.args.value.get(self.args.source)


def read_excel(filename):
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
