import csv
import pathlib
import textwrap

from spinta.manifests.components import Manifest
from spinta.manifests.tabular.constants import DATASET
from spinta.manifests.tabular.helpers import datasets_to_tabular


SHORT_NAMES = {
    'd': 'dataset',
    'r': 'resource',
    'b': 'base',
    'm': 'model',
}


def striptable(table):
    return textwrap.dedent(table).strip()


def create_tabular_manifest(path: pathlib.Path, manifest: str):
    header = None
    manifest = striptable(manifest)
    lines = iter(manifest.splitlines())
    for line in lines:
        line = line.strip()
        if line == '':
            continue
        header = line.split('|')
        header = [h.strip().lower() for h in header]
        header = [SHORT_NAMES.get(h, h) for h in header]
        break
    with path.open('w') as f:
        writer = csv.writer(f)
        for row in _read_tabular_manifest(header, lines):
            writer.writerow(row)


def _read_tabular_manifest(header, lines):
    # Find index where dimension columns end.
    dim = sum(1 for h in header if h in DATASET[:6])
    yield header
    for line in lines:
        line = line.strip()
        if line == '':
            continue
        row = line.split('|')
        row = [x.strip() for x in row]
        rem = len(header) - len(row)
        row = row[:dim - rem] + [''] * rem + row[dim - rem:]
        assert len(header) == len(row), line
        yield row


def render_tabular_manifest(manifest: Manifest) -> str:
    rows = datasets_to_tabular(manifest)
    cols = DATASET
    hs = 1                       # hierachical cols start
    he = cols.index('property')  # hierachical cols end
    hsize = 1                    # hierachical column size
    bsize = 3                    # border size
    sizes = dict(
        [(c, len(c)) for c in cols[:hs]] +
        [(c, 1) for c in cols[hs:he]] +
        [(c, len(c)) for c in cols[he:]]
    )
    rows = list(rows)
    for row in rows:
        for i, col in enumerate(cols):
            val = '' if row[col] is None else str(row[col])
            if col == 'id':
                sizes[col] = 2
            elif i < he:
                size = (hsize + bsize) * (he - hs - i) + sizes['property']
                if size < len(val):
                    sizes['property'] += len(val) - size
            elif sizes[col] < len(val):
                sizes[col] = len(val)

    line = []
    for col in cols:
        size = sizes[col]
        line.append(col[:size].ljust(size))

    depth = 0
    lines = [line]
    for row in rows:
        line = [
            row['id'][:2] if row['id'] else '  '
        ]

        for i, col in enumerate(cols[hs:he + 1]):
            val = row[col] or ''
            if val:
                depth = i
                break
        else:
            val = ''
            if depth < he - hs:
                depth += 1
            else:
                depth = 2

        line += [' ' * hsize] * depth
        size = (hsize + bsize) * (he - hs - depth) + sizes['property']
        line += [val.ljust(size)]

        for col in cols[he + 1:]:
            val = '' if row[col] is None else str(row[col])
            size = sizes[col]
            line.append(val.ljust(size))

        lines.append(line)

    lines = [' | '.join(line) for line in lines]
    lines = [l.rstrip() for l in lines]
    return '\n'.join(lines)
