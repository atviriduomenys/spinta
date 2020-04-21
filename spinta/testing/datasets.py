import csv
import itertools
import json
import pathlib
import textwrap

from spinta.cli import pull as pull_
from spinta.manifests.components import Manifest
from spinta.manifests.tabular.constants import DATASET
from spinta.manifests.tabular.helpers import datasets_to_tabular


def pull(cli, rc, dataset, model=None, *, push=True):
    cmd = [
        [dataset],
        ['--push'] if push else [],
        ['--model', model] if model else [],
        ['-e', 'stdout:jsonl'],
    ]
    cmd = list(itertools.chain(*cmd))
    result = cli.invoke(rc, pull_, cmd)
    data = []
    for line in result.stdout.splitlines():
        try:
            d = json.loads(line)
        except json.JSONDecodeError:
            print(line)
        else:
            data.append(d)
    return data


def striptable(table):
    return textwrap.dedent(table).strip()


def create_tabular_manifest(path: pathlib.Path, manifest: str):
    parser = None
    lines = iter(manifest.splitlines())
    for line in lines:
        line = line.strip()
        if line == '':
            continue
        if line.startswith('id | d | r | b | m | property'):
            parser = _create_tabular_dataset_manifest
        else:
            raise Exception(f"Unknown header: {line}")
        break
    with path.open('w') as f:
        writer = csv.writer(f)
        for row in parser(lines):
            writer.writerow(row)


def _create_tabular_dataset_manifest(lines):
    yield DATASET
    for line in lines:
        line = line.strip()
        if line == '':
            continue
        row = line.split('|')
        row = [x.strip() for x in row]
        rem = 14 - len(row)
        row = row[:6 - rem] + [''] * rem + row[6 - rem:]
        assert len(DATASET) == len(row), line
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
