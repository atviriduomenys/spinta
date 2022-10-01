import csv
import pathlib
from io import StringIO
from typing import List

from spinta.manifests.tabular.components import ManifestColumn
from spinta.manifests.tabular.constants import DATASET
from spinta.manifests.tabular.helpers import read_ascii_tabular_rows
from spinta.manifests.tabular.helpers import torow
from spinta.manifests.tabular.helpers import write_tabular_manifest


def create_tabular_manifest(
    path: pathlib.Path,
    manifest: str,
) -> None:
    path = pathlib.Path(path)
    rows = read_ascii_tabular_rows(manifest, strip=True)
    cols: List[ManifestColumn] = next(rows, [])
    if cols:
        rows = (torow(DATASET, dict(zip(cols, row))) for row in rows)
        write_tabular_manifest(str(path), rows)


def convert_ascii_manifest_to_csv(manifest: str) -> bytes:
    file = StringIO()
    writer = csv.writer(file)
    rows = read_ascii_tabular_rows(
        manifest,
        strip=True,
        check_column_names=False,
    )
    for row in rows:
        writer.writerow(row)
    return file.getvalue().encode()
