from typing import Optional

from typer import Argument

from spinta.manifests.tabular.helpers import write_tabular_manifest


def init(
    manifest: Optional[str] = Argument(None, help="path to a manifest"),
):
    """Initialize a new manifest table

    `MANIFEST` is a file path, for example `manifest.csv` or `manifest.xlsx`.
    Depending of file extensions, a CSV of a XLSX format manifest will be
    created.
    """
    write_tabular_manifest(manifest)

