from typing import Optional

from typer import Argument

from spinta.components import Context
from spinta.manifests.tabular.helpers import write_tabular_manifest
from typer import Context as TyperContext


def init(
    ctx: TyperContext,
    manifest: Optional[str] = Argument(None, help="path to a manifest"),
):
    """Initialize a new manifest table

    `MANIFEST` is a file path, for example `manifest.csv` or `manifest.xlsx`.
    Depending of file extensions, a CSV of a XLSX format manifest will be
    created.
    """
    context: Context = ctx.obj
    write_tabular_manifest(context, manifest)

