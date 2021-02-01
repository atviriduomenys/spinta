from pathlib import Path
from typing import Optional

from typer import Argument

from spinta.manifests.tabular.helpers import write_tabular_manifest


def init(
    manifest: Optional[Path] = Argument(None),
):
    """Initialize a new manifest table"""
    with manifest.open('w') as f:
        write_tabular_manifest(f, [])

