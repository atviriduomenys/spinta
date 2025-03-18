from __future__ import annotations

import pathlib
from typing import Any

from spinta.cli.manifest import copy_manifest
from spinta.components import Context


def migrate_columns(context: Context, destructive: bool, **kwargs: Any):
    files_path_str = kwargs.get('files_path')
    files_path = pathlib.Path(files_path_str)
    if files_path.is_file():
        copy_manifest(context, output=files_path_str, manifests=[files_path_str])
    else:
        files = [str(file) for file in files_path.rglob('*') if file.is_file()]
        for file in files:
            copy_manifest(context, output=file, manifests=[file])

    print("DSA files upgraded succesfully")
