from __future__ import annotations

import os
import pathlib
import shutil
import tempfile
from typing import Any

from spinta.cli.manifest import copy_manifest
from spinta.components import Context


def migrate_columns(context: Context, destructive: bool, **kwargs: Any):
    files_path_str = kwargs.get('files_path')
    files_path = pathlib.Path(files_path_str)
    print(files_path_str)
    with tempfile.TemporaryDirectory(dir="/tmp") as tmp_dir:
        if files_path.is_file():
            temp_file_str = os.path.join(tmp_dir, os.path.basename(files_path_str))
            copy_manifest(context, output=temp_file_str, manifests=[files_path_str])
            shutil.move(temp_file_str, files_path_str)
        else:
            files = [str(file) for file in files_path.rglob('*') if file.is_file()]
            for file in files:
                temp_file_str = os.path.join(tmp_dir, os.path.basename(file))
                copy_manifest(context, output=temp_file_str, manifests=[file])
                shutil.move(temp_file_str, file)

    print("DSA files upgraded succesfully")
