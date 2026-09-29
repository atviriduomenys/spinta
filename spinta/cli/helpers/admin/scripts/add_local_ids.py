import pathlib
from copy import copy
from typing import List

from click import echo

from spinta import commands
from spinta.cli.helpers.manifest import convert_str_to_manifest_path
from spinta.cli.helpers.message import cli_error
from spinta.cli.helpers.store import load_manifest
from spinta.components import Context
from spinta.core.context import configure_context
from spinta.manifests.tabular.helpers import write_tabular_manifest
from spinta.types.datatype import Base32, String

RESERVED_MODEL_PREFIX = "_"


def add_explicit_id_properties(context: Context, manifest: str, verbose: bool = True) -> int:
    count_of_ids = 0
    for model in commands.get_models(context, manifest).values():
        if model.model_type().startswith(RESERVED_MODEL_PREFIX):
            continue
        if not model.external or model.external.unknown_primary_key:
            if verbose:
                echo(f'Model "{model.model_type()}" has no `ref`, skipping.', err=True)
            continue
        prop = model.id_prop
        if prop is None or prop.explicitly_given:
            continue

        pkeys = model.external.pkeys
        composite = len(pkeys) > 1
        pkey_dtype = pkeys[0].dtype
        old_backend = prop.dtype.backend

        prop.explicitly_given = True
        prop.given.explicit = True
        if composite or isinstance(pkey_dtype, String):
            new_dtype = Base32()
            new_dtype.name = "base32"
            new_dtype.type = "base32"
            new_dtype.type_args = []
            new_dtype.unique = False
            new_dtype.required = False
            new_dtype.prop = prop
            new_dtype.backend = old_backend
        else:
            new_dtype = copy(pkey_dtype)
            new_dtype.prop = prop
            new_dtype.type_args = list(pkey_dtype.type_args or [])
            new_dtype.backend = old_backend
        prop.dtype = new_dtype
        count_of_ids += 1
    return count_of_ids


def add_local_ids(
    context: Context,
    manifests: List[str] | None = None,
    output_path: pathlib.Path | None = None,
    **kwargs,
) -> None:
    if not manifests:
        cli_error("`add_local_ids` requires at least one source manifest path.")

    manifest_paths = convert_str_to_manifest_path(list(manifests))
    context = configure_context(context, manifest_paths)
    store = load_manifest(context, verbose=False, full_load=True)
    manifest = store.manifest

    count_of_ids = add_explicit_id_properties(context, manifest)

    output = output_path or pathlib.Path(manifests[0])
    if count_of_ids == 0:
        echo(f'No `_id` rows to add in "{output}".')
        return

    write_tabular_manifest(context, str(output), manifest)
    echo(f'Added {count_of_ids} `_id` row(s) to "{output}".')
