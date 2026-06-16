import pathlib
from typing import List

from click import echo

from spinta import commands
from spinta.cli.helpers.manifest import convert_str_to_manifest_path
from spinta.cli.helpers.message import cli_error
from spinta.cli.helpers.store import load_manifest
from spinta.components import Context
from spinta.core.context import configure_context
from spinta.manifests.tabular.helpers import write_tabular_manifest

RESERVED_MODEL_PREFIX = "_"


def remove_explicit_id_properties(context: Context, manifest: str) -> int:
    count_of_ids = 0
    for model in commands.get_models(context, manifest).values():
        if model.model_type().startswith(RESERVED_MODEL_PREFIX):
            continue
        prop = model.properties.get("_id")
        if prop is None or not prop.explicitly_given:
            continue

        prop.explicitly_given = False
        prop.given.explicit = False
        count_of_ids += 1
    return count_of_ids


def remove_local_ids(
    context: Context,
    manifests: List[str] | None = None,
    output_path: pathlib.Path | None = None,
    **kwargs,
) -> None:
    if not manifests:
        cli_error("`remove_local_ids` requires at least one source manifest path.")

    manifest_paths = convert_str_to_manifest_path(list(manifests))
    context = configure_context(context, manifest_paths)
    store = load_manifest(context, verbose=False, full_load=True)
    manifest = store.manifest

    count_of_ids = remove_explicit_id_properties(context, manifest)

    output = output_path or pathlib.Path(manifests[0])
    if count_of_ids == 0:
        echo(f'No `_id` rows to remove in "{output}".')
        return

    write_tabular_manifest(context, str(output), manifest)
    echo(f'Removed {count_of_ids} `_id` row(s) from "{output}".')
