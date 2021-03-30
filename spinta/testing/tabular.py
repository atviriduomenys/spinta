import pathlib
from typing import List
from typing import Literal
from typing import Union
from typing import overload

from spinta import commands
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.manifests.components import Manifest
from spinta.manifests.helpers import load_manifest_nodes
from spinta.manifests.tabular.components import ManifestColumn
from spinta.manifests.tabular.constants import DATASET
from spinta.manifests.tabular.helpers import read_ascii_tabular_manifest
from spinta.manifests.tabular.helpers import read_ascii_tabular_rows
from spinta.manifests.tabular.helpers import read_tabular_manifest
from spinta.manifests.tabular.helpers import torow
from spinta.manifests.tabular.helpers import write_tabular_manifest
from spinta.testing.context import create_test_context


def create_tabular_manifest(
    path: pathlib.Path,
    manifest: str,
):
    path = pathlib.Path(path)
    rows = read_ascii_tabular_rows(manifest, strip=True)
    cols: List[ManifestColumn] = next(rows, [])
    if cols:
        rows = (torow(DATASET, dict(zip(cols, row))) for row in rows)
        write_tabular_manifest(str(path), rows)


@overload
def load_tabular_manifest(
    rc: RawConfig,
    manifest: Union[pathlib.Path, str],
    *,
    return_context: Literal[True],
) -> Context:
    ...


@overload
def load_tabular_manifest(
    rc: RawConfig,
    manifest: Union[pathlib.Path, str],
    *,
    return_context: Literal[False],
) -> Manifest:
    ...


def load_tabular_manifest(
    rc: RawConfig,
    manifest: Union[pathlib.Path, str],
    *,
    return_context: bool = False,
) -> Union[Manifest, Context]:
    if isinstance(manifest, str) and '|' in manifest:
        path = ''
    else:
        path = str(manifest)
        manifest = None

    rc = rc.fork({
        'manifest': 'default',
        'manifests': {
            'default': {
                'type': 'tabular',
                'path': path,
                'keymap': 'default',
            },
        },
    })

    context = create_test_context(rc)

    config = context.get('config')
    commands.load(context, config)

    store = context.get('store')
    commands.load(context, store)

    if path:
        schemas = read_tabular_manifest(path)
    else:
        schemas = read_ascii_tabular_manifest(manifest, strip=True)
    load_manifest_nodes(context, store.manifest, schemas)

    commands.link(context, store.manifest)

    if return_context:
        return context
    else:
        return store.manifest
