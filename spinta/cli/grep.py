from typing import Iterator
from typing import List
from typing import Optional

from typer import Argument
from typer import Context as TyperContext
from typer import Option

from spinta.cli.helpers.store import load_config
from spinta.components import Context
from spinta.components import Store
from spinta.core.context import configure_context
from spinta.manifests.helpers import create_manifest
from spinta.manifests.tabular.components import AsciiManifest
from spinta.manifests.tabular.components import CsvManifest
from spinta.manifests.tabular.components import TabularManifest
from spinta.manifests.tabular.helpers import read_tabular_manifest


def grep(
    ctx: TyperContext,
    cols: List[str] = Option('cols', help=(
        "Comma separated list of columns, like property,property.url"
    )),
    manifests: Optional[List[str]] = Argument(None, help=(
        "Manifest files to load"
    )),
):
    """Check configuration and manifests"""
    context = configure_context(ctx.obj, manifests)
    for row in _grep(context, cols):
        print(row)


def _grep(context: Context, cols: List[str]) -> Iterator[List[str]]:
    rc = load_config(context).rc
    store: Store = context.get('store')
    store.keymaps['default'] = None
    store.backends['default'] = None
    manifest = rc.get('manifest', required=True)
    manifest = create_manifest(context, store, manifest)

    for manifest in manifest.sync:
        if not isinstance(manifest, TabularManifest):
            raise NotImplementedError(
                f"Manifest of {manifest.type!r} type is not supported."
            )

        file = None
        if isinstance(manifest, (CsvManifest, AsciiManifest)):
            file = manifest.file

        rows = read_tabular_manifest(
            manifest.format,
            path=manifest.path,
            file=file,
        )

        for i, row in rows:
            yield [row.get(col) for col in cols]
            if row.get('type') == 'model' and 'properties' in row:
                for name, prop in row['properties'].items():
                    prop = {
                        'property': name,
                        **prop,
                    }
                    yield [prop.get(col) for col in cols]
