from typing import Any
from typing import Callable
from typing import Dict
from typing import Hashable
from typing import Iterator
from typing import Optional
from typing import Tuple
from typing import TypeVar
from typing import overload

from typer import Argument
from typer import Context as TyperContext
from typer import Option
from typer import echo

from spinta import commands
from spinta.cli.helpers.store import load_manifest
from spinta.components import Context
from spinta.components import Mode
from spinta.core.config import RawConfig
from spinta.core.config import ResourceTuple
from spinta.core.config import parse_resource_args
from spinta.core.context import configure_context
from spinta.manifests.components import Manifest
from spinta.manifests.components import ManifestPath
from spinta.manifests.helpers import get_manifest_from_type
from spinta.manifests.tabular.helpers import render_tabular_manifest
from spinta.manifests.tabular.helpers import write_tabular_manifest
from spinta.utils.schema import NA
from spinta.utils.schema import NotAvailable
from spinta.components import Model
from spinta.manifests.helpers import load_manifest_nodes


def inspect(
    ctx: TyperContext,
    manifest: Optional[str] = Argument(None, help="Path to manifest."),
    resource: Optional[Tuple[str, str]] = Option(
        (None, None), '-r', '--resource',
        help=(
            "Resource type and source URI "
            "(-r sql sqlite:////tmp/db.sqlite)"
        ),
    ),
    formula: str = Option('', '-f', '--formula', help=(
        "Formula if needed, to prepare resource for reading"
    )),
    backend: Optional[str] = Option(None, '-b', '--backend', help=(
        "Backend connection string"
    )),
    output: Optional[str] = Option(None, '-o', '--output', help=(
        "Output tabular manifest in a specified file"
    )),
    auth: Optional[str] = Option(None, '-a', '--auth', help=(
        "Authorize as a client"
    )),
):
    """Update manifest schema from an external data source"""
    context = configure_context(
        ctx.obj,
        [manifest] if manifest else None,
        mode=Mode.external,
        backend=backend,
    )
    store = load_manifest(context, ensure_config_dir=True)
    old = store.manifest

    manifest = Manifest()
    _merge(context, manifest, old)
    resources = parse_resource_args(*resource, formula)
    if resources:
        for resource in resources:
            schemas = _merge(context, old, resource)
            load_manifest_nodes(context, manifest, schemas)

    if output:
        write_tabular_manifest(output, manifest)
    else:
        echo(render_tabular_manifest(manifest))


def _merge(context: Context, old: Manifest, resource: ResourceTuple):
    rc: RawConfig = context.get('rc')
    Manifest_ = get_manifest_from_type(rc, resource.type)
    path = ManifestPath(type=Manifest_.type, path=resource.external)
    context = configure_context(context, [path], mode=Mode.external)
    store = load_manifest(context)
    new = store.manifest
    yield from commands.merge(context, old, new)


def _model_source_key(model: Model) -> str:
    if model.external and model.external.name:
        return model.external.name
    else:
        return model.name


@overload
@commands.merge.register(Context, Manifest, Manifest)
def merge(context: Context, old: Manifest, new: Manifest) -> None:
    items = zipitems(
        old.models.values(),
        new.models.values(),
        _model_source_key,
    )
    for o, n in items:
        commands.merge(context, o, n)


@overload
@commands.merge.register(Context, NotAvailable, Model)
def merge(context: Context, old: NotAvailable, new: Model) -> None:
    old.title = coalesce(old.title, new.title)
    old.description = coalesce(old.description, new.description)


@overload
@commands.merge.register(Context, Model, Model)
def merge(context: Context, old: Model, new: Model) -> None:
    old.title = coalesce(old.title, new.title)
    old.description = coalesce(old.description, new.description)


TItem = TypeVar('TItem')


def zipitems(
    a: TItem,
    b: TItem,
    key: Callable[[TItem], Hashable],
) -> Iterator[Tuple[TItem, TItem]]:

    res: Dict[
        Hashable,  # key
        Tuple[
            Any,   # a
            Any,   # b
        ],
    ] = {}

    for v in a:
        k = key(v)
        res[k] = [v, NA]

    for v in b:
        k = key(v)
        if k in res:
            res[k][1] = v
        else:
            res[k] = [NA, v]

    yield from res.values()


def coalesce(*args: Any) -> Any:
    for arg in args:
        if arg:
            return arg
    return arg
