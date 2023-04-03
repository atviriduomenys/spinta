from typing import Any
from typing import Callable
from typing import Dict
from typing import Hashable
from typing import Iterator
from typing import Optional
from typing import Tuple
from typing import TypeVar

from typer import Argument
from typer import Context as TyperContext
from typer import Option
from typer import echo

from spinta import commands
from spinta.cli.helpers.store import load_manifest
from spinta.components import Context, Property, Node, Namespace
from spinta.components import Mode
from spinta.core.config import RawConfig
from spinta.core.config import ResourceTuple
from spinta.core.config import parse_resource_args
from spinta.core.context import configure_context
from spinta.datasets.components import Dataset, Resource
from spinta.dimensions.prefix.components import UriPrefix
from spinta.manifests.components import Manifest
from spinta.manifests.components import ManifestPath
from spinta.manifests.helpers import get_manifest_from_type, init_manifest
from spinta.manifests.tabular.helpers import render_tabular_manifest
from spinta.manifests.tabular.helpers import write_tabular_manifest
from spinta.utils.schema import NA
from spinta.utils.schema import NotAvailable
from spinta.components import Model


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
    init_manifest(context, manifest, 'inspect')
    commands.merge(context, manifest, manifest, old)

    resources = parse_resource_args(*resource, formula)
    if resources:
        for resource in resources:
            _merge(context, manifest, old, resource)

    if output:
        write_tabular_manifest(output, manifest)
    else:
        echo(render_tabular_manifest(manifest))


def _merge(context: Context, manifest: Manifest, old: Manifest, resource: ResourceTuple):
    rc: RawConfig = context.get('rc')
    Manifest_ = get_manifest_from_type(rc, resource.type)
    path = ManifestPath(type=Manifest_.type, path=resource.external)
    context = configure_context(context, [path], mode=Mode.external)
    store = load_manifest(context)
    new = store.manifest
    commands.merge(context, manifest, old, new)


@commands.merge.register(Context, Manifest, Manifest, Manifest)
def merge(context: Context, manifest: Manifest, old: Manifest, new: Manifest) -> None:
    datasets = zipitems(
        old.datasets.values(),
        new.datasets.values(),
        _name_key,
    )
    for o, n in datasets:
        commands.merge(context, manifest, o, n)

    models = zipitems(
        old.models.values(),
        new.models.values(),
        _model_source_key,
    )
    for o, n in models:
        commands.merge(context, manifest, o, n)


@commands.merge.register(Context, Manifest, NotAvailable, UriPrefix)
def merge(context: Context, manifest: Manifest, old: NotAvailable, new: UriPrefix) -> None:
    old = UriPrefix()
    old.type = new.type
    old.id = new.id
    old.name = new.name
    old.uri = new.uri
    old.title = new.title
    old.description = new.description

    dataset = new.parent
    manifest.datasets[dataset.name].prefixes[old.name] = old


@commands.merge.register(Context, Manifest, UriPrefix, UriPrefix)
def merge(context: Context, manifest: Manifest, old: UriPrefix, new: UriPrefix) -> None:
    dataset = old.parent
    old.type = coalesce(new.type, old.type)
    old.id = coalesce(new.id, old.id)
    old.name = coalesce(new.name, old.name)
    old.uri = coalesce(new.uri, old.uri)
    old.title = coalesce(new.title, old.title)
    old.description = coalesce(new.description, old.description)
    manifest.datasets[dataset.name].prefixes[old.name] = old


@commands.merge.register(Context, Manifest, UriPrefix, NotAvailable)
def merge(context: Context, manifest: Manifest, old: UriPrefix, new: NotAvailable) -> None:
    dataset = old.parent
    manifest.datasets[dataset.name].prefixes.pop(old.name)
    old.name = f'_{old.name}'
    manifest.datasets[dataset.name].prefixes[old.name] = old


@commands.merge.register(Context, Manifest, NotAvailable, Namespace)
def merge(context: Context, manifest: Manifest, old: NotAvailable, new: Namespace) -> None:
    old = Namespace()
    old.type = new.type
    old.name = new.name
    old.title = new.title
    old.description = new.description
    old.enums = new.enums

    manifest.namespaces[old.name] = old


@commands.merge.register(Context, Manifest, Namespace, Namespace)
def merge(context: Context, manifest: Manifest, old: Namespace, new: Namespace) -> None:
    old.type = coalesce(new.type, old.type)
    old.name = coalesce(new.name, old.name)
    old.title = coalesce(new.title, old.title)
    old.description = coalesce(new.description, old.description)
    old.enums = coalesce(new.enums, old.enums)

    manifest.namespaces[old.name] = old


@commands.merge.register(Context, Manifest, Namespace, NotAvailable)
def merge(context: Context, manifest: Manifest, old: Namespace, new: NotAvailable) -> None:
    manifest.namespaces.pop(old.name)
    old.name = f'_{old.name}'
    manifest.namespaces[old.name] = old


@commands.merge.register(Context, Manifest, NotAvailable, Dataset)
def merge(context: Context, manifest: Manifest, old: NotAvailable, new: Dataset) -> None:
    old = Dataset()
    old.id = new.id
    old.name = new.name
    old.title = new.title
    old.description = new.description
    old.level = new.level
    old.access = new.access
    old.given = new.given
    old.lang = new.lang
    old.resources = new.resources
    old.ns = new.ns
    old.prefixes = new.prefixes

    manifest.datasets[old.name] = old


@commands.merge.register(Context, Manifest, Dataset, Dataset)
def merge(context: Context, manifest: Manifest, old: Dataset, new: Dataset) -> None:
    old.id = coalesce(new.id, old.id)
    old.name = coalesce(new.name, old.name)
    old.title = coalesce(new.title, old.title)
    old.description = coalesce(new.description, old.description)
    old.level = coalesce(new.level, old.level)
    old.access = coalesce(new.access, old.access)
    old.given = coalesce(new.given, old.given)
    old.lang = coalesce(new.lang, old.lang)
    if old.ns and new.ns and old.ns.name == new.ns.name:
        commands.merge(context, manifest, old.ns, new.ns)
        old.ns = manifest.namespaces[old.ns.name]
    else:
        old.ns = coalesce(new.ns, old.ns)
    manifest.datasets[old.name] = old

    _merge_prefixes(context, manifest, old, new)
    _merge_resources(context, manifest, old, new)


@commands.merge.register(Context, Manifest, Dataset, NotAvailable)
def merge(context: Context, manifest: Manifest, old: Dataset, new: NotAvailable) -> None:
    manifest.datasets.pop(old.name)


@commands.merge.register(Context, Manifest, NotAvailable, Resource)
def merge(context: Context, manifest: Manifest, old: NotAvailable, new: Resource) -> None:
    dataset = new.dataset
    old = Resource()
    old.name = new.name
    old.external = new.external
    old.prepare = new.prepare
    old.type = new.type
    old.backend = new.backend
    old.level = new.level
    old.given = new.given
    old.access = new.access
    old.title = new.title
    old.description = new.description
    old.comments = new.comments
    old.lang = new.lang
    manifest.datasets[dataset.name].resources[new.name] = old


@commands.merge.register(Context, Manifest, Resource, Resource)
def merge(context: Context, manifest: Manifest, old: Resource, new: Resource) -> None:
    dataset = old.dataset
    old.name = coalesce(new.name, old.name)
    old.external = coalesce(new.external, old.external)
    old.prepare = coalesce(new.prepare, old.prepare)
    old.type = coalesce(new.type, old.type)
    old.backend = coalesce(new.backend, old.backend)
    old.level = coalesce(new.level, old.level)
    old.given = coalesce(new.given, old.given)
    old.access = coalesce(new.access, old.access)
    old.title = coalesce(new.title, old.title)
    old.description = coalesce(new.description, old.description)
    old.comments = coalesce(new.comments, old.comments)
    old.lang = coalesce(new.lang, old.lang)
    manifest.datasets[dataset.name].resources[new.name] = old


@commands.merge.register(Context, Manifest, Resource, NotAvailable)
def merge(context: Context, manifest: Manifest, old: Resource, new: NotAvailable) -> None:
    dataset = old.dataset
    manifest.datasets[dataset.name].resources.pop(old.name)
    old.name = f'_{old.name}'
    manifest.datasets[dataset.name].resources[old.name] = old


@commands.merge.register(Context, Manifest, NotAvailable, Model)
def merge(context: Context, manifest: Manifest, old: NotAvailable, new: Model) -> None:
    old = Model()
    old.id = new.id
    old.eid = new.eid
    old.title = new.title
    old.name = new.name
    old.description = new.description
    old.access = new.access
    old.level = new.level
    old.external = new.external
    old.comments = new.comments
    old.lang = new.lang
    old.base = new.base

    if old.external and old.external.dataset:
        dataset = old.external.dataset.name
        old.external.dataset = manifest.datasets[dataset]

    manifest.models[old.name] = old

    _merge_model_properties(context, manifest, old, new)


@commands.merge.register(Context, Manifest, Model, Model)
def merge(context: Context, manifest: Manifest, old: Model, new: Model) -> None:
    old.id = coalesce(new.id, old.id)
    old.title = coalesce(new.title, old.title)
    old.description = coalesce(new.description, old.description)
    old.eid = coalesce(new.eid, old.eid)
    old.access = coalesce(new.access, old.access)
    old.level = coalesce(new.level, old.level)
    old.external = coalesce(new.external, old.external)
    old.comments = coalesce(new.comments, old.comments)
    old.lang = coalesce(new.lang, old.lang)
    old.base = coalesce(new.base, old.base)

    if old.external and old.external.dataset:
        dataset = old.external.dataset.name
        old.external.dataset = manifest.datasets[dataset]

    manifest.models[old.name] = old

    _merge_model_properties(context, manifest, old, new)


@commands.merge.register(Context, Manifest, Model, NotAvailable)
def merge(context: Context, manifest: Manifest, old: Model, new: NotAvailable) -> None:
    manifest.models.pop(old.name)
    old.name = f'_{old.name}'
    manifest.models[old.name] = old


@commands.merge.register(Context, Manifest, NotAvailable, Property)
def merge(context: Context, manifest: Manifest, old: NotAvailable, new: Property) -> None:
    old = Property()
    old.name = new.name
    old.title = new.title
    old.place = new.place
    old.access = new.access
    old.type = new.type
    old.given = new.given
    old.uri = new.uri
    old.level = new.level
    old.description = new.description
    old.dtype = new.dtype
    old.external = new.external
    old.enum = new.enum
    old.enums = new.enums
    old.unit = new.unit
    old.comments = new.comments
    old.lang = new.lang
    old.model = manifest.models[new.model.name]

    manifest.models[new.model.name].properties[old.name] = old


@commands.merge.register(Context, Manifest, Property, Property)
def merge(context: Context, manifest: Manifest, old: Property, new: Property) -> None:
    old.name = coalesce(new.name, old.name)
    old.title = coalesce(new.title, old.title)
    old.place = coalesce(new.place, old.place)
    old.access = coalesce(new.access, old.access)
    old.type = coalesce(new.type, old.type)
    old.given = coalesce(new.given, old.given)
    old.uri = coalesce(new.uri, old.uri)
    old.level = coalesce(new.level, old.level)
    old.description = coalesce(new.description, old.description)
    old.dtype = coalesce(new.dtype, old.dtype)
    old.external = coalesce(new.external, old.external)
    old.enum = coalesce(new.enum, old.enum)
    old.enums = coalesce(new.enums, old.enums)
    old.unit = coalesce(new.unit, old.unit)
    old.comments = coalesce(new.comments, old.comments)
    old.lang = coalesce(new.lang, old.lang)

    manifest.models[old.model.name].properties[old.name] = old


@commands.merge.register(Context, Manifest, Property, NotAvailable)
def merge(context: Context, manifest: Manifest, old: Property, new: NotAvailable) -> None:
    manifest.models[old.model.name].properties.pop(old.name)
    old.name = f"_{old.name}"
    old.place = f"_{old.place}"
    manifest.models[old.model.name].properties[old.name] = old


def _merge_model_properties(
    context: Context,
    manifest: Manifest,
    old: Model,
    new: Model
):
    properties = zipitems(
        old.properties.values(),
        new.properties.values(),
        _name_key,
    )
    for o, n in properties:
        commands.merge(context, manifest, o, n)


def _merge_prefixes(
    context: Context,
    manifest: Manifest,
    old: Dataset,
    new: Dataset,
):
    prefixes = zipitems(
        old.prefixes.values(),
        new.prefixes.values(),
        _name_key,
    )
    for o, n in prefixes:
        commands.merge(context, manifest, o, n)


def _merge_resources(
    context: Context,
    manifest: Manifest,
    old: Dataset,
    new: Dataset,
):
    resources = zipitems(
        old.resources.values(),
        new.resources.values(),
        _name_key,
    )
    for o, n in resources:
        commands.merge(context, manifest, o, n)


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


def _name_key(node: Node) -> str:
    return node.name


def _model_source_key(model: Model) -> str:
    if model.external and model.external.name:
        return model.external.name
    else:
        return model.name
