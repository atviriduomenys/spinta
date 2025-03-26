import logging
from typing import List
from typing import Optional
from typer import echo
from typer import Argument
from typer import Context as TyperContext

from spinta import commands
from spinta.cli.helpers.manifest import convert_str_to_manifest_path
from spinta.cli.helpers.store import prepare_manifest
from spinta.core.context import configure_context

log = logging.getLogger(__name__)


def getall(
    ctx: TyperContext,
    manifests: Optional[List[str]] = Argument(help=("Manifest files to load")),
    dsn: str = Argument(help=("Data Source Name path")),
    model: str = Argument(help=("Model path")),
):
    manifests = convert_str_to_manifest_path(manifests)
    context = configure_context(ctx.obj, manifests, backend="memory", dsn=dsn)
    prepare_manifest(context, ensure_config_dir=True, verbose=False)
    response_dict = {}
    store = context.get("store")
    model = commands.get_model(context, store.manifest, dataset)
    response_dict["_data"] = list(
        commands.getall(context, model, store.manifest.backend)
    )
    echo(response_dict)


def getone(
    ctx: TyperContext,
    manifests: Optional[List[str]] = Argument(help=("Manifest files to load")),
    dsn: str = Argument(help=("Data Source Name path")),
    model: str = Argument(help=("Model path")),
    id_: str = Argument(help=("Dataset model id")),
):
    manifests = convert_str_to_manifest_path(manifests)
    context = configure_context(ctx.obj, manifests, backend="memory", dsn=dsn)
    prepare_manifest(context, ensure_config_dir=True, verbose=False)
    store = context.get("store")
    model = commands.get_model(context, store.manifest, dataset)
    result = {}
    try:
        echo(commands.getone(context, model, store.manifest.backend, id_=id_))
    except KeyError:
        echo(result)
