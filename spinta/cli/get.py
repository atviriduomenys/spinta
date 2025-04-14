import logging
from typing import List
from typing import Optional
from typer import echo
from typer import Argument
from typer import Context as TyperContext

from spinta import commands
from spinta.cli.helpers.auth import require_auth
from spinta.cli.helpers.manifest import convert_str_to_manifest_path
from spinta.cli.helpers.store import prepare_manifest, attach_keymaps
from spinta.core.context import configure_context

log = logging.getLogger(__name__)


def getall(
    ctx: TyperContext,
    manifests: Optional[List[str]] = Argument(None, help=("Manifest files to load")),
    backend: str = Argument(None, help=("Backend connection string")),
    model: str = Argument(None, help=("Model path")),
):
    manifests = convert_str_to_manifest_path(manifests)
    context = configure_context(
        ctx.obj, manifests, backend_type="dask/memory", backend=backend
    )
    prepare_manifest(context, ensure_config_dir=True, verbose=False)
    response_dict = {}
    store = context.get("store")
    with context:
        require_auth(context)
        attach_keymaps(context, store)
        model = commands.get_model(context, store.manifest, model)
        response_dict["_data"] = list(
            commands.getall(context, model, store.manifest.backend)
        )
    echo(response_dict)
