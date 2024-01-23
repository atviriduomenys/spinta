from copy import copy
from typing import Any, List
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
from spinta.cli.helpers.manifest import convert_str_to_manifest_path
from spinta.components import Property, Node
from spinta.datasets.components import Dataset, Resource, ExternalBackend
from spinta.datasets.inspect.helpers import create_manifest_from_inspect
from spinta.manifests.tabular.helpers import render_tabular_manifest
from spinta.utils.schema import NA
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
    priority: str = Option('manifest', '-p', '--priority', help=(
        "Merge property priority ('manifest' or 'external')"
    ))
):
    """Update manifest schema from an external data source"""
    if priority not in ['manifest', 'external']:
        echo(f"Priority \'{priority}\' does not exist, there can only be \'manifest\' or \'external\', it will be set to default 'manifest'.")
        priority = 'manifest'

    manifest = convert_str_to_manifest_path(manifest)
    context, manifest = create_manifest_from_inspect(
        context=ctx.obj,
        manifest=manifest,
        resources=resource,
        formula=formula,
        backend=backend,
        auth=auth,
        priority=priority
    )

    if output:
        output_type = output.split(".")[-1]
        config = context.get("config")
        if output_type not in config.exporters:
            echo(f"unknown export file type {output_type!r}")
            raise Exception
        fmt = config.exporters[output_type]
        commands.render(ctx.obj, manifest, fmt, path=output)
    else:
        echo(render_tabular_manifest(context, manifest))
