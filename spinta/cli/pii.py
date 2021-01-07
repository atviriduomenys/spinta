import pathlib
import re
import sys
import uuid
from typing import Any
from typing import Iterable
from typing import TextIO

import click
import tqdm

from spinta.cli import main
from spinta.cli.helpers.auth import require_auth
from spinta.cli.helpers.data import ModelRow
from spinta.cli.helpers.data import count_rows
from spinta.cli.helpers.data import iter_model_rows
from spinta.cli.helpers.store import prepare_manifest
from spinta.components import Action
from spinta.components import Context
from spinta.components import Node
from spinta.components import Property
from spinta.core.config import RawConfig
from spinta.dimensions.prefix.components import UriPrefix
from spinta.manifests.components import Manifest
from spinta.manifests.tabular.helpers import datasets_to_tabular
from spinta.manifests.tabular.helpers import write_tabular_manifest
from spinta.types.namespace import sort_models_by_refs
from spinta.utils.data import take


@main.group()
@click.pass_context
def pii(ctx: click.Context):
    pass


def _get_new_metadata_row_id():
    return str(uuid.uuid4())


PREFIXES = {
    'person': 'https://www.w3.org/ns/person#',
    'ppi': 'https://data.gov.lt/ppi/',
}


def _ensure_prefix(node: Node, prefix: str, uri: str) -> str:
    if isinstance(node, Property):
        manifest = node.model.manifest
    else:
        raise RuntimeError("Don't know how to get manifest from {node!r}.")

    if prefix not in manifest.prefixes:
        prefix_ = UriPrefix()
        prefix_.id = _get_new_metadata_row_id()
        prefix_.name = prefix
        prefix_.uri = PREFIXES[prefix]
        manifest.prefixes[prefix] = prefix_

    return f'{prefix}:{uri}'


email_re = re.compile(r'(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)')


def _detect_email(prop: Property, value: Any):
    if isinstance(value, str) and email_re.match(value):
        prop.uri = _ensure_prefix(prop, 'ppi', 'email')


def _detect_pii(rows: Iterable[ModelRow]) -> None:
    """Detects PII and modifies given manifest in place"""
    for model, row in rows:
        for prop in take(model.properties).values():
            if prop.name in row:
                _detect_email(prop, row[prop.name])


def _save_manifest(manifest: Manifest, dest: TextIO):
    # TODO: Currently saving is hardcoded to tabular manifest type, but it
    #       should be possible to save or probably freeze to any manifest type.
    rows = datasets_to_tabular(manifest)
    write_tabular_manifest(dest, rows)


@pii.command(help='Push data to external data store.')
@click.argument('manifest')
@click.option('--output', '-o', help="Path to manifest with detected PII.")
@click.option('--auth', '-a', help="Authorize as client.")
@click.option('--stop-on-error', is_flag=True, default=False, help=(
    "Stop on first error."
))
@click.pass_context
def detect(
    ctx: click.Context,
    manifest: str,
    output: str,
    auth: str,
    stop_on_error: bool,
):
    context = ctx.obj

    config = {
        'backends.cli': {
            'type': 'memory',
        },
        'manifests.cli': {
            'type': 'tabular',
            'path': manifest,
            'keymap': 'default',
            'backend': 'cli',
        },
        'manifest': 'cli',
    }

    # Add given manifest file to configuration
    rc: RawConfig = context.get('rc')
    context: Context = context.fork('detect')
    context.set('rc', rc.fork(config))

    # Load manifest
    store = prepare_manifest(context)
    manifest = store.manifest
    with context:
        require_auth(context, auth)
        context.attach('transaction', manifest.backend.transaction)
        for backend in store.backends.values():
            context.attach(f'transaction.{backend.name}', backend.begin)
        for keymap in store.keymaps.values():
            context.attach(f'keymap.{keymap.name}', lambda: keymap)

        from spinta.types.namespace import traverse_ns_models

        ns = manifest.objects['ns']['']
        models = traverse_ns_models(context, ns, Action.SEARCH)
        models = sort_models_by_refs(models)
        models = list(reversed(list(models)))
        counts = count_rows(context, models)

        rows = iter_model_rows(
            context, models, counts,
            stop_on_error=stop_on_error,
        )
        total = sum(counts.values())
        rows = tqdm.tqdm(rows, 'PII DETECT', ascii=True, total=total)
        _detect_pii(rows)
        if output:
            with pathlib.Path(output).open('w') as f:
                _save_manifest(manifest, f)
        else:
            _save_manifest(manifest, sys.stdout)

