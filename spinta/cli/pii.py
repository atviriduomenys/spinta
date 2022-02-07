import pathlib
import re
import uuid
from typing import Any
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import TypedDict

import phonenumbers
import tqdm
from phonenumbers import NumberParseException
from typer import Context as TyperContext
from typer import Argument
from typer import Option
from typer import Typer
from typer import echo

from spinta.cli.helpers.auth import require_auth
from spinta.cli.helpers.data import ModelRow
from spinta.cli.helpers.data import count_rows
from spinta.cli.helpers.data import iter_model_rows
from spinta.cli.helpers.store import prepare_manifest
from spinta.components import Action
from spinta.components import Context
from spinta.components import Model
from spinta.components import Node
from spinta.components import Property
from spinta.core.config import RawConfig
from spinta.dimensions.prefix.components import UriPrefix
from spinta.manifests.components import Manifest
from spinta.manifests.tabular.helpers import render_tabular_manifest
from spinta.manifests.tabular.helpers import write_tabular_manifest
from spinta.types.namespace import sort_models_by_refs
from spinta.utils.data import take
from spinta.utils.nin import is_nin_lt


app = Typer()


def _get_new_metadata_row_id():
    return str(uuid.uuid4())


PREFIXES = {
    'person': 'https://www.w3.org/ns/person#',
    'pii': 'https://data.gov.lt/pii/',
}


def _ensure_prefix(node: Node, uri: str) -> None:
    if isinstance(node, Property):
        manifest = node.model.manifest
    else:
        raise RuntimeError("Don't know how to get manifest from {node!r}.")

    prefix, _ = uri.split(':', 1)
    if prefix not in manifest.prefixes:
        prefix_ = UriPrefix()
        prefix_.id = _get_new_metadata_row_id()
        prefix_.name = prefix
        prefix_.uri = PREFIXES[prefix]
        manifest.prefixes[prefix] = prefix_


class PiiMatch(TypedDict):
    num: int    # Number of rows matches
    total: int  # Total number of rows checked


PiiMatches = Dict[
    str,            # model.name
    Dict[
        str,        # property.place
        Dict[
            str,    # PII URI in `prefix:suffix` form.
            PiiMatch,
        ],
    ]
]


def _add_pii_match(
    matches: PiiMatches,
    model: Model,
    prop: Property,
    pii_: str,
    matched: bool,
) -> None:
    if model.name not in matches:
        matches[model.name] = {}

    if prop.place not in matches[model.name]:
        matches[model.name][prop.place] = {}

    if pii_ not in matches[model.name][prop.place]:
        matches[model.name][prop.place][pii_] = {
            'num': 0,
            'total': 0,
        }

    matches[model.name][prop.place][pii_]['total'] += 1

    if matched:
        matches[model.name][prop.place][pii_]['num'] += 1


email_re = re.compile(r'(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)')


def _detect_email(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    if '@' not in value:
        return False
    return email_re.match(value) is not None


def _detect_phone(value: Any) -> bool:
    if isinstance(value, str):
        try:
            phone = phonenumbers.parse(value, 'LT')
        except NumberParseException:
            return False
        else:
            return phonenumbers.is_valid_number(phone)
    return False


def _detect_nin_lt(value: Any):
    return is_nin_lt(str(value))


def _detect_pii(manifest: Manifest, rows: Iterable[ModelRow]) -> None:
    """Detects PII and modifies given manifest in place"""

    detectors = [
        (_detect_nin_lt, 'pii:id'),
        (_detect_email, 'pii:email'),
        (_detect_phone, 'pii:phone'),
    ]

    # Detect PII properties.
    result: PiiMatches = {}
    for model, row in rows:
        for prop in take(model.properties).values():
            if prop.name in row:
                value = row[prop.name]
                for detector, uri in detectors:
                    if _add_pii_match(
                        result,
                        model,
                        prop,
                        uri,
                        detector(value),
                    ):
                        break

    # Update manifest.
    for model_name, props in result.items():
        model = manifest.models[model_name]
        for prop_place, matches in props.items():
            prop = model.flatprops[prop_place]
            for uri, match in matches.items():
                percent = match['num'] / match['total'] * 100
                if percent > 50:
                    _ensure_prefix(prop, uri)
                    prop.uri = uri


@app.command()
def detect(
    ctx: TyperContext,
    manifest: Optional[pathlib.Path] = Argument(None, help="Path to manifest."),
    output: Optional[str] = Option(None, '-o', '--output', help=(
        "Path to manifest with detected PII."
    )),
    auth: Optional[str] = Option(None, '-a', '--auth', help=(
        "Authorize as a client"
    )),
    stop_on_error: bool = Option(False, help="Stop on first error"),
    limit: Optional[int] = Option(1000, help="Limit number of rows to check"),
):
    """Detect Person Identifying Information

    This will inspect row values and checks if any of value detection functions
    returns true. For those columns having more than 50% of values matching the
    checks a uri value is set.
    """
    context: Context = ctx.obj

    if manifest:
        config = {
            'backends.cli': {
                'type': 'memory',
            },
            'keymaps.default': {
                'type': 'sqlalchemy',
                'dsn': 'sqlite:///{data_dir}/keymap.db',
            },
            'manifests.cli': {
                'type': 'tabular',
                'path': str(manifest),
                'backend': 'cli',
                'keymap': 'default',
                'mode': 'external',
            },
            'manifest': 'cli',
        }

        # Add given manifest file to configuration
        rc: RawConfig = context.get('rc')
        context: Context = context.fork('detect')
        context.set('rc', rc.fork(config))

    # Load manifest
    store = prepare_manifest(context, verbose=False)
    manifest = store.manifest
    with context:
        require_auth(context, auth)
        context.attach('transaction', manifest.backend.transaction)
        backends = set()
        for backend in store.backends.values():
            backends.add(backend.name)
            context.attach(f'transaction.{backend.name}', backend.begin)
        for backend in manifest.backends.values():
            backends.add(backend.name)
            context.attach(f'transaction.{backend.name}', backend.begin)
        for dataset in manifest.datasets.values():
            for resource in dataset.resources.values():
                if resource.backend and resource.backend.name not in backends:
                    backends.add(resource.backend.name)
                    context.attach(f'transaction.{resource.backend.name}', resource.backend.begin)
        for keymap in store.keymaps.values():
            context.attach(f'keymap.{keymap.name}', lambda: keymap)

        from spinta.types.namespace import traverse_ns_models

        ns = manifest.objects['ns']['']
        models = traverse_ns_models(context, ns, Action.SEARCH)
        models = sort_models_by_refs(models)
        models = list(reversed(list(models)))
        counts = count_rows(context, models, limit=limit)

        rows = iter_model_rows(
            context, models, counts,
            stop_on_error=stop_on_error,
            limit=limit,
        )
        total = sum(counts.values())
        rows = tqdm.tqdm(rows, 'PII DETECT', ascii=True, total=total)
        _detect_pii(manifest, rows)
        if output:
            write_tabular_manifest(output, manifest)
        else:
            echo(render_tabular_manifest(manifest))
