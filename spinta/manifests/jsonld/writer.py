"""Write the JSON-LD context documents of a manifest (`.tmp/specs/json-ld.md`, sections 3.3 and 9, decisions 10.13, 10.15).

The output depends only on the manifest, never on a request (rule 5). Its IRIs are the ones the `simple` output gives
(`UriContext`, the same as `implied.uri`), so the same call gives the same IRIs in both outputs.
"""

from __future__ import annotations

import logging
from typing import Any

import click

from spinta import commands
from spinta.components import Context, Model
from spinta.manifests.components import Manifest
from spinta.manifests.jsonld.context import ModelContext, in_scope
from spinta.manifests.simple.helpers import canonical_json
from spinta.manifests.simple.implied import uri_context

log = logging.getLogger(__name__)


def render_jsonld_manifest(
    context: Context,
    manifest: Manifest,
    *,
    dataset: str | None = None,
    model: str | None = None,
) -> str:
    """The JSON-LD context documents of a manifest, as canonical JSON (`Spinta.copy` to `jsonld`).

    Without `model`, the result is one object: each key is the name of a model in scope, and each value is that
    model's context document. A model is in scope if it is not `protected` or `private`, and, with `dataset`, if it
    belongs to that dataset. Datasets with no models are not in it.

    With `model`, the result is that model's context document itself (`{"@context": {...}}`), so it can be hosted
    as `{uri}/context/{model path}.jsonld`. A model that is unknown, not in scope or outside `dataset` raises
    `ValueError`.
    """
    if dataset is not None and not commands.has_dataset(context, manifest, dataset):
        available = ", ".join(sorted(commands.get_datasets(context, manifest))) or "none"
        raise click.UsageError(f"Dataset {dataset!r} not found in manifest. Available datasets: {available}.")
    uris = uri_context(context, manifest)
    if not uris.uri:
        log.warning(
            "`uri` is empty, so the JSON-LD contexts have relative IRIs, which are not valid linked data. "
            "Set `uri`, for example `SPINTA_URI=https://data.gov.lt`."
        )
    models = commands.get_models(context, manifest)

    if model is not None:
        node = models.get(model)
        if node is None:
            raise ValueError(f"Model {model!r} not found in the manifest (3.1).")
        if not in_scope(node):
            raise ValueError(f"Model {model!r} has visibility {node.visibility.name}, so it has no context (10.2).")
        if dataset is not None and _dataset_name(uris, node) != dataset:
            raise ValueError(f"Model {model!r} is not in dataset {dataset!r} (3.1).")
        return canonical_json(_model_document(uris, manifest, node))

    selected = [
        node for node in models.values() if in_scope(node) and (dataset is None or _dataset_name(uris, node) == dataset)
    ]
    return canonical_json({node.name: _model_document(uris, manifest, node) for node in selected})


def _model_document(uris, manifest: Manifest, model: Model) -> dict[str, Any]:
    return ModelContext(uris, manifest, model).document()


def _dataset_name(uris, model: Model) -> str | None:
    dataset = uris.dataset(model)
    return dataset.name if dataset is not None else None
