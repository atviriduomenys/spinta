"""JSON-LD helpers for the renderers (`.tmp/specs/json-ld.md`, rules 4, 6, decisions 10.2, 10.16).

- `jsonld_enabled`: whether a response is enriched, from its negotiated format.
- `context_value`: the `@context` of an object of a model, a URL or the inline context (10.16).
- `protocol_context_value`: the `@context` of a collection, `:all` or batch response.
- `enrich_object`: the JSON-LD keys of one model object (rule 3), added to its data.
- `enrich_refs`: the keys of every `ref` value in an object, at any depth (rule 7, 3.2).
"""

from __future__ import annotations

import logging
from typing import Any

from spinta.components import Context, Model
from spinta.formats.components import Format
from spinta.formats.json.components import Json
from spinta.formats.jsonld.components import JsonLd
from spinta.manifests.jsonld.context import ModelContext, in_scope
from spinta.manifests.simple.implied import uri_context
from spinta.types.datatype import Array, Object, Ref

log = logging.getLogger(__name__)

EMPTY_URI_WARNING = (
    "`uri` is empty, so `@id`, `@type` and the term IRIs of JSON-LD output are relative and do not resolve. "
    "The output is not valid linked data until `uri` is set, for example `SPINTA_URI=https://data.gov.lt`."
)

# Set once the empty `uri` warning is logged at request time (see `_warn_empty_uri`).
_warned_empty_uri = False


def jsonld_enabled(context: Context, fmt: Format) -> bool:
    """Tell whether a response in format `fmt` is enriched with JSON-LD.

    The format is already negotiated from the `Accept` header and the
    `/:format/...` path (`get_response_type`), so the request is not needed.

    - `JsonLd` (`Accept: application/ld+json`, `/:format/jsonld`): always.
    - `Json` (`Accept: application/json`, `/:format/json`): only when the
      `jsonld` config switch is on.
    - Any other format: never.
    """
    if isinstance(fmt, JsonLd):
        return True
    if isinstance(fmt, Json):
        return bool(context.get("config").jsonld)
    return False


def jsonld_context_url(context: Context) -> str:
    """The base of the context URLs, without a trailing `/`. Empty when the contexts are inline (10.16)."""
    return (context.get("config").jsonld_context_url or "").rstrip("/")


def context_value(context: Context, model: Model) -> str | dict[str, Any] | None:
    """The `@context` of an object of `model` (10.16).

    A URL, `{jsonld_context_url}/{model path}.json`, when `jsonld_context_url` is set. Otherwise the inline model
    context (`ModelContext`, the same as `spinta copy` writes), which is built once and cached. The inline dict is
    shared: a caller must not change it.

    A `protected` or `private` model has no context (10.2), so the value is `None`.
    """
    _warn_empty_uri(context)
    if not in_scope(model):
        return None
    base = jsonld_context_url(context)
    if base:
        return f"{base}/{model.name}.json"
    return _inline_context(context, model)


def protocol_context_value(context: Context) -> str | dict[str, Any]:
    """The top-level `@context` of a collection, `:all` or batch response (10.4, 10.16).

    `{jsonld_context_url}/_uapi.json` when `jsonld_context_url` is set, else the inline `{"_data": "@graph"}`.
    """
    _warn_empty_uri(context)
    base = jsonld_context_url(context)
    if base:
        return f"{base}/_uapi.json"
    return {"_data": "@graph"}


def enrich_object(context: Context, model: Model, data: dict[str, Any]) -> dict[str, Any]:
    """Add `@context`, `@id` and `@type` to one object of `model` (rules 3 and 7, 3.2).

    The three keys come first. The keys of `data` keep their order and values. A `protected` or `private` model is not
    enriched (10.2), so `data` is returned as it is. A `select(...)` that leaves out `_id` gives no `@id` (rule 3). The
    object is then a blank node in RDF, typed by `@type` (10.17). That is expected, so nothing is logged.
    """
    if not in_scope(model):
        return data
    data = enrich_refs(context, model, data)
    uris = uri_context(context, model.manifest)
    keys: dict[str, Any] = {"@context": context_value(context, model)}
    if data.get("_id") is not None:
        keys["@id"] = uris.instance_id(model, data["_id"])
    keys["@type"] = uris.class_iri(model)
    return {**keys, **data}


def enrich_refs(context: Context, model: Model, data: dict[str, Any]) -> dict[str, Any]:
    """Enrich every `ref` value of an object of `model`, at any depth (rule 7, 3.2).

    A `ref` value is a model object of its target model, so it gets the same keys as a top-level object. The walk goes
    through nested objects and arrays, because a `ref` can be there too. `data` is not changed: a new dict is returned.
    """
    return _enrich_properties(context, model.properties, data)


def _enrich_properties(context: Context, properties: dict[str, Any] | None, data: dict[str, Any]) -> dict[str, Any]:
    result = dict(data)
    for key, prop in (properties or {}).items():
        if key in result:
            result[key] = _enrich_value(context, prop.dtype, result[key])
    return result


def _enrich_value(context: Context, dtype: Any, value: Any) -> Any:
    if value is None:
        return None
    if isinstance(dtype, Ref):
        return _enrich_ref(context, dtype.model, value)
    if isinstance(dtype, Array) and dtype.items is not None and isinstance(value, list):
        return [_enrich_value(context, dtype.items.dtype, item) for item in value]
    if isinstance(dtype, Object) and isinstance(value, dict):
        return _enrich_properties(context, dtype.properties, value)
    return value


def _enrich_ref(context: Context, target: Any, value: Any) -> Any:
    """The keys of one `ref` value, a model object of `target` (rule 7, 3.2).

    Left as it is when the value is not an object, when `target` is `protected` or `private` (10.2), or when `_id` is
    null: there is no target, so no `@id` can be built (task 8). A value without `_id` (a `select(...)` that leaves it
    out) gets the keys without `@id`, and is a blank node in RDF (rule 7, 10.17).
    """
    if not isinstance(value, dict) or not isinstance(target, Model) or not in_scope(target):
        return value
    if "_id" in value and value["_id"] is None:
        return value
    uris = uri_context(context, target.manifest)
    keys: dict[str, Any] = {"@context": context_value(context, target)}
    if "_id" in value:
        keys["@id"] = uris.instance_id(target, value["_id"])
    keys["@type"] = uris.class_iri(target)
    return {**keys, "_type": target.name, **value}


def _inline_context(context: Context, model: Model) -> dict[str, Any]:
    # The inline context is cached on the model, which lives as long as its manifest. The `uri` it was built for is
    # kept with it, so a changed `uri` (in a test) builds it again. Models are not hashable, so no dict is used.
    uri = context.get("config").uri or ""
    cached = getattr(model, "_jsonld_context", None)
    if cached is not None and cached[0] == uri:
        return cached[1]
    manifest = model.manifest
    document = ModelContext(uri_context(context, manifest), manifest, model).document()["@context"]
    model._jsonld_context = (uri, document)
    return document


def _warn_empty_uri(context: Context) -> None:
    """Log the empty `uri` warning once, for a JSON-LD response that is not enriched by the switch.

    With the switch on, the startup check (`spinta.types.config.check`) has already logged it. With the switch off, the
    response is JSON-LD only because of `Accept: application/ld+json` or `/:format/jsonld`, and the startup check does
    not run, so the first such response logs it here.
    """
    global _warned_empty_uri
    config = context.get("config")
    if _warned_empty_uri or config.jsonld or (config.uri or "").rstrip("/"):
        return
    _warned_empty_uri = True
    log.warning(EMPTY_URI_WARNING)
