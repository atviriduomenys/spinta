"""Inheritance of metadata from base models, in the link phase of `copy` and `check`.

A model or a property that does not set `status`, `visibility`, `access`, `uri`, `eli`, `title` or
`description` takes the value from its nearest base that sets it:

- a model, from its base model (its `base` row);
- a property, from the property of its model's base with the same place.

Datasets and resources are not inherited. "Sets" means the value is given, or inherited from a base in turn.
A base's own `access` is taken before the access of the element's owner, which is the fallback that
`link_access_param` already has.

The inherited values go into the nodes, before the nodes are linked. So the writers, and `implied` of a
`simple` document, see them as if they were given. The `given` cells of a `simple` document are not changed.
Readers and writers do not know about inheritance: the nodes are the only place it is applied.

A `uri` is a CURIE (`locn:Country`) or an IRI. An inherited CURIE keeps its prefix in the target dataset when
that dataset has the same IRI under another name (that prefix is reused), or the same name when the name is
free (the prefix is added to the dataset, so that the CURIE is valid there). Otherwise the IRI is written in full.

Inheritance is not part of the server. Only `copy` and `check` run it, through `load_document` and
`load_manifest(inherit=True)`.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from spinta import commands
from spinta.components import Model, Property
from spinta.core.enums import Access, Status, Visibility
from spinta.dimensions.prefix.components import UriPrefix
from spinta.manifests.open_api.openapi_config import KNOWN_PREFIXES
from spinta.utils.enums import get_enum_by_name

# The fields that a model and a property can inherit (as `implied` names them).
MODEL_FIELDS = ("title", "description", "uri", "eli", "status", "visibility", "access")
PROPERTY_FIELDS = MODEL_FIELDS


@dataclass
class Values:
    """What an element sets, given or inherited. `None` is a value that is not set.

    `uri` is kept as written, and `iri` is the same URI with its prefix expanded
    in the context of the element it belongs to, so that it can be written
    with another prefix in another dataset.
    """

    title: str | None = None
    description: str | None = None
    uri: str | None = None
    iri: str | None = None
    eli: str | None = None
    status: Status | None = None
    visibility: Visibility | None = None
    access: str | None = None


@dataclass
class ModelSource:
    """The values of a model, and of its properties (by place).

    Kept for a default base (`copy -b`), whose model is not loaded: the models that have it as their base
    take the values from here.
    """

    values: Values
    properties: dict[str, Values]


def source_of_model(context, manifest, model: Model) -> ModelSource:
    """The values of a loaded model, as they are once inherited (for a default base, see `bases.py`).

    Reads the nodes only. The base manifest is linked, so its values are the
    ones that its own inheritance gave.
    """
    dataset = _dataset_of(model, commands.get_datasets(context, manifest))
    return ModelSource(
        values=_values_of(model, dataset, manifest),
        properties=_property_values(model, dataset, manifest),
    )


def inherit_manifest(context, manifest) -> None:
    """Give each model and property of `manifest` the values it does not set, from its nearest base (see the module doc).

    Runs before the nodes are linked, and before `link` sets the access of each element. Names of links are
    resolved here, because they are not resolved until `link`. Does not raise for a link that cannot be
    resolved: the link errors are reported by `link`.
    """
    inheritance = _Inheritance(context, manifest)
    for model in inheritance.models.values():
        if not model.name.startswith("_"):
            inheritance.model(model)


class _Inheritance:
    """One pass of `inherit_manifest`. Each model is inherited once; `_busy` breaks a cycle of bases."""

    def __init__(self, context, manifest) -> None:
        self.manifest = manifest
        self.datasets: dict[str, Any] = commands.get_datasets(context, manifest)
        self.models: dict[str, Model] = commands.get_models(context, manifest)
        self._done: dict[str, ModelSource] = {}
        self._busy: set[str] = set()

    def model(self, model: Model) -> ModelSource | None:
        if model.name in self._done:
            return self._done[model.name]
        if model.name in self._busy:
            return None
        self._busy.add(model.name)
        try:
            dataset = _dataset_of(model, self.datasets)
            base = self._base_source(model)
            if base is not None:
                _apply(model, base.values, MODEL_FIELDS, dataset, self.manifest)
                for place, prop in _flat_properties(model):
                    base_prop = base.properties.get(place)
                    if base_prop is not None:
                        _apply(prop, base_prop, PROPERTY_FIELDS, dataset, self.manifest)
            source = ModelSource(
                values=_values_of(model, dataset, self.manifest),
                properties=_property_values(model, dataset, self.manifest),
            )
        finally:
            self._busy.discard(model.name)
        self._done[model.name] = source
        return source

    def _base_source(self, model: Model) -> ModelSource | None:
        """The values of the base of `model`: a default base's, or a loaded base's once it is inherited."""
        if model.base is None:
            return None
        if model.base.default:
            return model.base.source
        base_model = self._base_model_node(model)
        return self.model(base_model) if base_model is not None else None

    def _base_model_node(self, model: Model) -> Model | None:
        """The loaded model that `model` is based on. Its link is a name until `link`, and a node after."""
        parent = model.base.parent
        if isinstance(parent, Model):
            node = parent
        elif isinstance(parent, str):
            node = self.models.get(parent)
        else:
            node = None
        return None if node is None or node is model else node


def _name(value: Any) -> str | None:
    """The name of a link, which is a name before `link` and a node after it. `None` when there is none."""
    if value is None or value == "":
        return None
    return value if isinstance(value, str) else value.name


def _dataset_of(model: Model, datasets: dict[str, Any]):
    external = model.external
    name = _name(external.dataset) if external is not None else None
    return datasets.get(name) if name is not None else None


def _flat_properties(model: Model) -> list[tuple[str, Property]]:
    """The properties of a model that can inherit: the flat ones, without the built-in `_` properties."""
    return [(place, prop) for place, prop in model.flatprops.items() if not place.startswith("_")]


def _property_values(model: Model, dataset, manifest) -> dict[str, Values]:
    return {place: _values_of(prop, dataset, manifest) for place, prop in _flat_properties(model)}


def _set_or_none(value: Any) -> Any:
    """The value of a cell or a node attribute, or `None` when it sets nothing (`""`, `None`, `absent`)."""
    if value is None or value == "" or getattr(value, "name", None) == "absent":
        return None
    return value


def _access_name(value: Any) -> str | None:
    if isinstance(value, Access):
        return value.name
    return _set_or_none(value)


def _values_of(node: Any, dataset, manifest) -> Values:
    """What `node` sets, as it is now: a model or a property."""
    given = getattr(node, "given", None)
    uri = _set_or_none(getattr(node, "uri", None))
    return Values(
        title=_set_or_none(getattr(node, "title", None)),
        description=_set_or_none(getattr(node, "description", None)),
        uri=uri,
        iri=expand_uri(uri, dataset, manifest) if uri is not None else None,
        eli=_set_or_none(getattr(node, "eli", None)),
        status=_set_or_none(getattr(node, "status", None)),
        visibility=_set_or_none(getattr(node, "visibility", None)),
        access=_access_name(getattr(given, "access", None)),
    )


def _apply(node: Any, values: Values, fields: Iterable[str], dataset, manifest) -> None:
    """Set each field of `node` that it does not set, from `values`. `dataset` is where a `uri` is written."""
    for name in fields:
        value = getattr(values, name)
        if value is None:
            continue
        if name == "access":
            if _access_name(node.given.access) is None:
                node.given.access = value
                node.access = get_enum_by_name(Access, value)
        elif name == "uri":
            uri = _inherited_uri(dataset, values, manifest)
            if uri is not None and _set_or_none(node.uri) is None:
                node.uri = uri
        elif _set_or_none(getattr(node, name)) is None:
            setattr(node, name, value)


def expand_uri(value: str | None, dataset: Any, manifest: Any) -> str | None:
    """An IRI with its prefix expanded, first by the dataset's prefixes, then by the manifest's, then the known ones.

    `None` when there is no value. A value without a prefix is returned as it is. So is one with an unknown prefix.
    """
    if not value:
        return None
    prefix, sep, local = value.partition(":")
    if not sep or local.startswith("//"):
        return value
    declared = _declared(prefix, dataset, manifest)
    return value if declared is None else declared + local


def _declared(prefix: str, dataset: Any, manifest: Any) -> str | None:
    for prefixes in (getattr(dataset, "prefixes", None) or {}, getattr(manifest, "prefixes", None) or {}):
        declared = prefixes.get(prefix)
        if declared is not None and getattr(declared, "uri", None):
            return declared.uri
    return KNOWN_PREFIXES.get(prefix)


def _inherited_uri(dataset: Any, values: Values, manifest: Any) -> str | None:
    """The URI of a base, as the element in `dataset` writes it (see the module doc).

    `None` when the base has a prefix that is not declared anywhere: that URI is not valid, so it is not inherited.
    """
    raw = values.uri
    prefix, sep, local = raw.partition(":")
    if not sep or local.startswith("//"):
        return raw
    if values.iri == raw:
        return None
    # `iri` is the namespace of the prefix followed by `local`. The prefix is looked up by its namespace.
    namespace = values.iri[: len(values.iri) - len(local)]
    curie = _curie_in(dataset, prefix, namespace, manifest)
    if curie is None:
        return values.iri
    return f"{curie}:{local}"


def _curie_in(dataset: Any, prefix: str, namespace: str, manifest: Any) -> str | None:
    """A prefix name for `namespace` in `dataset`: the one that has it, else `prefix` if it is free there, else `None`."""
    prefixes = getattr(dataset, "prefixes", None) or {}
    for name, declared in prefixes.items():
        if declared.uri == namespace:
            return name
    if _declared(prefix, dataset, manifest) == namespace:
        return prefix
    if dataset is not None and prefix not in prefixes:
        _add_prefix(dataset, prefix, namespace)
        return prefix
    return None


def _add_prefix(dataset: Any, name: str, uri: str) -> None:
    prefix = UriPrefix()
    prefix.name = name
    prefix.uri = uri
    prefix.parent = dataset
    dataset.prefixes[name] = prefix
