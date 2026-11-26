"""The `implied.uri` of a `simple` document (4.6): the URIs that Spinta gives a model, a property and an enum item.

A model has two URIs, `concept` (the RDFS class, or the concept of a concept scheme) and `object` (the URI of its
instances). A property and an enum item have one `uri`, which is their concept:

- a model concept: `{uri}/def/{dataset}/{model}`;
- a model object: `{uri}/id/{dataset}/{model}`;
- a property: `{uri}/def/{dataset}/{model}/{property}`;
- an enum item: `{uri}/def/{dataset}/{scheme}/{concept}`, where the scheme is the enum, or the property of the
  model for an unnamed enum (`{model}/{property}`).

`{uri}` is the `uri` configuration option, empty by default, so that the URIs start with `/def/...`. The dataset
segment is left out for a model with no dataset, and for an enum of the manifest, which belongs to no dataset.

A URI is found by these rules, given the base chain of an element: the element, its base, the base of that base,
and so on until a base that is not loaded (a default base, see `spinta.manifests.simple.bases`):

- R1. The element sets a `uri`: use it, with its prefix expanded in the element's dataset.
- R2. No `uri`: use the `uri` of the first ancestor in the chain that sets one, expanded in that ancestor's dataset.
  A URI that is built by R3 to R5 does not count, so it is never in a chain. Inheritance (see
  `spinta.manifests.inherit`) has already copied such a `uri` onto the element, which is the same value. An ancestor's
  `uri` with a prefix that is not declared anywhere is not valid, so it is skipped, as inheritance skips it.
- R3. No `uri` in the chain, but there is a base: build the URI from the deepest base whose dataset is
  `type=vocabulary` or `type=registry`. A vocabulary owns the concepts it defines (classes, properties and enums), and
  a registry owns them too. The element's own dataset is not used here, only the bases.
- R4. No such base: build it from the deepest base whose dataset is `type=dataset`. A vocabulary or a registry
  that is a base is skipped here. `dataset` is the default `type`.
- R5. No base, or no dataset of the bases to use: build it from the element's own dataset.

The `object` of a model is owned by a registry, never by a vocabulary. It is built from the nearest `type=registry`
dataset in the chain, the element's own first. Without a registry, it is built from the deepest `type=dataset` base
(R4, a vocabulary is skipped). Without that, it is built from the element's own dataset (R5). So when a vocabulary
is deeper than a registry, the vocabulary owns the class and the registry owns the objects.

A property and an enum item are owned by the chain of their property, not by the chain of the model. The chain is the
property, then the same property in each base of its model, while the base has it. R3 to R5 use the datasets of that
chain: a property that a base has is owned by the vocabulary or registry of that base, and a property that no base has
(`flag`, a private property of a model based on a vocabulary) is owned by its own dataset (R5). The class segment is the
model's local name, so a property is `{uri}/def/{dataset}/{model}/{property}`.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote

from spinta.components import Model
from spinta.manifests.inherit import _declared, expand_uri
from spinta.types.datatype import Array, Object

VOCABULARY = "vocabulary"
REGISTRY = "registry"
DATASET = "dataset"


def _set(value: Any) -> str | None:
    """A `uri` that the element sets, `None` when it sets none."""
    return value if isinstance(value, str) and value else None


def _segment(value: str) -> str:
    """A name that is a segment of a URI. `/` and the other reserved characters are escaped, `@` is kept (`name@lt`)."""
    return quote(value, safe="@")


@dataclass
class UriContext:
    """What the URIs of a document need: its manifest, its datasets, and the `uri` configuration option."""

    manifest: Any
    datasets: dict[str, Any] = field(default_factory=dict)
    uri: str = ""

    def __post_init__(self) -> None:
        self.uri = (self.uri or "").rstrip("/")

    # Datasets and chains.

    def dataset(self, model: Model) -> Any | None:
        """The dataset of a model, `None` when it has none."""
        external = model.external
        dataset = external.dataset if external is not None else None
        if isinstance(dataset, str):
            return self.datasets.get(dataset)
        return dataset

    def chain(self, model: Model) -> list[Model]:
        """The model, then its bases, the nearest first. It stops at a base that is not loaded, or that repeats."""
        chain = [model]
        current = model
        while current.base is not None and not current.base.default and isinstance(current.base.parent, Model):
            parent = current.base.parent
            if any(parent is seen for seen in chain):
                break
            chain.append(parent)
            current = parent
        return chain

    def local_name(self, model: Model) -> str:
        """The name of a model without its dataset: `Country` for `datasets/gov/ar/Country` in `datasets/gov/ar`."""
        name = model.name
        dataset = self.dataset(model)
        if dataset is not None and name.startswith(dataset.name + "/"):
            return name[len(dataset.name) + 1 :]
        return name

    # Owners (R3 to R5).

    def class_owner(self, chain: list[Any]) -> Any | None:
        """The dataset that owns the concept of `chain[0]`: R3, R4 and R5 (see the module doc).

        `chain` is a list of nodes that have a dataset, the element first and then its bases. The chain of a model is
        its base chain. The chain of a property is the same property, by its name in the document, in each base of its
        model (`_property_chain`). The items of an array are the property `tags[]`, so they are found in the bases too.
        """
        deepest_first = list(reversed(chain[1:]))
        for kinds in ((VOCABULARY, REGISTRY), (DATASET,)):
            for base in deepest_first:
                dataset = self.dataset(base)
                if dataset is not None and _kind(dataset) in kinds:
                    return dataset
        return self.dataset(chain[0])

    def object_owner(self, chain: list[Model]) -> Any | None:
        """The dataset that owns the objects of `chain[0]`: the nearest registry, else the deepest dataset base, else
        the element's own dataset. A vocabulary is never an object owner, so this does not use `class_owner`."""
        for model in chain:
            dataset = self.dataset(model)
            if dataset is not None and _kind(dataset) == REGISTRY:
                return dataset
        for base in reversed(chain[1:]):
            dataset = self.dataset(base)
            if dataset is not None and _kind(dataset) == DATASET:
                return dataset
        return self.dataset(chain[0])

    # URIs.

    def model_uri(self, model: Model) -> dict[str, str]:
        """`concept` and `object` of a model (4.6)."""
        chain = self.chain(model)
        local = _segments(self.local_name(model))
        concept = self._explicit((node, self.dataset(node)) for node in chain)
        if concept is None:
            concept = self._build("def", self.class_owner(chain), *local)
        return {
            "concept": concept,
            "object": self._build("id", self.object_owner(chain), *local),
        }

    def property_uri(self, prop: Any, model: Model, name: str) -> str:
        """The `uri` of a property of `model`. `name` is its name in the document, without `[]`: `address.street`."""
        pairs = self._property_chain(prop, model, name)
        concept = self._explicit((node, self.dataset(owner)) for node, owner in pairs)
        if concept is None:
            concept = self._build(
                "def",
                self.class_owner([owner for _, owner in pairs]),
                *_segments(self.local_name(model)),
                *_segments(name),
            )
        return concept

    def enum_uri(self, item: Any, prop: Any = None, model: Model | None = None, name: str | None = None) -> str:
        """The `uri` of an enum item. A property's enum is in `prop` (of `model`, named `name` in the document)."""
        enum = item.name or ""
        if model is None:
            # An enum of the manifest belongs to no dataset, so its URI has no dataset.
            explicit = self._explicit([(item, None)])
            if explicit is not None:
                return explicit
            return self._build("def", None, *_segments(enum), _segment(item.source))

        chain = self._property_chain(prop, model, name or _flat_place(model, prop) or prop.name)
        pairs = []
        for node, owner in chain:
            items = (node.enums or {}).get(enum) or {}
            if item.source in items:
                pairs.append((items[item.source], self.dataset(owner)))
        explicit = self._explicit(pairs)
        if explicit is not None:
            return explicit

        scheme = [*_segments(self.local_name(model)), *_segments(name or "")]
        if enum:
            scheme.extend(_segments(enum))
        return self._build("def", self.class_owner([owner for _, owner in chain]), *scheme, _segment(item.source))

    # What the JSON-LD code uses (`spinta/formats/jsonld`, `spinta copy -o *.jsonld`).

    def class_iri(self, model: Model) -> str:
        """The class IRI of a model: its `uri` (prefix expanded) when it is given, else the concept (R2 to R5)."""
        return self.model_uri(model)["concept"]

    def property_iri(self, model: Model, prop: Any, name: str | None = None) -> str:
        """The IRI of a property of `model`: its `uri` when it is given, else resolved from the base chain.

        `name` is the property's name in the document (`address.street`). It defaults to the flat place of the
        property, or to its own name.
        """
        if name is None:
            name = _flat_place(model, prop) or prop.name
        return self.property_uri(prop, model, name)

    def instance_base(self, model: Model) -> str:
        """The base of the instance IRIs of a model: `{uri}/id/{owner}/{Model}`, without a trailing `/`."""
        chain = self.chain(model)
        return self._build("id", self.object_owner(chain), *_segments(self.local_name(model)))

    def instance_id(self, model: Model, _id: str) -> str:
        """The IRI of one instance of a model: `instance_base(model)/{_id}`."""
        return f"{self.instance_base(model)}/{_id}"

    # Helpers.

    def _explicit(self, pairs: Iterable[tuple[Any, Any]]) -> str | None:
        """The `uri` of the first node that sets one, expanded in its dataset (R1, R2).

        The first pair is the element itself, and its `uri` is used as it is written (R1). A `uri` of a base is
        skipped when its prefix is not declared anywhere, as inheritance does: such a URI is not valid (R2).
        """
        for index, (node, dataset) in enumerate(pairs):
            uri = _set(getattr(node, "uri", None))
            if uri is None:
                continue
            if index > 0 and not self._declared(uri, dataset):
                continue
            return expand_uri(uri, dataset, self.manifest)
        return None

    def _declared(self, uri: str, dataset: Any) -> bool:
        """`False` for a CURIE whose prefix is not declared in the dataset, the manifest, or the known prefixes."""
        prefix, sep, local = uri.partition(":")
        if not sep or local.startswith("//"):
            return True
        return _declared(prefix, dataset, self.manifest) is not None

    def _property_chain(self, prop: Any, model: Model, name: str) -> list[tuple[Any, Model]]:
        """The property, then the property with the same `name` in each base of its model, while the base has it.

        `name` is the name of the property in the document, so the items of an array are found in the bases too.
        """
        pairs = [(prop, model)]
        for base in self.chain(model)[1:]:
            base_prop = _property_at(base, name)
            if base_prop is None:
                break
            pairs.append((base_prop, base))
        return pairs

    def _build(self, kind: str, owner: Any | None, *segments: str) -> str:
        """`{uri}/{kind}/{dataset}/{segments}`. The dataset is left out when there is no owner."""
        parts = [self.uri, kind]
        if owner is not None:
            parts.append(owner.name)
        parts.extend(segments)
        return "/".join(parts)


def _kind(dataset: Any) -> str:
    """The `type` of a dataset, `dataset` when it is not given."""
    return getattr(dataset, "dataset_type", None) or DATASET


def _segments(name: str) -> list[str]:
    """The escaped segments of a name, which is a path of names: `tags[]` is `tags`, `a/b` is `a` and `b`."""
    return [_segment(part) for part in name.replace("[]", "").split("/") if part]


def _property_at(model: Model, name: str) -> Any | None:
    """The property of `model` at the document path `name` (`address.city`, `tags[]`, `children[].born_on`), or `None`.

    The items of an array are its `[]` child: the items of `tags` are `tags[]`. The path is walked through the
    properties, so items and the children of items are found, which the flat places of a model do not have.
    """
    prop = None
    for segment in name.split("."):
        many = segment.endswith("[]")
        key = segment[:-2] if many else segment
        children = model.properties if prop is None else _children(prop)
        prop = (children or {}).get(key)
        if prop is None:
            return None
        if many:
            if not isinstance(prop.dtype, Array) or prop.dtype.items is None:
                return None
            prop = prop.dtype.items
    return prop


def _children(prop: Any) -> dict[str, Any] | None:
    """The properties of an object, `None` for any other property."""
    return prop.dtype.properties if isinstance(prop.dtype, Object) else None


def _flat_place(model: Model, prop: Any) -> str | None:
    """The place of a flat property of `model` (`address.street`), `None` for a property that is not flat."""
    for place, flat in model.flatprops.items():
        if flat is prop:
            return place
    return None
