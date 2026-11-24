"""`include` of `copy` (3.2): keep the elements that the references of a document need.

Included documents are used only to resolve references. They are `simple`
documents, read before they are linked, so this is plain dict work: it does
not link, does not load Spinta and does not touch a database (3.2).

The targets of an element are read from its `given` cells, because a document
read from a table has no `implied` yet (4.6). The rules are those of the
tabular reader: a relative model name is in the dataset of the element, an
absolute one (`/locn/Country`) is in no dataset, and an enum name is looked up
in the owners of its dataset, nearest first, then in the manifest (see
`spinta.dimensions.enum.helpers.link_prop_enum`).

Each element is followed from the document it is in. The targets that the
main document does not have are looked up in the included documents, and the
targets of the kept elements are followed the same way, until no new element
is kept. The parent chain of a kept element is kept too (4.2). The kept
elements are then merged after the main document (3.1), and they have no
`index`: they are not rows of the source (4.2). Their `given` is kept, so the
document reads back to the same elements. The main rows keep their `index`,
so the line numbers that read errors report do not change. The reader puts the
rows without an index in their place (`spinta.manifests.simple.helpers.row_order`).
"""

from __future__ import annotations

import copy as _copy
from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from spinta.manifests.simple.helpers import _enum_name
from spinta.manifests.simple.merge import merge_documents, same_element
from spinta.manifests.simple.types import MANIFEST_TYPE, SPINTA_KEY, SimpleManifest, SimpleManifestError
from spinta.manifests.tabular.helpers import _parse_dtype_string, _parse_property_ref

Key = tuple[str, str]

# The types whose `ref` cell names a model (4.6, `ref_model`). Any other type with a `ref` cell names an enum.
_MODEL_REF_TYPES = frozenset({"ref", "backref", "array", "generic"})


@dataclass(frozen=True)
class Included:
    """The result of `include_documents`.

    `doc` is the document with the kept elements. `keys` are the `(type, path)`
    of the kept elements that are not in the main document (manifests excluded):
    they have no `index` (4.2).
    """

    doc: SimpleManifest
    keys: frozenset[Key] = frozenset()


def include_documents(
    doc: SimpleManifest,
    includes: Sequence[SimpleManifest],
    *,
    version: str,
) -> Included:
    """`doc` with the elements of `includes` that its references need (3.2).

    `doc` is returned as it is when nothing is needed from `includes`.
    """
    if not includes:
        return Included(doc)
    pool = _Pool([doc, *includes])
    pool.run()
    pruned = [part for d in range(1, len(pool.docs)) if (part := pool.pruned(d)) is not None]
    keys = frozenset(key for key, d in pool.kept.items() if d > 0 and key[0] != MANIFEST_TYPE)
    if not pruned:
        return Included(doc, keys)
    merged = merge_documents([doc, *pruned], version=version)
    for type_, path in keys:
        merged[type_][path]["index"] = None
    return Included(merged, keys)


class _Pool:
    """The documents of a copy, and the elements of each one that are kept.

    Document 0 is the main document, all of its elements are kept from the
    start. Documents 1.. are the included ones. `kept` maps each kept key to
    the document it is taken from.
    """

    def __init__(self, docs: list[SimpleManifest]) -> None:
        self.docs = docs
        self.elements: list[dict[Key, dict[str, Any]]] = [_elements_of(doc) for doc in docs]
        self.enums: list[dict[tuple[str | None, str], list[Key]]] = []
        self.owned: list[dict[Key, list[Key]]] = []
        for elements in self.elements:
            enums, owned = _enum_index(elements)
            self.enums.append(enums)
            self.owned.append(owned)
        self.kept: dict[Key, int] = {}
        self.queue: deque[Key] = deque()
        for key in self.elements[0]:
            self.kept[key] = 0
            self.queue.append(key)

    def run(self) -> None:
        """Keep the targets and the parents of every kept element, until no new element is kept."""
        while self.queue:
            key = self.queue.popleft()
            d = self.kept[key]
            element = self.elements[d][key]
            if element["parent"] is not None:
                parent = element["parent"]
                self._need((parent["type"], parent["path"]), d, parent=True)
            self._follow(key, d, element)

    def pruned(self, d: int) -> SimpleManifest | None:
        """The kept elements of included document `d`, as a `simple` document of their own, or `None`."""
        picked = [(key, element) for key, element in self.elements[d].items() if self.kept.get(key) == d]
        if not picked:
            return None
        doc: SimpleManifest = {SPINTA_KEY: _copy.deepcopy(self.docs[d][SPINTA_KEY])}
        for (type_, path), element in picked:
            doc.setdefault(type_, {})[path] = _copy.deepcopy(element)
        return doc

    # Lookup

    def _order(self, own: int) -> list[int]:
        """The included documents to search for an element: its own document first, then the others."""
        others = [d for d in range(1, len(self.docs)) if d != own]
        return ([own] if own else []) + others

    def _locate(self, key: Key, own: int) -> int | None:
        if key in self.kept:
            return self.kept[key]
        for d in self._order(own):
            if key in self.elements[d]:
                return d
        return None

    def _need(self, key: Key, own: int, *, parent: bool = False) -> None:
        """Keep `key`, found in an included document, unless the document has it already.

        A parent is compared with the element of the document it is needed from, as merged documents are (3.1):
        the same `given` and `implied` are kept once, anything else is a conflict.
        """
        if key in self.kept:
            found = self.kept[key]
            if parent and found != own and key in self.elements[own]:
                _same_or_raise(self.elements[own][key], self.elements[found][key], key)
            return
        d = self._locate(key, own)
        if d is None:
            return
        self.kept[key] = d
        self.queue.append(key)

    def _first(self, keys: list[Key], own: int) -> None:
        """Keep the first of `keys` that exists, the way a link looks a name up in order of precedence."""
        for key in keys:
            if self._locate(key, own) is not None:
                self._need(key, own)
                return

    def _enum(self, owners: list[str | None], name: str, own: int) -> None:
        """Keep the items of the enum `name` of the nearest owner that has one. The main document wins."""
        for owner in owners:
            group = (owner, name)
            if group in self.enums[0]:
                return
            for d in self._order(own):
                items = self.enums[d].get(group)
                if items:
                    for key in items:
                        self._need(key, d)
                    return

    # Targets of an element

    def _follow(self, key: Key, d: int, element: dict[str, Any]) -> None:
        type_ = key[0]
        given = element.get("given") or {}
        if type_ == "model":
            self._model(key, d, element, given)
        elif type_ == "base":
            self._base(key, d, given)
        elif type_ == "property":
            self._property(key, d, element, given)
        if type_ in ("model", "property", "scope"):
            self._uri(key, d, given)

    def _model(self, key: Key, d: int, element: dict[str, Any], given: dict[str, str]) -> None:
        """A model needs its base and its primary key (4.6)."""
        if element.get("base") is not None:
            self._need(("base", element["base"]), d)
        for name in _names(given.get("ref")):
            self._need(("property", f"{key[1]}/{name}"), d)

    def _base(self, key: Key, d: int, given: dict[str, str]) -> None:
        """A base needs its model and the key properties of that model (4.6)."""
        model = self._base_model(key, d)
        if model is None:
            return
        self._need(("model", model), d)
        for name in _names(given.get("ref")):
            self._need(("property", f"{model}/{name}"), d)

    def _property(self, key: Key, d: int, element: dict[str, Any], given: dict[str, str]) -> None:
        """A property needs its referenced model and properties, its inherited or denormalised property, and its enum."""
        model = self._ancestor(key, d, "model")
        if model is None:
            return
        model_key = ("model", model)
        dataset = self._ancestor(key, d, "dataset")
        name = key[1][len(model) + 1 :]
        ref = given.get("ref")
        dtype = _type_name(given)
        if dtype is None:
            model_element = self.elements[d].get(model_key) or {}
            if model_element.get("base") is not None:
                dtype = "inherit"  # a property of a base scope, with no type (4.3)
            elif "." in name:
                dtype = "denorm"

        if dtype in _MODEL_REF_TYPES and ref:
            target, props = _parse_property_ref(ref)
            target_model = _model_path(dataset, target)
            self._need(("model", target_model), d)
            for prop in props:
                self._need(("property", f"{target_model}/{prop}"), d)
        elif dtype == "denorm":
            self._denorm(key, d, model, dataset, name)
        elif dtype == "inherit":
            base = self._base_model_of_model(model_key, d)
            if base is not None:
                self._need(("property", f"{base}/{name}"), d)
        elif ref:
            self._enum(_owners(dataset), ref, d)
        # The items of an enum that the property has itself (4.3).
        for item in self.owned[d].get(key, []):
            self._need(item, d)

    def _denorm(self, key: Key, d: int, model: str, dataset: str | None, name: str) -> None:
        """`country.name` reads `name` of the model that `country` refers to (4.6, `denorm`)."""
        head, _, tail = name.partition(".")
        head_key = ("property", f"{model}/{head}")
        found = self._locate(head_key, d)
        if found is None:
            return
        self._need(head_key, d)
        head_given = self.elements[found][head_key].get("given") or {}
        if _type_name(head_given) in _MODEL_REF_TYPES and head_given.get("ref"):
            target, _ = _parse_property_ref(head_given["ref"])
            self._need(("property", f"{_model_path(dataset, target)}/{tail}"), d)

    def _base_model_of_model(self, model_key: Key, d: int) -> str | None:
        """The model that a model is based on, through its base element (4.6, `base`)."""
        element = self.elements[d].get(model_key) or {}
        if element.get("base") is None:
            return None
        return self._base_model(("base", element["base"]), d)

    def _base_model(self, key: Key, d: int) -> str | None:
        """The model that a base element names. Its name is the path less its dataset and any `#n` (4.3)."""
        found = self._locate(key, d)
        if found is None:
            return None
        dataset = self._ancestor(key, found, "dataset")
        name = _relative(key[1], dataset).split("#", 1)[0]
        return _model_path(dataset, name)

    def _uri(self, key: Key, d: int, given: dict[str, str]) -> None:
        """A prefix named in a CURIE is kept, looked up in the dataset of the element, then in the manifest."""
        uri = given.get("uri")
        if not uri:
            return
        prefix, sep, local = uri.partition(":")
        if not sep or local.startswith("//"):
            return  # an IRI, not a CURIE (`expand_uri`)
        dataset = self._ancestor(key, d, "dataset")
        candidates = [("prefix", f"{dataset}/{prefix}")] if dataset else []
        self._first([*candidates, ("prefix", prefix)], d)

    # Parents

    def _ancestor(self, key: Key, d: int, type_: str) -> str | None:
        """The path of the nearest ancestor of type `type_` of `key` (or `key` itself), in document `d`."""
        if key[0] == type_:
            return key[1]
        element = self.elements[d].get(key)
        while element is not None and element["parent"] is not None:
            parent = element["parent"]
            if parent["type"] == type_:
                return parent["path"]
            element = self.elements[d].get((parent["type"], parent["path"]))
        return None


def _elements_of(doc: SimpleManifest) -> dict[Key, dict[str, Any]]:
    return {
        (type_, path): element
        for type_, elements in doc.items()
        if type_ != SPINTA_KEY
        for path, element in elements.items()
    }


def _enum_index(
    elements: dict[Key, dict[str, Any]],
) -> tuple[dict[tuple[str | None, str], list[Key]], dict[Key, list[Key]]]:
    """The enum items of a document, grouped by their owner and enum name, and by the element that owns them.

    A group is keyed by the path of its owner, `None` for the manifest (4.3). An item without `given` has no
    row, so it is not an enum of the document.
    """
    groups: dict[tuple[str | None, str], list[Key]] = {}
    owned: dict[Key, list[Key]] = {}
    for key, element in elements.items():
        if key[0] != "enum" or element["given"] is None:
            continue
        parent = element["parent"]
        owner = None if parent is None or parent["type"] == MANIFEST_TYPE else parent["path"]
        name = _enum_name(key[1], owner, element["given"])
        groups.setdefault((owner, name), []).append(key)
        if parent is not None:
            owned.setdefault((parent["type"], parent["path"]), []).append(key)
    return groups, owned


def _owners(dataset: str | None) -> list[str | None]:
    """The owners an enum name is looked up in: the dataset, its parent namespaces, then the manifest (`link_prop_enum`)."""
    owners: list[str | None] = []
    if dataset:
        parts = dataset.split("/")
        owners = ["/".join(parts[:i]) for i in range(len(parts), 0, -1)]
    return [*owners, None]


def _type_name(given: dict[str, str]) -> str | None:
    """The bare type of a `given` type cell (`ref required` is `ref`), `None` when there is none."""
    if not given.get("type"):
        return None
    return _parse_dtype_string(given["type"])["type"]


def _model_path(dataset: str | None, name: str) -> str:
    """The path of a model named in a cell, as the tabular reader makes it (`get_relative_model_name`)."""
    if name.startswith("/"):
        return name[1:]
    if "/" in name or dataset is None:
        return name
    return f"{dataset}/{name}"


def _relative(path: str, dataset: str | None) -> str:
    """`path` without its dataset and the `/` after it. A base name keeps its leading `/`."""
    if dataset and path.startswith(dataset + "/"):
        return path[len(dataset) + 1 :]
    return path


def _names(text: str | None) -> list[str]:
    """The names of a comma separated cell, such as the key properties of a model."""
    return [name.strip() for name in text.split(",") if name.strip()] if text else []


def _same_or_raise(mine: dict[str, Any], theirs: dict[str, Any], key: Key) -> None:
    if not same_element(mine, theirs):
        raise SimpleManifestError(f"{key[0]} {key[1]!r} is defined in two documents with different content.")
