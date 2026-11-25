"""Default bases: a model is matched to a model of the same name in the `-b` manifests (`copy -b`).

A model of the manifest that has no base of its own gets one, when exactly one
model of the default bases has the same name without its dataset (`Country`
for `datasets/gov/ar/Country`). The base is written as a `base` row, `/locn/Country`
with the keys of the matched model as `ref`, so the output has the base that
the match found. A name that has no candidate, or more than one, is not a
match: the model gets a `DefaultBaseNotFound` error, and the output is still
written (3.1).

The base manifests are loaded only to build a `DefaultBases` index: the index
keeps names, keys and the values that each model gives (see
`spinta.manifests.inherit`), never nodes. Once the index is built, the base
contexts are released. The matched base is not linked against a loaded model: its
`parent` is the name of the model, and its `pk` are `StubProperty` objects (see
`spinta.components.Base`). Readers know nothing about the base manifests. A
writer meets such a base only as a name, so it draws no relation that needs the
loaded model (RDF `rdfs:subClassOf`).

The check runs in the link phase of the main document (`load_document`), after
its models are loaded and before they are linked, so it is the same for every
input and output format.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any

from spinta import commands
from spinta.components import Base, Model, StubProperty
from spinta.exceptions import DefaultBaseNotFound
from spinta.manifests.inherit import ModelSource, source_of_model
from spinta.manifests.simple.types import SimpleManifest
from spinta.nodes import get_node, load_node

# The visibilities that `--base-visibility` takes, as Visibility names. `absent` is the visibility of a model
# that gives none. It is in the default scope, so a model without visibility is matched too.
VISIBILITIES = ("public", "package", "protected", "private")
_ABSENT = "absent"


@dataclass(frozen=True)
class DefaultModel:
    """A model of a default base: its path, its name without the dataset, its key names, and its values.

    `source` is what the base gives the model that matches it: the values it sets, given or inherited, and those
    of its properties (see `spinta.manifests.inherit`). Inheritance takes them from there.
    """

    path: str
    name: str
    keys: tuple[str, ...] = ()
    source: ModelSource | None = None


@dataclass
class DefaultBases:
    """The models of the default bases, by name, and the visibilities whose models must match (3.1).

    Holds names, keys and the values of each model, as plain data. The base manifests it was built from are
    not kept.
    """

    scope: frozenset[str]
    by_name: dict[str, list[DefaultModel]] = field(default_factory=dict)

    def add(self, models: Iterable[DefaultModel]) -> None:
        for model in models:
            candidates = self.by_name.setdefault(model.name, [])
            # The same model given by two base manifests is one model.
            if all(c.path != model.path for c in candidates):
                candidates.append(model)

    def candidates(self, name: str) -> list[DefaultModel]:
        return self.by_name.get(name, [])


def visibility_scope(values: Sequence[str] | None) -> frozenset[str]:
    """The visibilities whose models must match a default base. `None` is all of them (3.1).

    Raises `ValueError` for a value that is not one of `VISIBILITIES`, and for an empty list.
    """
    if values is None:
        return frozenset(VISIBILITIES) | {_ABSENT}
    if isinstance(values, str):
        raise ValueError("`base_visibility` takes a list of visibilities, not a string (3.1).")
    names = list(values)
    if not names:
        raise ValueError("`base_visibility` is empty. Name a visibility, or leave it out to match all of them.")
    unknown = [name for name in names if name not in VISIBILITIES]
    if unknown:
        known = ", ".join(VISIBILITIES)
        raise ValueError(f"Unknown visibility {', '.join(map(repr, unknown))} in `base_visibility`; use {known}.")
    return frozenset(names)


def default_models(context, manifest) -> list[DefaultModel]:
    """The models of a loaded base manifest, as the index keeps them. Internal models (`_`) are not bases."""
    found = []
    for model in commands.get_models(context, manifest).values():
        if model.name.startswith("_"):
            continue
        found.append(
            DefaultModel(
                path=model.name,
                name=model.basename,
                keys=_keys_of(model),
                source=source_of_model(context, manifest, model),
            )
        )
    return found


def _keys_of(model: Model) -> tuple[str, ...]:
    """The key names of a model, as its `implied.ref` has them (4.6): given by `ref`, or inherited from a base."""
    has_keys = bool(model.given and model.given.pkeys) or model.base is not None
    if model.external and has_keys:
        return tuple(prop.name for prop in model.external.pkeys)
    return ()


def link_default_bases(context, manifest, bases: DefaultBases) -> None:
    """Match each model without a base of `manifest` to a default base, and record the models that have none (3.1).

    A model that has its own `base` row is not matched. A model with a visibility outside `bases.scope` is
    matched when it can, and gets no error when it cannot.
    """
    manager = context.get("error_manager")
    for model in commands.get_models(context, manifest).values():
        if model.base is not None or model.name.startswith("_"):
            continue
        candidates = bases.candidates(model.basename)
        if len(candidates) == 1:
            model.base = _matched_base(context, manifest, model, candidates[0])
            continue
        if _visibility_name(model) not in bases.scope:
            continue
        manager.handle_error(_not_found(model, candidates))


def _visibility_name(model: Model) -> str:
    visibility = model.visibility
    return visibility.name if visibility is not None else _ABSENT


def _not_found(model: Model, candidates: list[DefaultModel]) -> DefaultBaseNotFound:
    if candidates:
        paths = ", ".join(repr(c.path) for c in candidates)
        reason = f"more than one model named {model.basename!r} ({paths})"
    else:
        reason = f"no model named {model.basename!r}"
    dataset = model.external.dataset if model.external else None
    return DefaultBaseNotFound(
        dataset=None if dataset is None else dataset if isinstance(dataset, str) else dataset.name,
        model=model.name,
        reason=reason,
    )


def _matched_base(context, manifest, model: Model, match: DefaultModel) -> Base:
    """The `Base` node of a matched model. Its `parent` is the name of the default model (see the module doc)."""
    config = context.get("config")
    data: dict[str, Any] = {
        "name": f"/{match.path}",
        "parent": match.path,
        "pk": list(match.keys),
        "level": None,
    }
    base = get_node(context, config, manifest, model.eid, data, group="nodes", ctype="base", parent=model)
    load_node(context, base, data)
    base.model = model
    base.default = True
    base.source = match.source
    commands.load(context, base, data, manifest)
    base.pk = [StubProperty(key) for key in match.keys]
    return base


def with_default_base_elements(doc: SimpleManifest, context, manifest) -> SimpleManifest:
    """`doc` with a `base` element for each model that a default base matched (3.1, 4.2).

    The matched base is not a row of the input, so its element has no `index`. Its `given` holds the key of
    the model as `ref`, as a base row has it. The model points to the element with `base`, as a model row
    with a base does, so the output reads back with the same base. A base element that is already there
    keeps its place: a repeated path gets a `#n` suffix (4.3). Changes `doc` in place and returns it.
    """
    for model in commands.get_models(context, manifest).values():
        base = model.base
        if base is None or not getattr(base, "default", False):
            continue
        element = doc.get("model", {}).get(model.name)
        if element is None:
            continue
        parent = element["parent"]
        prefix = parent["path"] if parent["type"] == "dataset" else None
        path = "/".join(p for p in (prefix, base.name) if p)
        bases = doc.setdefault("base", {})
        candidate, n = path, 1
        while candidate in bases:
            n += 1
            candidate = f"{path}#{n}"
        bases[candidate] = {
            "index": None,
            "parent": dict(parent),
            "given": {"ref": ",".join(p.name for p in base.pk)} if base.pk else {},
            "implied": None,
            "errors": [],
        }
        element["base"] = candidate
    return doc
