"""The `implied` keys of the elements of a `simple` document (4.6).

`copy` and `check` load a document into nodes (3.4), and then every element
that has a node gets its `implied`: what Spinta derived for it. Typed values
(levels, counts, types, parsed `prepare` formulas, resolved references) come
from the node, so a downgraded reference has `type` `object` and `null` for
the keys that name its target (4.6). Raw cells (`source`, `ref`) come from
`given`. `title`, `description` and `eli` of a model or a property are what it
sets, given or inherited from a base (see `spinta.manifests.inherit`). An
element that has no node, such as an element that could not be read, keeps
`implied` `null`, so a reader rebuilds it from `given` (4.7 step 2).

Keys are derived by the rules of 4.6. Access is the effective one: the given
cell, else the access that the element inherits from a base (the node has it,
see `spinta.manifests.inherit`), else the access of the owner, else `private`.
`title`, `description` and `eli` of a model or a property are the values it
sets, given or inherited, so an inherited value is in `implied` and not in `given`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from lark import ParseError

from spinta import commands, spyna
from spinta.core.enums import Access
from spinta.manifests.simple.types import MANIFEST_TYPE, SPINTA_KEY, SimpleManifest
from spinta.manifests.simple.uri import UriContext
from spinta.manifests.tabular.elements import escape
from spinta.manifests.tabular.helpers import _parse_dtype_string, combine_source_prepare
from spinta.types.datatype import Array, ArrayBackRef, BackRef, Denorm, Inherit, Object, Ref
from spinta.types.text.components import Text
from spinta.utils.enums import get_enum_by_name
from spinta.utils.schema import NotAvailable

Key = tuple[str, str]

# Types whose `implied` is computed from `given` alone (4.6 L keys only).
_GIVEN_ONLY = frozenset({"append", "comment", "unique", "lang"})


@dataclass
class _Found:
    """A node of the loaded document, and the model a property or its array item belongs to.

    An enum item of a property has the property (`prop`), and the place of the property in the document (`place`).
    """

    node: Any
    model: Any = None
    prop: Any = None
    place: str | None = None


def _name(value: Any) -> str:
    """The name of a node. A reference that the document gives by name only is the string itself (3.4)."""
    return value if isinstance(value, str) else value.name


def _enum_path(owner: str | None, enum: str, item: str) -> str:
    """The path of an enum item (4.3): `{owner}/{enum}/{item}`, with no `enum` segment when unnamed."""
    parts = [owner] if owner else []
    if enum:
        parts.append(enum)
    parts.append(escape(item))
    return "/".join(parts)


def _index(context, manifest) -> dict[Key, _Found]:
    """Every node of the document, by `(type, path)`, with the paths of 4.4."""
    found: dict[Key, _Found] = {}

    def put(type_: str, path: str, node: Any, model: Any = None, prop: Any = None, place: str | None = None) -> None:
        found.setdefault((type_, path), _Found(node, model, prop, place))

    for name, backend in manifest.backends.items():
        put("backend", name, backend)
    for name, ns in commands.get_namespaces(context, manifest).items():
        put("ns", name, ns)
    for name, prefix in manifest.prefixes.items():
        put("prefix", name, prefix)
    for enum_name, items in manifest.enums.items():
        for source, item in items.items():
            put("enum", _enum_path(None, enum_name, source), item)

    for dataset in commands.get_datasets(context, manifest).values():
        put("dataset", dataset.name, dataset)
        for name, prefix in (dataset.prefixes or {}).items():
            put("prefix", f"{dataset.name}/{name}", prefix)
        for resource in (dataset.resources or {}).values():
            path = f"{dataset.name}/{resource.name}"
            put("resource", path, resource)
            for param in resource.params or []:
                put("param", f"{path}/{param.name}", param)

    for model in commands.get_models(context, manifest).values():
        put("model", model.name, model)
        dataset = model.external.dataset if model.external else None
        if model.base is not None:
            prefix = f"{_name(dataset)}/" if dataset is not None else ""
            put("base", f"{prefix}{model.base.name}", model.base)
        for scope in (model.scopes or {}).values():
            put("scope", f"{model.name}/{scope.name}", scope)
        for param in model.params or []:
            put("param", f"{model.name}/{param.name}", param)
        for place, prop in model.flatprops.items():
            _index_property(put, model, place, prop)
    return found


def _doc_place(model, place: str) -> str:
    """The place of a flat property as its path has it (4.4).

    A flat property under an array is named with a dot, `children.born_on`, but its path has `[]` after the
    array: `children[].born_on`.
    """
    parts = place.split(".")
    flat = doc = parts[0]
    for part in parts[1:]:
        prop = model.flatprops.get(flat)
        if prop is not None and isinstance(prop.dtype, Array | ArrayBackRef):
            doc += "[]"
        flat += f".{part}"
        doc += f".{part}"
    return doc


def _index_property(put, model, place: str, prop) -> None:
    place = _doc_place(model, place)
    path = f"{model.name}/{place}"
    put("property", path, prop, model)
    _index_children(put, model, place, prop)
    for enum_name, items in (prop.enums or {}).items():
        for source, item in items.items():
            put("enum", _enum_path(path, enum_name, source), item, model, prop, place)


def _index_children(put, model, place: str, prop) -> None:
    """The properties under a property that the flat properties of the model do not have (4.4).

    The items of an array (`tags[]`), the properties of an object item (`children[].born_on`) and the languages
    of a text (`name@lt`) are children of their owner, with the place of their path.
    """
    dtype = prop.dtype
    if isinstance(dtype, Array | ArrayBackRef) and dtype.items is not None:
        children = [(f"{place}[]", dtype.items)]
    elif isinstance(dtype, Object):
        children = [(f"{place}.{name}", child) for name, child in (dtype.properties or {}).items()]
    elif isinstance(dtype, Text):
        children = [(f"{place}@{code}", lang) for code, lang in (dtype.langs or {}).items()]
    else:
        return
    for child_place, child in children:
        put("property", f"{model.name}/{child_place}", child, model)
        _index_children(put, model, child_place, child)


def _int_value(value: Any) -> int | None:
    """A level or a count as an int (4.6): a `Level`, or a numeric string, or `null` when there is none."""
    if value is None or value == "":
        return None
    if hasattr(value, "value") and isinstance(value.value, int):
        return int(value.value)
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _name_of(value: Any) -> str | None:
    """The name of an enum value such as `Status` or `Visibility`, `null` when none."""
    if value is None or value == "":
        return None
    return getattr(value, "name", value)


def _expr(value: Any) -> Any:
    """A `prepare` as a Spyna AST, or a literal (4.6). `null` when there is none."""
    if value is None or isinstance(value, NotAvailable):
        return None
    if hasattr(value, "todict"):
        return value.todict()
    return value


def _parse(text: str | None) -> Any:
    """The Spyna AST of a formula typed in a cell, or `null`."""
    if not text:
        return None
    try:
        return _expr(spyna.parse(text))
    except ParseError:
        return None


class _Env:
    """What the builders need: the manifest, the document, its nodes and the access of each element."""

    def __init__(self, manifest: Any, doc: SimpleManifest, found: dict[Key, _Found], uris: UriContext) -> None:
        self.manifest = manifest
        self.found = found
        self.uris = uris
        self.elements: dict[Key, dict[str, Any]] = {
            (type_, path): element
            for type_, elements in doc.items()
            if type_ not in (SPINTA_KEY, MANIFEST_TYPE)
            for path, element in elements.items()
        }
        self._access: dict[Key, str] = {}

    def access(self, key: Key) -> str:
        """The effective access (4.6): given, else inherited from a base, else the owner's, else `private`."""
        if key in self._access:
            return self._access[key]
        element = self.elements.get(key)
        given = element.get("given") if element else None
        if given and given.get("access"):
            value = get_enum_by_name(Access, given["access"]).name
        elif (inherited := self._inherited_access(key)) is not None:
            value = inherited
        else:
            parent = element.get("parent") if element else None
            if parent is None or parent["type"] == MANIFEST_TYPE:
                value = "private"
            else:
                value = self.access((parent["type"], parent["path"]))
        self._access[key] = value
        return value

    def _inherited_access(self, key: Key) -> str | None:
        """The access that the node of `key` has from a base, or `None` when no base gives one."""
        found = self.found.get(key)
        given = getattr(found.node, "given", None) if found is not None else None
        value = getattr(given, "access", None)
        if value is None or value == "":
            return None
        return value.name if isinstance(value, Access) else get_enum_by_name(Access, value).name

    def given_access(self, key: Key) -> str | None:
        """The access cell of an element, as written, or `null` (4.6 `access` L of a comment)."""
        given = self.elements[key].get("given") or {}
        return get_enum_by_name(Access, given["access"]).name if given.get("access") else None


# Builders: each returns the `implied` dict of an element (4.6), with every key of its type.


def _ns(key, element, found, env):
    return {"name": found.node.name}


def _backend(key, element, found, env):
    return {"name": key[1], "dsn": (element.get("given") or {}).get("source")}


def _prefix(key, element, found, env):
    return {"name": found.node.name}


def _text(value: Any) -> str | None:
    """A text that the element sets, given or inherited, `null` when it sets none."""
    return value or None


def _dataset(key, element, found, env):
    ds = found.node
    return {
        "name": ds.name,
        "level": _int_value(ds.level),
        "access": env.access(key),
        "count": _int_value(ds.count),
    }


def _resource(key, element, found, env):
    res = found.node
    backend = _name(res.backend) if res.backend is not None else None
    return {
        "name": res.name,
        "backend": backend if backend in env.manifest.backends else None,
        "prepare": _expr(res.prepare),
        "level": _int_value(res.level),
        "access": env.access(key),
        "source_params": sorted(res.source_params or []),
    }


def _unresolved_base(base: Any) -> bool:
    """Whether a base names a model that is not in the document (3.4). Its parent is then the name, not a node.

    A default base (`copy -b`) has its model name as the parent too, but it is matched, and it is marked `default`.
    """
    return isinstance(base.parent, str) and not getattr(base, "default", False)


def _base(key, element, found, env):
    base = found.node
    if _unresolved_base(base):
        # A downgraded base has `null` for the model it names (4.6), and its keys are not known.
        return {"name": base.name, "model": None, "ref": None, "level": _int_value(base.level)}
    parent = _name(base.parent)
    return {
        "name": base.name,
        "model": parent,
        "ref": [f"{parent}/{p.name}" for p in base.pk or []],
        "level": _int_value(base.level),
    }


def _model(key, element, found, env):
    model = found.node
    external = model.external
    dataset = external.dataset if external else None
    resource = external.resource if external else None
    name = model.name
    if dataset is not None and name.startswith(_name(dataset) + "/"):
        name = name[len(_name(dataset)) + 1 :]
    # The primary key is given with `ref`, or inherited from a base. Otherwise the model has none (4.4).
    has_keys = bool(model.given and model.given.pkeys) or model.base is not None
    pkeys = external.pkeys if external and has_keys else []
    return {
        "name": name,
        "resource": f"{_name(dataset)}/{_name(resource)}" if dataset is not None and resource is not None else None,
        "base": _name(model.base.parent) if model.base is not None and not _unresolved_base(model.base) else None,
        "level": _int_value(model.level),
        "access": env.access(key),
        "status": _name_of(model.status),
        "visibility": _name_of(model.visibility),
        "count": _int_value(model.count),
        "uri": env.uris.model_uri(model),
        "title": _text(model.title),
        "description": _text(model.description),
        "eli": _text(model.eli),
        "source": (external.name or None) if external else None,
        "prepare": _expr(external.prepare) if external else None,
        "ref": [f"{model.name}/{p.name}" for p in pkeys],
        "unique": [[f"{model.name}/{p.name}" for p in cols] for cols in model.unique or []],
    }


def _property_type(prop: Any) -> str:
    """The bare type of a property (4.6): an external reference is a `ref`, and every array an `array`."""
    dtype = prop.dtype
    if isinstance(dtype, Array | ArrayBackRef):
        return "array"
    if isinstance(dtype, Ref):
        return "ref"
    return dtype.name


def _known(model: Any) -> bool:
    """Whether a reference has its target model. An unresolved one keeps the path as a string (3.4)."""
    return model is not None and not isinstance(model, str)


def _references(prop: Any) -> tuple[str | None, list[str] | None]:
    """`ref_model` and `ref` of a property (4.6): the target model, and the referenced property paths.

    Both are `null` while the target is not known, as an unresolved reference is not (3.4).
    """
    dtype = prop.dtype
    if isinstance(dtype, Array | ArrayBackRef):
        return None, None
    if isinstance(dtype, Ref):
        if not _known(dtype.model):
            return None, None
        return dtype.model.name, [f"{dtype.model.name}/{p.name}" for p in dtype.refprops or []]
    if isinstance(dtype, BackRef):
        refprop = dtype.refprop
        if not _known(dtype.model) or not _known(refprop):
            return None, None
        return dtype.model.name, [f"{refprop.model.name}/{refprop.name}"]
    if isinstance(dtype, Denorm):
        # `rel_prop` is the property the denorm reads, `Country/name` for `country.name` (set by its link).
        target = getattr(dtype, "rel_prop", None)
        if not _known(target):
            return None, None
        return target.model.name, [f"{target.model.name}/{target.name}"]
    if isinstance(dtype, Inherit):
        base = prop.model.base if prop.model is not None else None
        if base is None:
            return None, None
        parent = _name(base.parent)
        return parent, [f"{parent}/{p.name}" for p in base.pk or []]
    return None, None


def _property(key, element, found, env):
    prop = found.node
    model = found.model if found.model is not None else prop.model
    path = key[1]
    name = path[len(model.name) + 1 :]
    given_type = _parse_dtype_string((element.get("given") or {}).get("type", "")) if element.get("given") else {}
    base = _name(model.base.parent) if model.base is not None else None

    ref_model, ref = _references(prop)
    enum = None
    if prop.enum and ref_model is None and ref is None:
        item = next(iter(prop.enum.values()))
        enum = item.name or path
    external = prop.external
    return {
        "name": name,
        "model": model.name,
        "base": base,
        "type": _property_type(prop),
        "type_args": list(given_type.get("type_args") or []),
        "required": bool(given_type.get("required")),
        "unique": bool(given_type.get("unique")),
        "level": _int_value(prop.level),
        "access": env.access(key),
        "status": _name_of(prop.status),
        "visibility": _name_of(prop.visibility),
        "uri": env.uris.property_uri(prop, model, name),
        "title": _text(prop.title),
        "description": _text(prop.description),
        "eli": _text(prop.eli),
        "source": (external.name or None) if external is not None else None,
        "prepare": _expr(external.prepare) if external is not None else None,
        "ref_model": ref_model,
        "ref": ref,
        "enum": enum,
        "unit": prop.unit or None,
    }


def _enum(key, element, found, env):
    item = found.node
    return {
        "name": item.source,
        "enum": item.name or "",
        "source": item.source,
        "prepare": _expr(item.prepare),
        "level": _int_value(item.level),
        "access": env.access(key),
        "status": _name_of(item.status),
        "visibility": _name_of(item.visibility),
        "uri": env.uris.enum_uri(item, found.prop, found.model, found.place),
        "count": _int_value(item.count),
    }


def _param(key, element, found, env):
    param = found.node
    return {
        "name": param.name,
        "sources": list(param.source or []),
        "formulas": [_expr(prepare) for prepare in param.prepare or []],
    }


def _scope(key, element, found, env):
    scope = found.node
    return {
        "name": scope.name,
        "prepare": _expr(scope.prepare),
        "level": _int_value(scope.level),
        "access": env.access(key),
        "status": _name_of(scope.status),
        "visibility": _name_of(scope.visibility),
        "count": _int_value(scope.count),
        "uri": scope.uri or None,
    }


def _lang(key, element, found, env):
    return {"name": key[1].rsplit("/", 1)[-1]}


def _unique(key, element, found, env):
    model_path = element["parent"]["path"]
    cols = [c.strip() for c in (element.get("given") or {}).get("ref", "").split(",") if c.strip()]
    return {"name": "_".join(cols), "ref": [f"{model_path}/{c}" for c in cols]}


def _append(key, element, found, env):
    given = element.get("given") or {}
    parent = element["parent"]
    if parent is not None and parent["type"] == "property":
        # A property has one formula: the source cell is folded into it (4.6, as `combine_source_prepare`).
        prepare = _parse(combine_source_prepare(given.get("source"), given.get("prepare")))
        return {"name": None, "source": None, "prepare": prepare}
    return {"name": None, "source": given.get("source"), "prepare": _parse(given.get("prepare"))}


def _comment(key, element, found, env):
    given = element.get("given") or {}
    return {
        "name": None,
        "author": given.get("source"),
        "created": given.get("title"),
        "comment": given.get("description"),
        "reply_to": given.get("ref"),
        "access": env.given_access(key),
        "level": _int_value(given.get("level")),
    }


_BUILDERS = {
    "ns": _ns,
    "backend": _backend,
    "prefix": _prefix,
    "dataset": _dataset,
    "resource": _resource,
    "base": _base,
    "model": _model,
    "property": _property,
    "enum": _enum,
    "param": _param,
    "scope": _scope,
    "lang": _lang,
    "unique": _unique,
    "append": _append,
    "comment": _comment,
}


def uri_context(context, manifest) -> UriContext:
    """The URIs of a loaded manifest, with the `uri` option of `context` (`uri.md`).

    `implied_of_document` and the JSON-LD code use this one, so their IRIs are the same.
    """
    config = context.get("config")
    return UriContext(manifest, datasets=commands.get_datasets(context, manifest), uri=config.uri)


def implied_of_document(context, manifest, doc: SimpleManifest) -> dict[Key, dict[str, Any] | None]:
    """The `implied` of every element of `doc`, loaded as `manifest` (4.6).

    An element without a node, and not given-only, gets `None`: it is rebuilt
    from `given` on read (4.7 step 2).
    """
    found = _index(context, manifest)
    env = _Env(manifest, doc, found, uri_context(context, manifest))
    out: dict[Key, dict[str, Any] | None] = {}
    for key, element in env.elements.items():
        type_ = key[0]
        builder = _BUILDERS.get(type_)
        if builder is None:
            out[key] = None
            continue
        node = found.get(key)
        if node is None and type_ not in _GIVEN_ONLY:
            out[key] = None
            continue
        out[key] = builder(key, element, node, env)
    return out
