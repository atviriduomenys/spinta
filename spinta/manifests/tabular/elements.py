"""The element that each row of a table makes (sections 4.2 to 4.4).

The tabular reader keeps its own schema dicts. They have no row numbers and no
paths, so this module records, for every row, the element the row makes: its
type, its path (4.4), its parent (4.2), its `index` (4.2) and its `given`
cells (4.5). This is the side table of section 7, change 2.

The reader loop calls `describe_row` before it reads a row, with the reader
state as the row sees it. The owners it uses are the owners the reader uses
(`state.prop`, `state.model`, ...), and the state is not changed here.

This module imports nothing from the tabular reader, so the reader can import
it. Problems are stored as `(code, message)` pairs; the reader loop adds them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from spinta.manifests.simple.types import KEYWORD_TYPES, NAME_COLUMN

# Element types whose continuation rows (empty `type`) are elements of the same type (4.3).
SAME_TYPE_CONTINUATION = frozenset({"prefix", "enum", "lang", "comment", "scope", "unique"})

# Rows of these dimensions may hold `/`, which closes a scope and is not an element (4.3).
MAIN_DIMENSIONS = ("dataset", "resource", "base", "model", "property")


@dataclass(eq=False)
class Element:
    """An element of a table, as the reader found it. `parent` is another Element."""

    type: str
    path: str
    parent: Element | None
    line: str | None
    index: int | None
    given: dict[str, str] | None
    enum: str | None = None  # the name of the enum an enum item belongs to ("" if unnamed)
    appends: int = 0  # number of `append` rows that continue this element
    comments: int = 0  # number of comments under this element
    implied: bool = False  # an implied element has no row (4.2)
    errors: list[tuple[str, str, dict[str, str]]] = field(
        default_factory=list
    )  # read errors of its row: (code, message, context)
    # A model only: the paths of the resource and of the base that the reader had open for its row,
    # `None` if none (4.3, 4.8). The writer places the `/` closers of 4.8 from them.
    resource: str | None = None
    base: str | None = None


@dataclass
class Problem:
    """A row, or part of it, that makes no element. Stored on the `manifest` of its input (4.10)."""

    line: str
    message: str
    code: str = "TabularManifestError"


@dataclass
class ElementTable:
    """The elements of one table, in row order, and the problems of rows that make none."""

    columns: list[str] = field(default_factory=list)
    elements: list[Element] = field(default_factory=list)
    by_path: dict[tuple[str, str], Element] = field(default_factory=dict)
    problems: list[Problem] = field(default_factory=list)
    failed_lines: set[str] = field(default_factory=set)
    counter: int = 0
    # Whether a duplicate model is renamed with a number suffix, as the reader does (`copy --rename-duplicates`).
    rename_duplicates: bool = False

    def problem(self, line: str, message: str, code: str = "TabularManifestError") -> None:
        self.problems.append(Problem(line, message, code))
        self.failed_lines.add(line)


def _join(*parts: str | None) -> str:
    return "/".join(p for p in parts if p)


def escape(segment: str) -> str:
    """A value that is a path segment, escaped as 4.4 says."""
    return segment.replace("%", "%25").replace("/", "%2F").replace("#", "%23")


def _index(table: ElementTable, line: str) -> int:
    """The data row number (4.2): the record number minus the header."""
    if line.isdigit():
        return int(line) - 1
    table.counter += 1
    return table.counter


def _given(row: dict[str, str], columns: list[str], skip: set[str]) -> dict[str, str]:
    given = {}
    for column in columns:
        if column in skip:
            continue
        value = str(row.get(column) or "").strip()
        if value:
            given[column] = value
    return given


def _skip_columns(type_: str) -> set[str]:
    skip = set()
    name = NAME_COLUMN.get(type_)
    if name:
        skip.add(name)
    # A `param` keeps its `type` cell: a parameter row may repeat the keyword or leave it empty (4.5).
    if type_ in KEYWORD_TYPES and type_ != "param":
        skip.add("type")
    return skip


def _make(
    table: ElementTable,
    type_: str,
    path: str | None,
    parent: Element | None,
    line: str,
    row: dict[str, str],
) -> Element | None:
    """Add the element of a row. A duplicate keeps the first element and reports the row."""
    if path is None:
        return None
    key = (type_, path)
    existing = table.by_path.get(key)
    if existing is not None and not existing.implied:
        table.problem(line, f"{type_.capitalize()} {path!r} is already defined.")
        return None
    given = _given(row, table.columns, _skip_columns(type_))
    if existing is not None:
        # An implied parent (4.3) that the file defines later is filled in place,
        # so that its children keep the same parent.
        existing.implied = False
        existing.line = line
        existing.index = _index(table, line)
        existing.given = given
        return existing
    element = Element(
        type=type_,
        path=path,
        parent=parent,
        line=line,
        index=_index(table, line),
        given=given,
    )
    table.elements.append(element)
    table.by_path[key] = element
    return element


def _owner(state: Any) -> Element | None:
    """The element of the last main row, as the reader finds it for prefix, enum and param."""
    node = state.prop or state.model or state.base or state.resource or state.dataset or state.manifest
    return getattr(node, "element", None) if node is not None else None


def _top(state: Any) -> Element | None:
    """The element of the row before this one: the nearest open reader that has one."""
    for reader in reversed(state.stack):
        element = getattr(reader, "element", None)
        if element is not None:
            return element
    return None


def _comment_owner(state: Any) -> Element | None:
    """The element a comment belongs to (4.3): the element above it, except that a comment under an `append`
    row belongs to the main row above that (the property, or the resource), not to the append."""
    top = _top(state)
    if top is not None and top.type == "append":
        return _owner(state)
    if top is not None and top.type == "comment":
        # A comment has no comments of its own, the next comment belongs to the same element.
        return top.parent
    return top


def _split_place(place: str) -> tuple[str, str | None]:
    """The parent part of a property name (4.3): `name@lt`, `tags[]`, `address.street`."""
    if "@" in place:
        return place.split("@", 1)[0], "@"
    if place.endswith("[]"):
        return place[:-2], "[]"
    if "." in place:
        return place.rsplit(".", 1)[0], "."
    return place, None


def _property_parent(table: ElementTable, model: Element, place: str, line: str) -> Element:
    """The parent of a property (4.2). An implied parent is made when a row needs one."""
    parent_place, kind = _split_place(place)
    if kind is None:
        return model
    parent_path = _join(model.path, parent_place)
    found = table.by_path.get(("property", parent_path))
    if found is not None:
        return found
    if kind == ".":
        # An object parent is not implied (F8): the reader reports it, and the
        # element keeps the model as its parent.
        return model
    grandparent = _property_parent(table, model, parent_place, line)
    implied = Element(
        type="property",
        path=parent_path,
        parent=grandparent,
        line=None,
        index=None,
        given=None,
        implied=True,
    )
    table.elements.append(implied)
    table.by_path[("property", parent_path)] = implied
    return implied


def _describe_dataset(table: ElementTable, row: dict[str, str], line: str) -> Element | None:
    name = str(row["dataset"]).strip()
    type_ = "ns" if str(row.get("type") or "").strip() == "ns" else "dataset"
    return _make(table, type_, name, None, line, row)


def _describe_resource(table: ElementTable, state: Any, row: dict[str, str], line: str) -> Element | None:
    name = str(row["resource"]).strip()
    dataset = getattr(state.dataset, "element", None) if state.dataset is not None else None
    if dataset is not None:
        return _make(table, "resource", _join(dataset.path, name), dataset, line, row)
    # A resource outside a dataset is a backend (4.3).
    return _make(table, "backend", name, None, line, row)


def _describe_base(table: ElementTable, state: Any, row: dict[str, str], line: str) -> Element | None:
    name = str(row["base"]).strip()
    dataset = getattr(state.dataset, "element", None) if state.dataset is not None else None
    path = _join(dataset.path if dataset else None, name)
    # A repeated base name gets a `#n` suffix (4.3).
    candidate, n = path, 1
    while ("base", candidate) in table.by_path:
        n += 1
        candidate = f"{path}#{n}"
    return _make(table, "base", candidate, dataset, line, row)


def _describe_model(table: ElementTable, state: Any, row: dict[str, str], line: str) -> Element | None:
    written = str(row["model"]).strip()
    dataset = getattr(state.dataset, "element", None) if state.dataset is not None else None
    name = written
    # The same rule as the reader: an absolute or a qualified name is used as it is.
    if name.startswith("/"):
        name = name[1:]
    elif "/" not in name and dataset is not None:
        name = _join(dataset.path, name)
    # A model is in a dataset only when its path starts with the dataset's path (4.4).
    parent = dataset if dataset is not None and name.startswith(dataset.path + "/") else None
    features = "/:" in name
    if features:
        name = name.rsplit("/:", 1)[0]
    if table.rename_duplicates and _taken(table, name):
        # The reader's rule: the first free `name_N`. The renamed name is kept in `given`, so the
        # document reads the same without the option.
        dup = 1
        while _taken(table, f"{name}_{dup}"):
            dup += 1
        name = f"{name}_{dup}"
        written = f"{written}_{dup}"
        row = {**row, "model": written}
    element = _make(table, "model", name, parent, line, row)
    if element is not None:
        resource = getattr(state.resource, "element", None) if state.resource is not None else None
        base = getattr(state.base, "element", None) if state.base is not None else None
        element.resource = resource.path if resource is not None else None
        element.base = base.path if base is not None else None
    if element is not None and features:
        # The features (`/:part`) are not in the path, so the name cell is kept (4.4).
        element.given = {"model": written, **(element.given or {})}
    return element


def _taken(table: ElementTable, path: str) -> bool:
    existing = table.by_path.get(("model", path))
    return existing is not None and not existing.implied


def _describe_property(table: ElementTable, state: Any, row: dict[str, str], line: str) -> Element | None:
    place = str(row["property"]).strip()
    model = getattr(state.model, "element", None) if state.model is not None else None
    if model is None:
        table.problem(line, "Property is not inside a model.")
        return None
    parent = _property_parent(table, model, place, line)
    element = _make(table, "property", _join(model.path, place), parent, line, row)
    if element is not None and not element.implied and _moves_source(row):
        _add_text_source(table, element)
    return element


def _moves_source(row: dict[str, str]) -> bool:
    """Whether a property row is a text property with a source, whose source moves to `name@C` (4.3)."""
    return str(row.get("type") or "").strip() == "text" and bool(str(row.get("source") or "").strip())


def _add_text_source(table: ElementTable, text: Element) -> None:
    """The implied `name@C` element of a text property (4.3): it has the source the text property was given.

    It has no row, so its `index` is `null`. It goes right after its text property, as 4.7 step 4 expects.
    """
    element = Element(
        type="property",
        path=f"{text.path}@C",
        parent=text,
        line=None,
        index=None,
        given=None,
        implied=True,
    )
    table.elements.insert(table.elements.index(text) + 1, element)
    table.by_path[("property", element.path)] = element


def _describe_prefix(table: ElementTable, owner: Element | None, row: dict[str, str], line: str) -> Element | None:
    name = str(row["ref"]).strip()
    if not name:
        table.problem(line, "Prefix does not have a name (the `ref` column).")
        return None
    return _make(table, "prefix", _join(owner.path if owner else None, name), owner, line, row)


def _describe_enum(
    table: ElementTable,
    owner: Element | None,
    inherited: str | None,
    row: dict[str, str],
    line: str,
) -> Element | None:
    named = str(row["ref"]).strip()
    enum = named or (inherited if inherited is not None else "")
    item = str(row["source"] or "").strip() or str(row["prepare"] or "").strip()
    if not item:
        table.problem(line, "At least source or prepare must be specified for an enum.")
        return None
    segments = [owner.path if owner else None, enum or None, escape(item)]
    element = _make(table, "enum", _join(*segments), owner, line, row)
    if element is not None:
        element.enum = enum
    return element


def _describe_param(table: ElementTable, owner: Element | None, row: dict[str, str], line: str) -> Element | None:
    name = str(row["ref"]).strip()
    if not name:
        table.problem(line, "Parameter must have a name.")
        return None
    return _make(table, "param", _join(owner.path if owner else None, name), owner, line, row)


def _describe_lang(table: ElementTable, owner: Element | None, row: dict[str, str], line: str) -> Element | None:
    code = str(row["ref"]).strip()
    if owner is None:
        table.problem(line, "Language metadata needs an element above it.")
        return None
    return _make(table, "lang", _join(owner.path, code), owner, line, row)


def _describe_scope(table: ElementTable, model: Element | None, row: dict[str, str], line: str) -> Element | None:
    if model is None:
        table.problem(line, "Scope is not inside a model.")
        return None
    name = str(row["ref"]).strip()
    return _make(table, "scope", _join(model.path, name), model, line, row)


def _describe_unique(table: ElementTable, model: Element | None, row: dict[str, str], line: str) -> Element | None:
    if model is None:
        table.problem(line, "Unique is not inside a model.")
        return None
    columns = [c.strip() for c in str(row["ref"]).split(",") if c.strip()]
    return _make(table, "unique", _join(model.path, "_".join(columns)), model, line, row)


def _describe_comment(table: ElementTable, owner: Element | None, row: dict[str, str], line: str) -> Element | None:
    if owner is None:
        table.problem(line, "Comment needs an element above it.")
        return None
    owner.comments += 1
    return _make(table, "comment", _join(owner.path, f"#{owner.comments}"), owner, line, row)


def _describe_continuation(table: ElementTable, state: Any, row: dict[str, str], line: str) -> Element | None:
    """A row with an empty `type` (4.3): it continues the element above it."""
    if not any(str(v or "").strip() for v in row.values()):
        return None  # a blank row is not an element (4.3)
    target = _top(state)
    while target is not None and target.type == "append":
        target = target.parent
    if target is None:
        table.problem(line, "Row does not continue an element.")
        return None

    if target.type == "property":
        target.appends += 1
        return _make_append(table, target, row, line)

    if target.type == "param":
        if str(row["ref"]).strip():
            return _describe_param(table, target.parent, row, line)
        target.appends += 1
        return _make_append(table, target, row, line)

    owner = target.parent
    if target.type == "prefix":
        return _describe_prefix(table, owner, row, line)
    if target.type == "enum":
        return _describe_enum(table, owner, target.enum, row, line)
    if target.type == "lang":
        return _describe_lang(table, owner, row, line)
    if target.type == "comment":
        return _describe_comment(table, owner, row, line)
    if target.type == "scope":
        return _describe_scope(table, owner, row, line)
    if target.type == "unique":
        return _describe_unique(table, owner, row, line)

    table.problem(line, f"Updates are not supported in context of {target.type!r}.")
    return None


def _make_append(table: ElementTable, target: Element, row: dict[str, str], line: str) -> Element | None:
    # The `append` element keeps its source and prepare cells (4.5).
    path = f"{target.path}#{target.appends + 1}"
    return _make(table, "append", path, target, line, row)


def _is_scope_closer(row: dict[str, str]) -> bool:
    return any(str(row.get(d) or "").strip() == "/" for d in MAIN_DIMENSIONS)


def is_skipped_row(row: dict[str, str]) -> bool:
    """A blank row or a `/` row. Neither is an element, and neither is an error (4.3)."""
    if _is_scope_closer(row):
        return True
    return not any(str(v or "").strip() for v in row.values())


def describe_row(
    table: ElementTable,
    state: Any,
    dimension: str,
    row: dict[str, str],
    line: str,
) -> Element | None:
    """The element of a row, or None when the row makes none (4.3).

    Problems that make the row an error are stored in `table.problems`. A row
    that makes no element and has no problem is a blank row or a `/` row.
    """
    if is_skipped_row(row):
        return None

    if dimension == "dataset":
        return _describe_dataset(table, row, line)
    if dimension == "resource":
        return _describe_resource(table, state, row, line)
    if dimension == "base":
        return _describe_base(table, state, row, line)
    if dimension == "model":
        return _describe_model(table, state, row, line)
    if dimension == "property":
        return _describe_property(table, state, row, line)

    if dimension == "prefix":
        return _describe_prefix(table, _owner(state), row, line)
    if dimension == "enum":
        return _describe_enum(table, _owner(state), None, row, line)
    if dimension == "param":
        return _describe_param(table, _owner(state), row, line)
    if dimension == "lang":
        return _describe_lang(table, _top(state), row, line)
    if dimension == "comment":
        return _describe_comment(table, _comment_owner(state), row, line)
    if dimension == "scope":
        return _describe_scope(
            table, getattr(state.model, "element", None) if state.model is not None else None, row, line
        )
    if dimension == "unique":
        model = _top(state)
        if model is not None and model.type == "unique":
            model = model.parent
        return _describe_unique(table, model, row, line)
    if dimension == "":
        return _describe_continuation(table, state, row, line)

    # `ns` without a dataset name: the reader reports it.
    return None
