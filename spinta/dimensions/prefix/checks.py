"""Check that the prefix of a CURIE in a `uri` or an `eli` is declared.

A `uri` or an `eli` is a CURIE (`locn:Location`) or an IRI (`https://...`). The prefix of a CURIE must be declared in
the dataset of the element, or in the manifest, or be one of `KNOWN_PREFIXES`, the same way as `uri` is expanded (see
`spinta.manifests.inherit`). An IRI and a value without a colon have no prefix, so they are not checked.
"""

from __future__ import annotations

from typing import Any

from spinta.exceptions import UndeclaredEliPrefix, UndeclaredUriPrefix
from spinta.manifests.inherit import _declared


def undeclared_prefix(value: Any, dataset: Any, manifest: Any) -> str | None:
    """The prefix of the CURIE `value` when it is not declared, `None` when it is declared or `value` is no CURIE."""
    if not isinstance(value, str) or not value:
        return None
    prefix, sep, local = value.partition(":")
    if not sep or local.startswith("//"):
        return None
    if _declared(prefix, dataset, manifest) is not None:
        return None
    return prefix


def check_uri_eli_prefixes(
    context: Any,
    this: Any,
    node: Any,
    *,
    owner: str,
    dataset: Any,
    manifest: Any,
) -> None:
    """Report each `uri` and `eli` of `node` whose CURIE prefix is not declared.

    `this` is the element the error is about, which is `node` or the node that owns it (see the `get_error_context`
    commands). `owner` names `node` in the message. Errors are handled, not raised, so the check goes on.
    """
    manager = context.get("error_manager")
    for attr, error in (("uri", UndeclaredUriPrefix), ("eli", UndeclaredEliPrefix)):
        value = getattr(node, attr, None)
        prefix = undeclared_prefix(value, dataset, manifest)
        if prefix is not None:
            manager.handle_error(error(this, prefix=prefix, value=value, owner=owner))
