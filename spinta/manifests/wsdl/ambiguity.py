from __future__ import annotations

from typing import Any

from spinta import exceptions

from .qname import WsdlQName, wsdl_qname_key


DUPLICATE_EXPANDED_QNAME_CONFLICT = (
    "Duplicate expanded QName conflict detected in a scope that requires uniqueness."
)


def raise_ambiguous_wsdl_reference(
    *,
    path: str,
    qname: str,
    scope: str,
    error: str,
) -> None:
    raise exceptions.AmbiguousWsdlReference(
        path=path,
        qname=qname,
        scope=scope,
        error=error,
    )


def raise_duplicate_wsdl_qname_conflict(
    *,
    path: str,
    qname: str,
    scope: str,
) -> None:
    raise_ambiguous_wsdl_reference(
        path=path,
        qname=qname,
        scope=scope,
        error=DUPLICATE_EXPANDED_QNAME_CONFLICT,
    )


def ensure_unique_wsdl_name(
    items: dict[str, Any],
    qname: WsdlQName | None,
    *,
    key: str,
    scope: str,
    path: str,
) -> None:
    if not key or key not in items:
        return

    raise_duplicate_wsdl_qname_conflict(
        path=path,
        qname=wsdl_qname_key(qname, fallback=key),
        scope=scope,
    )