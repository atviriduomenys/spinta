from __future__ import annotations

from typing import TYPE_CHECKING

from spinta.core.ufuncs import asttoexpr

if TYPE_CHECKING:
    from spinta.components import UrlParams
    from spinta.core.ufuncs import Expr


def urlparams_to_expr(
    params: UrlParams,
    # XXX: `add_count` is a hack, because, not all backends supports it yet.
    add_count: bool = True,
) -> Expr:
    """Convert UrlParams to ufunc Expr.

    This is compatibility function. Currently all query handling is done using
    UrlParams, but in future UrlParams should be replaced by ufunc Expr. This
    way getall and getone commands will receive just one Expr parameter
    containing all the query information.
    """

    ast = []

    if params.query:
        ast += params.query

    if params.select:
        ast.append({'name': 'select', 'args': [
            arg if isinstance(arg, dict) else {'name': 'bind', 'args': [arg]}
            for arg in params.select
        ]})

    if params.sort:
        ast.append({'name': 'sort', 'args': [
            arg if isinstance(arg, dict) else {'name': 'bind', 'args': [arg]}
            for arg in params.sort
        ]})

    if params.limit:
        ast.append({'name': 'limit', 'args': [params.limit]})

    if params.offset:
        ast.append({'name': 'offset', 'args': [params.offset]})

    if params.count and add_count:
        ast.append({'name': 'count', 'args': []})

    if len(ast) == 0:
        ast = {
            'name': 'select',
            'args': [],
        }
    elif len(ast) == 1:
        ast = ast[0]
    else:
        ast = {'name': 'and', 'args': ast}

    return asttoexpr(ast)
