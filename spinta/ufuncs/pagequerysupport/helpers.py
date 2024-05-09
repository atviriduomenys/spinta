from spinta.components import Context
from spinta.core.ufuncs import Expr
from spinta.exceptions import UnknownMethod
from spinta.ufuncs.pagequerysupport.components import PaginationQuerySupport


# By default, all expr functions should support pagination
# in case we do not find the function, we should assume it is supported
# we need to explicitly set what functions are not supported in PaginationQuerySupport resolvers
def expr_supports_pagination(context: Context, expr: Expr):
    page_query_support = PaginationQuerySupport(context)
    try:
        page_query_support.resolve(expr)
    except (NotImplementedError, UnknownMethod):
        return True
    return page_query_support.supported
