from spinta.core.ufuncs import Expr, ufunc
from spinta.ufuncs.pagequerysupport.components import PaginationQuerySupport


@ufunc.resolver(PaginationQuerySupport, Expr, name='and')
def and_(env, expr):
    args, kwargs = expr.resolve(env)
    args = [a for a in args if a is not None]
    return args


@ufunc.resolver(PaginationQuerySupport, Expr, name='or')
def or_(env, expr):
    args, kwargs = expr.resolve(env)
    args = [a for a in args if a is not None]
    return args


@ufunc.resolver(PaginationQuerySupport, Expr)
def select(env, expr):
    expr.resolve(env)


@ufunc.resolver(PaginationQuerySupport)
def count(env):
    env.supported = False
