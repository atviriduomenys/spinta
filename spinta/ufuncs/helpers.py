from spinta.core.ufuncs import Expr
from spinta.core.ufuncs import ShortExpr


def merge_formulas(a: Expr, b: Expr) -> Expr:
    if a and b:
        if a.name == 'and' and b.name == 'and':
            args = a.args + b.args
        elif a.name == 'and':
            args = a.args + (b,)
        elif b.name == 'and':
            args = (a,) + b.args
        else:
            args = a, b
        return ShortExpr('and', *args)
    elif a:
        return a
    elif b:
        return b
