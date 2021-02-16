from spinta.core.ufuncs import Bind
from spinta.core.ufuncs import Expr
from spinta.core.ufuncs import ShortExpr
from spinta.core.ufuncs import ufunc
from spinta.exceptions import PropertyNotFound
from spinta.naming.components import NameFormatter
from spinta.types.datatype import DataType
from spinta.types.datatype import Ref
from spinta.ufuncs.components import ForeignProperty


@ufunc.resolver(NameFormatter, Bind, Bind, name='getattr')
def getattr_(env: NameFormatter, obj: Bind, attr: Bind) -> DataType:
    if obj.name not in env.model.properties:
        raise PropertyNotFound(env.model, property=obj.name)
    prop = env.model.properties[obj.name]
    return env.call('getattr', prop.dtype, attr)


@ufunc.resolver(NameFormatter, Ref, Bind, name='getattr')
def getattr_(env: NameFormatter, dtype: Ref, attr: Bind) -> ForeignProperty:
    prop = dtype.model.properties[attr.name]
    return ForeignProperty(None, dtype, prop.dtype)


@ufunc.resolver(NameFormatter, Expr, names=['testlist', 'list'])
def containers(env: NameFormatter, op: str, expr: Expr) -> Expr:
    args, kwargs = expr.resolve(env)
    return Expr(op, *args)


@ufunc.resolver(NameFormatter, Expr, names=['or', 'and'])
def containers2(env: NameFormatter, op: str, expr: Expr) -> Expr:
    args, kwargs = expr.resolve(env)
    return ShortExpr(op, *args)

