from spinta.core.ufuncs import Bind
from spinta.core.ufuncs import Expr
from spinta.core.ufuncs import ShortExpr
from spinta.core.ufuncs import ufunc
from spinta.exceptions import PropertyNotFound
from spinta.types.datatype import DataType
from spinta.types.datatype import Ref
from spinta.ufuncs.changebase.components import ChangeModelBase
from spinta.ufuncs.components import ForeignProperty


@ufunc.resolver(ChangeModelBase, Bind, Bind, name='getattr')
def getattr_(env: ChangeModelBase, obj: Bind, attr: Bind) -> DataType:
    if obj.name not in env.model.properties:
        raise PropertyNotFound(env.model, property=obj.name)
    prop = env.model.properties[obj.name]
    return env.call('getattr', prop.dtype, attr)


@ufunc.resolver(ChangeModelBase, Ref, Bind, name='getattr')
def getattr_(env: ChangeModelBase, dtype: Ref, attr: Bind) -> ForeignProperty:
    prop = dtype.model.properties[attr.name]
    return ForeignProperty(None, dtype, prop.dtype)


@ufunc.resolver(ChangeModelBase, Expr, names=['testlist', 'list'])
def containers(env: ChangeModelBase, op: str, expr: Expr) -> Expr:
    args, kwargs = expr.resolve(env)
    return Expr(op, *args)


@ufunc.resolver(ChangeModelBase, Expr, names=['or', 'and'])
def containers2(env: ChangeModelBase, op: str, expr: Expr) -> Expr:
    args, kwargs = expr.resolve(env)
    return ShortExpr(op, *args)

