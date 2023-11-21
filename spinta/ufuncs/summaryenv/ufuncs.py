from spinta.core.ufuncs import ufunc, Expr
from spinta.exceptions import PropertyNotFound, InvalidRequestQuery, SummaryWithMultipleProperties
from spinta.ufuncs.summaryenv.components import SummaryEnv, BBox


@ufunc.resolver(SummaryEnv, Expr, name='bbox')
def bbox(env, expr):
    args, kwargs = expr.resolve(env)
    if len(args) == 4:
        env.bbox = BBox(
            x_min=args[0],
            y_min=args[1],
            x_max=args[2],
            y_max=args[3]
        )
    else:
        raise InvalidRequestQuery(query="bbox", format="bbox(min_lon, min_lat, max_lon, max_lat)")


@ufunc.resolver(SummaryEnv, Expr, name='and')
def and_(env, expr):
    expr.resolve(env)


@ufunc.resolver(SummaryEnv, Expr, name='select')
def select(env, expr):
    args, kwargs = expr.resolve(env)
    prop = None
    for key in args:
        if prop is None and str(key) in env.model.properties:
            prop = env.model.properties[str(key)]
        elif prop is not None:
            raise SummaryWithMultipleProperties(env.model)
        else:
            raise PropertyNotFound(env.model, property=args[0])
    env.prop = prop

