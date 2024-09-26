from spinta import commands
from spinta.backends.postgresql.ufuncs.result.components import PgResultBuilder
from spinta.cli.helpers.push.utils import get_data_checksum
from spinta.components import Property
from spinta.core.ufuncs import ufunc, Expr
from spinta.ufuncs.resultbuilder.helpers import get_row_value


@ufunc.resolver(PgResultBuilder, Expr)
def checksum(env: PgResultBuilder, expr: Expr):
    values = {}
    model = None
    if expr.args:
        for arg in expr.args:
            if arg.prop is None or not isinstance(arg.prop, Property):
                continue

            if model is None:
                model = arg.prop.model

            values[arg.prop.place] = commands.cast_backend_to_python(
                env.context,
                arg.prop,
                model.backend,
                get_row_value(env.context, env, env.data, arg)
            )
    return get_data_checksum(values, model)
