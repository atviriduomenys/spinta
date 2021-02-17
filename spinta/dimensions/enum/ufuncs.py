from spinta.core.ufuncs import ufunc
from spinta.dimensions.enum.components import EnumFormula
from spinta.exceptions import FormulaError


@ufunc.resolver(EnumFormula, str)
def bind(env: EnumFormula, name: str) -> None:
    raise FormulaError(env.node, formula=name, error="Binds are not supported.")

