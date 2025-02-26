from spinta.core.ufuncs import ufunc
from spinta.dimensions.enum.components import EnumFormula


@ufunc.resolver(EnumFormula, str)
def bind(env: EnumFormula, name: str) -> None:
    return env.resolve(name)
