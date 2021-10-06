from typing import Any
from typing import Iterator
from typing import List

from spinta.components import Context
from spinta.components import Model
from spinta.datasets.components import Param
from spinta.dimensions.param.components import ParamBuilder
from spinta.dimensions.param.components import ResolvedParams


def _recursive_iter_params(
    context: Context,
    model: Model,
    params: List[Param],
    values: ResolvedParams,
) -> Iterator[ResolvedParams]:
    if len(params) == 1:
        yield values
    elif len(params) > 1:
        param = params[0]
        for source, formula in zip(param.sources, param.formulas):
            env = ParamBuilder(context)
            env.update(model=model, params=values, this=source)
            expr = env.resolve(formula)
            result: Iterator[Any] = env.execute(expr)
            if not isinstance(result, Iterator):
                result = iter([result])
            for value in result:
                yield from _recursive_iter_params(
                    context,
                    model,
                    params[1:],
                    {**values, param.name: value},
                )


def iterparams(context: Context, model: Model) -> Iterator[ResolvedParams]:
    values = {}
    params = model.external.params
    if params:
        raise NotImplementedError()
        yield from _recursive_iter_params(context, model, params, values)
    else:
        yield {}
