from typing import Any
from typing import Iterator
from typing import List

from spinta.components import Context, Model
from spinta.core.ufuncs import Expr
from spinta.datasets.components import Param
from spinta.dimensions.param.components import ParamBuilder
from spinta.dimensions.param.components import ResolvedParams
from spinta.manifests.components import Manifest
from spinta.utils.schema import NA


def _recursive_iter_params(
    context: Context,
    model: Model,
    manifest: Manifest,
    params: List[Param],
    values: ResolvedParams,
) -> Iterator[ResolvedParams]:
    if len(params) > 0:
        param = params[0]
        env = ParamBuilder(context)
        env.update(params=values if values else {}, target_param=param.name, manifest=manifest)
        for source, formula in zip(param.sources, param.formulas):
            env.update(this=source)

            resolve_formula = formula or source
            expr = env.resolve(resolve_formula)
            result: Iterator[Any] = env.execute(expr)
            result = _recurse(env, param, model, resolve_formula, result)
            if not isinstance(result, Iterator):
                result = iter([result])

            for value in result:
                new_value = {**values, param.name: value}

                yield from _recursive_iter_params(
                    context,
                    model,
                    manifest,
                    params[1:],
                    new_value,
                )
    else:
        yield values


def iterparams(context: Context, model: Model, manifest: Manifest, params: List[Param] = []) -> Iterator[ResolvedParams]:
    values = {}

    if params:
        yield from _recursive_iter_params(context, model, manifest, params, values)
    else:
        yield {}


def _recurse(env: ParamBuilder, param: Param, source: Model, expr: Expr, values: Iterator) -> Iterator:
    if not isinstance(values, Iterator):
        values = iter([values])

    for value in values:
        if value is not None:
            yield value

            if param.name in param.dependencies:
                if env.target_param not in env.params or env.params[env.target_param] != value:
                    env.params[env.target_param] = value
                    if isinstance(expr, Expr):
                        yield from _recurse(env, param, source, expr, env.resolve(expr))

