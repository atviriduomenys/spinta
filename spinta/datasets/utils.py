from typing import Any
from typing import Iterator
from typing import List

from spinta.components import Context
from spinta.components import Model
from spinta.datasets.components import Param
from spinta.dimensions.param.components import ParamBuilder
from spinta.dimensions.param.components import ResolvedParams
from spinta.manifests.components import Manifest
from spinta.utils.schema import NA


def _recursive_iter_params(
    context: Context,
    manifest: Manifest,
    params: List[Param],
    values: ResolvedParams,
) -> Iterator[ResolvedParams]:
    if len(params) > 0:
        param = params[0]
        values_memory = [values] if values else []
        for source, formula in zip(param.sources, param.formulas):
            for value_memory in values_memory.copy() or [values]:
                env = ParamBuilder(context)
                env.update(params=value_memory, target_param=param.name, this=source, manifest=manifest)
                expr = env.resolve(formula)
                if expr is not NA:
                    result: Iterator[Any] = env.execute(expr)
                else:
                    result = source
                if not isinstance(result, Iterator):
                    result = iter([result])
                for value in result:
                    new_value = {**values, param.name: value}
                    if new_value not in values_memory:
                        values_memory.append(new_value)

                    yield from _recursive_iter_params(
                        context,
                        manifest,
                        params[1:],
                        new_value,
                    )
    else:
        yield values


def iterparams(context: Context, manifest: Manifest, params: List[Param]) -> Iterator[ResolvedParams]:
    values = {}

    if params:
        yield from _recursive_iter_params(context, manifest, params, values)
    else:
        yield {}
