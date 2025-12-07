from typing import List, Any

from spinta import commands, spyna
from spinta.components import Context, Model
from spinta.core.ufuncs import asttoexpr, Expr
from spinta.datasets.components import Param, Dataset
from spinta.dimensions.param.components import ParamLoader
from spinta.manifests.components import Manifest
from spinta.nodes import load_node
from spinta.ufuncs.loadbuilder.components import LoadBuilder
from spinta.utils.schema import NA


def load_param_formulas_and_sources(
    context: Context, param: Param, prepare_asts: list[dict], sources: list[str]
) -> None:
    param_formulas = []
    param_sources = []
    builder = LoadBuilder(context)
    for source, ast in zip(sources, prepare_asts):
        builder.update(this=source, param=param)

        # If possible - resolve formulas with LoadBuilder
        expr = asttoexpr(ast)
        if isinstance(expr, Expr):
            if formula := builder.resolve(expr):
                param_formulas.append(formula)
                param_sources.append(source)
        else:
            param_formulas.append(expr)
            param_sources.append(source)

    param.formulas = param_formulas
    param.sources = param_sources


def load_params(context: Context, manifest: Manifest, param_data: Any) -> List[Param]:
    params = []
    if isinstance(param_data, dict):
        for key, data in param_data.items():
            param = Param()
            load_node(context, param, data)
            load_param_formulas_and_sources(context, param, data["prepare"], data["source"].copy())
            param.name = data["name"]
            params.append(param)
    elif isinstance(param_data, list):
        for item in param_data:
            if isinstance(item, dict) and len(item.keys()) == 1:
                param = Param()
                name = list(item.keys())[0]
                prepare = item[name]
                if isinstance(prepare, str):
                    prepare = spyna.unparse(prepare)
                data = {"name": name, "source": [NA], "prepare": [prepare], "title": "", "description": ""}
                load_node(context, param, data)
                load_param_formulas_and_sources(context, param, data["prepare"], data["source"].copy())
                param.name = data["name"]
                params.append(param)

    return params


def link_params(context: Context, manifest: Manifest, params: List[Param], dataset: Dataset = None):
    param_loader = ParamLoader(context)
    param_loader.update(manifest=manifest, dataset=dataset)
    param_loader.resolve(Expr("resolve_param", params))


def finalize_param_link(context, manifest: Manifest):
    datasets = commands.get_datasets(context, manifest)
    for dataset in datasets.values():
        for resource in dataset.resources.values():
            if resource:
                for param in resource.params:
                    for source in param.sources:
                        if isinstance(source, Model):
                            if source.external and source.external.resource:
                                param.dependencies.update(source.external.resource.source_params)
