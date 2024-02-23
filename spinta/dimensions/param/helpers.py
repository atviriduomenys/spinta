from typing import List

from spinta import commands
from spinta.components import Context
from spinta.core.ufuncs import asttoexpr
from spinta.datasets.components import Param, Dataset
from spinta.dimensions.param.components import ParamLoader
from spinta.manifests.components import Manifest
from spinta.nodes import load_node


def load_params(context: Context, manifest: Manifest, param_data: dict) -> List[Param]:
    params = []
    if param_data:
        for key, data in param_data.items():
            param = Param()
            load_node(context, param, data)
            param.sources = data['source'].copy()
            param.formulas = [asttoexpr(prep) for prep in data['prepare']]
            param.name = data['name']
            params.append(param)
    return params


def link_params(context: Context, manifest: Manifest, params: List[Param], dataset: Dataset = None):
    param_loader = ParamLoader(context)
    param_loader.update(manifest=manifest, dataset=dataset)
    param_loader.call("resolve_param", params)
