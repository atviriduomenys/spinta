from __future__ import annotations
from typing import Any
from typing import Dict

from spinta.core.ufuncs import Env, Expr
from spinta.datasets.components import Dataset
from spinta.manifests.components import Manifest


class ParamBuilder(Env):
    manifest: Manifest
    params: dict
    target_param: str
    url_query_params: Expr | None


class ParamLoader(Env):
    manifest: Manifest
    dataset: Dataset


ResolvedParams = Dict[str, Any]
