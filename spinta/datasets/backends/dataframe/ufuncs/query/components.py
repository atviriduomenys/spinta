from __future__ import annotations

from typing import Any

from dask.dataframe import DataFrame

from spinta.components import Model, Property
from spinta.core.ufuncs import Env, Expr

from spinta.exceptions import UnknownMethod
from spinta.ufuncs.querybuilder.components import Selected
from spinta.utils.schema import NA
from spinta.datasets.backends.dataframe.components import DaskBackend


class DaskDataFrameQueryBuilder(Env):
    backend: DaskBackend
    model: Model
    dataframe: DataFrame
    params: dict

    def init(self, backend: DaskBackend, dataframe: DataFrame, params: dict) -> DaskDataFrameQueryBuilder:
        return self(
            backend=backend,
            dataframe=dataframe,
            resolved={},
            selected=None,
            sort={"desc": [], "asc": []},
            limit=None,
            offset=None,
            params=params,
        )

    def build(self, where):
        if self.selected is None:
            self.call("select", Expr("select"))
        df = self.dataframe

        if self.limit is not None:
            df = df.head(self.limit, npartitions=-1, compute=False)

        if self.offset is not None:
            df = df.loc[self.offset :]

        if where is not None:
            df = df[where]
        return df

    def execute(self, expr: Any):
        expr = self.call("_resolve_unresolved", expr)
        return super().execute(expr)

    def default_resolver(self, expr, *args, **kwargs):
        raise UnknownMethod(name=expr.name, expr=str(expr(*args, **kwargs)))


class DaskSelected(Selected):
    # Item name in select list.
    item: str = None
    # Model property if a property is selected.
    prop: Property = None
    # A value or an Expr for further processing on selected value.
    prep: Any = NA
