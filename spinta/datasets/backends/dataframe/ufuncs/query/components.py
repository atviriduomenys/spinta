from __future__ import annotations

import dataclasses
from typing import Any

import pandas as pd
from dask.dataframe import DataFrame, from_delayed
from dask import delayed

from spinta.components import Model, Property
from spinta.core.ufuncs import Env, Expr

from spinta.exceptions import UnknownMethod
from spinta.ufuncs.propertyresolver.components import PropertyResolver
from spinta.ufuncs.querybuilder.components import Selected, Func
from spinta.utils.schema import NA
from spinta.datasets.backends.dataframe.components import DaskBackend


RESERVED_COUNT_PROP = "__dask_count"
DASK_PK_KEY = "_id"
DASK_PK_COMBINE_KEY = "_combine"


class DaskDataFrameQueryBuilder(Env):
    backend: DaskBackend
    model: Model
    dataframe: DataFrame
    params: dict
    url_query_params: Expr | None
    property_resolver: PropertyResolver = None

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
            url_query=None,
            count=False,
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

        if self.count:
            # To only return one row, we need to calculate the count first and then transform it back to dataframe
            # Otherwise iterrows will duplicate the result with the number of rows equal to count.
            # Dask allows delayed calculations which ar lazy.

            # Create a delayed scalar function that returns the count of the dataframe as a new one-column dataframe
            count_scalar = delayed(lambda value: pd.DataFrame({RESERVED_COUNT_PROP: [value]}))(
                df.map_partitions(len).sum()
            )
            df = from_delayed([count_scalar], meta=pd.DataFrame({RESERVED_COUNT_PROP: pd.Series(dtype="int64")}))

        return df

    def execute(self, expr: Any):
        expr = self.call("_resolve_unresolved", expr)
        return super().execute(expr)

    def default_resolver(self, expr, *args, **kwargs):
        raise UnknownMethod(name=expr.name, expr=str(expr(*args, **kwargs)))

    def resolve_property(self, *args, **kwargs) -> Property:
        if self.property_resolver is None:
            resolver = PropertyResolver(self.context)
            resolver = resolver.init(model=self.model, ufunc_types=True)
            self.property_resolver = resolver
        result = self.property_resolver.resolve_property(*args, **kwargs)
        return result


class DaskSelected(Selected):
    # Item name in select list.
    item: str = None
    # Model property if a property is selected.
    prop: Property = None
    # A value or an Expr for further processing on selected value.
    prep: Any = NA


@dataclasses.dataclass
class Count(Func):
    pass
