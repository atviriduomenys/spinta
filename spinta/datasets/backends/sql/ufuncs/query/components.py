from __future__ import annotations

from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union
from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy.sql.functions import Function

from spinta.components import Model
from spinta.core.ufuncs import Expr

from spinta.exceptions import UnknownMethod
from spinta.types.datatype import Array
from spinta.ufuncs.querybuilder.components import QueryBuilder, QueryPage, QueryParams
from spinta.ufuncs.querybuilder.helpers import (
    merge_with_page_selected_list,
    merge_with_page_sort,
    merge_with_page_limit,
)
from spinta.ufuncs.components import ForeignProperty
from spinta.utils.itertools import ensure_list

if TYPE_CHECKING:
    from spinta.datasets.backends.sql.components import Sql


class SqlFrom:
    backend: Sql
    joins: Dict[str, sa.Table]
    from_: sa.Table

    def __init__(self, backend: Sql, table: sa.Table):
        self.backend = backend
        self.joins = {}
        self.from_ = table

    def get_table(
        self,
        env: SqlQueryBuilder,
        prop: ForeignProperty,
    ) -> sa.Table:
        fpr: Optional[ForeignProperty] = None
        for fpr in prop.chain:
            if fpr.name in self.joins:
                continue

            # Left table foreign keys
            lmodel = fpr.left.prop.model
            if len(fpr.chain) > 1:
                # Use table alias of previous join.
                ltable = self.joins[fpr.chain[-2].name]
            else:
                # Use the main table, without alias.
                ltable = self.backend.get_table(lmodel)
            lenv = env(model=lmodel, table=ltable)
            lfkeys = lenv.call("join_table_on", fpr.left.prop)
            lfkeys = ensure_list(lfkeys)

            # Right table primary keys
            rpkeys = []
            rmodel = fpr.right.prop.model
            rtable = self.backend.get_table(rmodel).alias()
            renv = env(model=rmodel, table=rtable)
            for rpk in fpr.left.refprops:
                rpkeys += ensure_list(renv.call("join_table_on", rpk))

            # Number of keys on both left and right must be equal.
            assert len(lfkeys) == len(rpkeys), (lfkeys, rpkeys)
            condition = []
            for lfk, rpk in zip(lfkeys, rpkeys):
                condition += [lfk == rpk]

            # Build `JOIN rtable ON (condition)`.
            assert len(condition) > 0
            if len(condition) == 1:
                condition = condition[0]
            else:
                condition = sa.and_(*condition)

            self.joins[fpr.name] = rtable
            self.from_ = self.from_.outerjoin(rtable, condition)

        return self.joins[fpr.name]

    def get_intermediate_table(
        self,
        env: SqlQueryBuilder,
        dtype: Array,
    ) -> sa.Table:
        table_name = dtype.model.name
        if table_name in self.joins:
            return self.joins[table_name]

        lmodel = dtype.left_prop.model
        ltable = self.backend.get_table(lmodel).alias()
        lenv = env(model=lmodel, table=ltable)

        lfkeys = lenv.call("join_table_on", dtype.left_prop)
        lfkeys = ensure_list(lfkeys)

        rpkeys = []
        rmodel = dtype.prop.model
        # This should be main table model, so there is no need for alias
        rtable = self.backend.get_table(rmodel)
        renv = env(model=rmodel, table=rtable)

        for rpk in dtype.left_prop.dtype.refprops:
            rpkeys += ensure_list(renv.call("join_table_on", rpk))

        # Number of keys on both left and right must be equal.
        assert len(lfkeys) == len(rpkeys), (lfkeys, rpkeys)
        condition = []
        for lfk, rpk in zip(lfkeys, rpkeys):
            condition += [lfk == rpk]

        # Build `JOIN rtable ON (condition)`.
        assert len(condition) > 0
        if len(condition) == 1:
            condition = condition[0]
        else:
            condition = sa.and_(*condition)

        self.joins[table_name] = ltable
        self.from_ = self.from_.outerjoin(ltable, condition)

        return self.joins[table_name]


class SqlQueryBuilder(QueryBuilder):
    backend: Sql
    model: Model
    table: sa.Table
    joins: SqlFrom
    columns: List[sa.Column]

    def init(self, backend: Sql, table: sa.Table, params: QueryParams = None):
        result = self(
            backend=backend,
            table=table,
            columns=[],
            resolved={},
            selected=None,
            joins=SqlFrom(backend, table),
            sort=[],
            limit=None,
            offset=None,
            distinct=False,
            group_by=[],
            page=QueryPage(),
        )
        result.init_query_params(params)
        return result

    def build(self, where):
        if self.selected is None:
            # If select list was not explicitly given by client, then select all
            # properties.
            self.call("select", Expr("select"))
        merged_selected = merge_with_page_selected_list(self.columns, self.page)
        merged_sorted = merge_with_page_sort(self.sort, self.page)
        merged_limit = merge_with_page_limit(self.limit, self.page)
        qry = sa.select(merged_selected)
        qry = qry.select_from(self.joins.from_)
        if where is not None:
            qry = qry.where(where)

        if merged_sorted:
            qry = qry.order_by(*merged_sorted)

        if merged_limit is not None:
            qry = qry.limit(merged_limit)

        if self.offset is not None:
            qry = qry.offset(self.offset)

        if self.group_by is not None:
            qry = qry.group_by(*self.group_by)

        if self.distinct:
            qry = qry.distinct()
        return qry

    def execute(self, expr: Any):
        expr = self.call("_resolve_unresolved", expr)
        return super().execute(expr)

    def default_resolver(self, expr, *args, **kwargs):
        raise UnknownMethod(name=expr.name, expr=str(expr(*args, **kwargs)))

    def add_column(self, column: Union[sa.Column, Function]) -> int:
        """Returns position in select column list, which is stored in
        Selected.item.
        """
        assert isinstance(column, (sa.Column, Function)), column
        if column not in self.columns:
            self.columns.append(column)
        return self.columns.index(column)

    def add_to_group_by(self, columns: [List[sa.Column], sa.Column]):
        columns = ensure_list(columns)

        if self.group_by is None:
            self.group_by = []

        for column in columns:
            if column not in self.group_by:
                self.group_by.append(column)
