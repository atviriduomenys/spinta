from __future__ import annotations

import dataclasses
from typing import List, Union, Any, Tuple

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import array_agg
from sqlalchemy.sql.functions import Function

from spinta.backends import get_property_base_model
from spinta.backends.constants import TableType
from spinta.backends.postgresql.components import PostgreSQL
from spinta.components import Model, Property
from spinta.core.ufuncs import Expr
from spinta.datasets.backends.sql.ufuncs.components import Selected
from spinta.exceptions import PropertyNotFound
from spinta.exceptions import UnknownMethod
from spinta.types.datatype import DataType, Denorm
from spinta.ufuncs.basequerybuilder.components import BaseQueryBuilder, QueryPage, QueryParams, Func
from spinta.ufuncs.basequerybuilder.helpers import merge_with_page_selected_list, merge_with_page_sort, \
    merge_with_page_limit
from spinta.ufuncs.components import ForeignProperty


class PgQueryBuilder(BaseQueryBuilder):
    backend: PostgreSQL
    model: Model
    table: sa.Table
    joins: dict
    columns: List[sa.Column]

    def init(self, backend: PostgreSQL, table: sa.Table, params: QueryParams = None):
        result = self(
            backend=backend,
            table=table,
            selected=None,
            resolved={},
            columns=[],
            joins={},
            from_=table,
            sort=[],
            limit=None,
            offset=None,
            aggregate=False,
            page=QueryPage(),
            group_by=[],
        )
        result.init_query_params(params)
        return result

    def build(self, where):
        if self.selected is None:
            self.call('select', Expr('select'))

        if not self.aggregate:
            self.selected['_id'] = Selected(
                item=self.add_column(self.table.c['_id']),
                prop=self.model.properties['_id']
            )
            self.selected['_revision'] = Selected(
                item=self.add_column(self.table.c['_revision']),
                prop=self.model.properties['_revision']
            )
        merged_selected = merge_with_page_selected_list(self.columns, self.page)
        merged_sorted = merge_with_page_sort(self.sort, self.page)
        merged_limit = merge_with_page_limit(self.limit, self.page)

        qry = sa.select(merged_selected)
        qry = qry.select_from(self.from_)

        if where is not None:
            qry = qry.where(where)

        if merged_sorted:
            qry = qry.order_by(*merged_sorted)

        if merged_limit is not None:
            qry = qry.limit(merged_limit)

        if self.offset is not None:
            qry = qry.offset(self.offset)

        if self.group_by:
            qry = qry.group_by(*self.group_by)
        return qry

    def add_column(self, column: Union[sa.Column, Function, sa.sql.expression.ColumnElement]) -> int:
        """Returns position in select column list, which is stored in
        Selected.item.
        """
        assert isinstance(column, (sa.Column, Function, sa.sql.expression.ColumnElement)), column
        if column not in self.columns:
            self.columns.append(column)
        return self.columns.index(column)

    def default_resolver(self, expr, *args, **kwargs):
        raise UnknownMethod(expr=repr(expr(*args, **kwargs)), name=expr.name)

    def get_joined_table(self, prop: ForeignProperty) -> sa.Table:
        for fpr in prop.chain:
            if fpr.name in self.joins:
                continue

            lmodel = fpr.left.prop.model
            if len(fpr.chain) > 1:
                ltable = self.joins[fpr.chain[-2].name]
            else:
                ltable = self.backend.get_table(lmodel)
            lrkey = self.backend.get_column(ltable, fpr.left.prop)

            rmodel = fpr.right.prop.model if not isinstance(fpr.right, Denorm) else fpr.right.rel_prop.model
            if rmodel.base:
                rtable = self.get_joined_base_table(rmodel, prop.right.prop.basename, from_Fk=True)
            else:
                rtable = self.backend.get_table(rmodel).alias()

            rpkey = self.backend.get_column(rtable, rmodel.properties['_id'])

            if isinstance(lrkey, list) and not isinstance(rpkey, list):
                if len(lrkey) == 1:
                    lrkey = lrkey[0]

            condition = lrkey == rpkey
            self.joins[fpr.name] = rtable
            self.from_ = self.from_.outerjoin(rtable, condition)

        return self.joins[fpr.name]

    def get_joined_table_from_ref(self, prop: ForeignProperty) -> sa.Table:
        for fpr in prop.chain:
            if fpr.name in self.joins:
                continue

            if len(fpr.chain) > 1:
                ltable = self.joins[fpr.chain[-2].name]
            else:
                ltable = self.backend.get_table(fpr.left.prop.model)
            rmodel = fpr.right.prop.model
            rtable = self.backend.get_table(rmodel).alias()
            rpkey = self.backend.get_refprop_columns(rtable, fpr.left.prop, rmodel)
            lrkey = self.backend.get_column(ltable, fpr.left.prop)
            if type(lrkey) != type(rpkey):
                raise Exception("COUNT DONT MATCH")

            if not isinstance(lrkey, list):
                lrkey = [lrkey]
            if not isinstance(rpkey, list):
                rpkey = [rpkey]

            condition = None
            for left, right in zip(lrkey, rpkey):
                if condition is None:
                    condition = left == right
                else:
                    condition = sa.and_(condition, left == right)
            self.joins[fpr.name] = rtable
            if condition is not None:
                self.from_ = self.from_.outerjoin(rtable, condition)
        return self.joins[fpr.name]

    def get_backref_joined_table(self, prop: ForeignProperty, selector: sa.sql.expression) -> sa.sql.expression:
        for fpr in prop.chain:
            if fpr.name in self.joins:
                continue

            l_prop = fpr.left.prop
            r_prop = fpr.right.prop
            if len(fpr.chain) > 1:
                ltable = self.joins[fpr.chain[-2].name]
            else:
                ltable = self.backend.get_table(l_prop.model)
            lrkey = self.backend.get_column(ltable, l_prop)
            rpkey = self.backend.get_column(selector, r_prop, override_table=False)
            condition = None
            if isinstance(lrkey, list) and isinstance(rpkey, list):
                if len(lrkey) == len(rpkey):
                    for i, key in enumerate(lrkey):
                        if condition is None:
                            condition = key == rpkey[i]
                        else:
                            condition = sa.and_(condition, key == rpkey[i])
                else:
                    raise Exception("DOESNT MATCH")
            elif type(lrkey) != type(rpkey):
                raise Exception("TYPE DOESNT MATCH")
            else:
                condition = lrkey == rpkey
            self.joins[fpr.name] = selector
            self.from_ = self.from_.outerjoin(selector, condition)
        return self.joins[fpr.name]

    def get_joined_base_table(self, model: Model, prop: str, from_Fk=False):
        inherit_model = model
        base_model = get_property_base_model(inherit_model, prop)

        if not base_model:
            raise PropertyNotFound(model, property=prop)

        ltable = self.backend.get_table(inherit_model)
        lrkey = self.backend.get_column(ltable, inherit_model.properties['_id'])

        rtable = self.backend.get_table(base_model).alias()
        rpkey = self.backend.get_column(rtable, base_model.properties['_id'])

        if base_model.name in self.joins:
            if self.joins[base_model.name] == rtable:
                return self.joins[base_model.name]

        condition = lrkey == rpkey
        self.joins[base_model.name] = rtable
        if not from_Fk:
            self.from_ = self.from_.outerjoin(rtable, condition)

        return self.joins[base_model.name]

    def generate_backref_select(self, left: Property, right: Property, required_columns: List[Tuple[str, str]],
                                aggregate: bool = False):
        select_columns = []
        group_by = []

        left_table = self.backend.get_table(left.model)
        left_keys = self.backend.get_column(left_table, left)

        join_condition = None
        right_table = self.backend.get_table(right)
        right_list_table = None
        list_join_condition = None
        right_keys = self.backend.get_column(right_table, right, override_table=False)
        right_list_keys = None
        if right.list is not None:
            right_list_table = self.backend.get_table(right, TableType.LIST)
            list_join_condition = right_list_table.c['_rid'] == right_table.c['_id']
            right_list_keys = self.backend.get_column(right_list_table, right)
        from_ = right_list_table if right_list_table is not None else right_table
        initial_join_right_keys = right_list_keys if right_list_keys is not None else right_keys

        if aggregate:
            json_columns = []
            for col, label in required_columns:
                json_columns.extend([col, right_table.c[col]])
            column = array_agg(sa.func.json_build_object(*json_columns))
            column = column.label(left.name)
            select_columns.append(column)

            if isinstance(initial_join_right_keys, list):
                for key in initial_join_right_keys:
                    group_by.append(key)
            else:
                group_by.append(initial_join_right_keys)

        else:
            for col, label in required_columns:
                column = right_table.c[col]
                column = column.label(label)
                select_columns.append(column)

        if isinstance(initial_join_right_keys, list):
            select_columns.extend(initial_join_right_keys)
        else:
            select_columns.append(initial_join_right_keys)

        if isinstance(left_keys, list) and isinstance(initial_join_right_keys, list):
            if len(left_keys) == len(initial_join_right_keys):
                for i, key in enumerate(left_keys):
                    if join_condition is None:
                        join_condition = key == initial_join_right_keys[i]
                    else:
                        join_condition = sa.and_(join_condition, key == initial_join_right_keys[i])
            else:
                raise Exception("DOESNT MATCH")
        elif type(left_keys) != type(initial_join_right_keys):
            raise Exception("TYPE DOESNT MATCH")
        else:
            join_condition = left_keys == initial_join_right_keys

        from_ = from_.outerjoin(left_table, join_condition)
        if list_join_condition is not None:
            from_ = from_.outerjoin(right_table, list_join_condition)
        stmt = sa.select(select_columns)
        stmt = stmt.select_from(from_)
        stmt = stmt.group_by(*group_by)
        stmt = stmt.subquery()
        return stmt


@dataclasses.dataclass
class Lower(Func):
    dtype: DataType = None


@dataclasses.dataclass
class Negative(Func):
    arg: Any


@dataclasses.dataclass
class Positive(Func):
    arg: Any


@dataclasses.dataclass
class Recurse(Func):
    args: List[Union[DataType, Func]] = None


# This might be a hack, to access `_base` property
# should probably be remade to work with ForeignProperty
class InheritForeignProperty:

    def __init__(
        self,
        model: Model,
        prop_name: str,
        base_prop: Property
    ):
        self.model = model
        self.base_prop = base_prop
        self.prop_name = prop_name
