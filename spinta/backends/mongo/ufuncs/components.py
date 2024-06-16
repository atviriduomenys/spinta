from __future__ import annotations

import dataclasses
from typing import List, Union, Any

import pymongo

from spinta.backends.mongo.components import Mongo
from spinta.components import Property
from spinta.core.ufuncs import Expr
from spinta.exceptions import UnknownMethod
from spinta.types.datatype import DataType
from spinta.ufuncs.basequerybuilder.components import BaseQueryBuilder, QueryPage
from spinta.ufuncs.basequerybuilder.helpers import merge_with_page_selected_list, merge_with_page_sort, \
    merge_with_page_limit


class MongoQueryBuilder(BaseQueryBuilder):

    def init(self, backend: Mongo, table: pymongo.collection.Collection):
        return self(
            backend=backend,
            table=table,
            select=None,
            sort=[],
            limit=None,
            offset=None,
            page=QueryPage()
        )

    def build(self, where: list):
        if self.select is None:
            self.call('select', Expr('select'))

        select = []
        merged_selected = merge_with_page_selected_list(list(self.select.values()), self.page)
        merged_sorted = merge_with_page_sort(self.sort, self.page)
        merged_limit = merge_with_page_limit(self.limit, self.page)
        for sel in merged_selected:
            if isinstance(sel.item, list):
                select += sel.item
            else:
                select.append(sel.item)

        select = {k: 1 for k in select}
        select['_id'] = 0
        select['__id'] = 1
        select['_revision'] = 1

        where = where or {}

        cursor = self.table.find(where, select)

        if merged_sorted:
            cursor = cursor.sort(merged_sorted)

        if merged_limit is not None:
            cursor = cursor.limit(merged_limit)

        if self.offset is not None:
            cursor = cursor.skip(self.offset)

        return cursor

    def default_resolver(self, expr, *args, **kwargs):
        raise UnknownMethod(expr=str(expr(*args, **kwargs)), name=expr.name)


class ForeignProperty:

    def __init__(self, fpr: ForeignProperty, left: Property, right: Property):
        if fpr is None:
            self.name = left.place
            self.chain = [self]
        else:
            self.name += '->' + left.place
            self.chain = fpr.chain + [self]

        self.left = left
        self.right = right

    def __repr__(self):
        return f'<{self.name}->{self.right.name}:{self.right.dtype.name}>'


class Func:
    pass


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
