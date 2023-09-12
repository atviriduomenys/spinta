import base64
import json
from typing import Any

from spinta.components import Page, PageBy, Model
from spinta.core.ufuncs import Env, Negative, Bind, Expr
from spinta.datasets.components import ExternalBackend
from spinta.exceptions import FieldNotInResource


class QueryPage:
    page_: Page
    size: int
    select: list
    sort: list

    def __init__(self):
        self.size = 0
        self.select = []
        self.sort = []
        self.page_ = Page()


class BaseQueryBuilder(Env):
    page: QueryPage


class LoadBuilder(Env):
    model: Model

    def resolve(self, expr: Any):
        if not isinstance(expr, Expr):
            # Expression is already resolved, return resolved value.
            return expr

        if expr.name in self._resolvers:
            ufunc = self._resolvers[expr.name]

        else:
            args, kwargs = expr.resolve(self)
            return self.default_resolver(expr, *args, **kwargs)

        if ufunc.autoargs:
            # Resolve arguments automatically.
            args, kwargs = expr.resolve(self)
            try:
                return ufunc(self, *args, **kwargs)
            except NotImplementedError:
                return self.default_resolver(expr, *args, **kwargs)

        else:
            # Resolve arguments manually.
            try:
                return ufunc(self, expr)
            except NotImplementedError:
                pass

    def load_page(self):
        page = Page()
        page_given = False
        if self.model.external and self.model.external.prepare:
            resolved = self.resolve(self.model.external.prepare)
            if not isinstance(resolved, list):
                resolved = [resolved]
            for item in resolved:
                if isinstance(item, Page):
                    page = item
                    page_given = True
                    break
        if not page_given:
            args = ['_id']
            if self.model.given.pkeys:
                if isinstance(self.model.given.pkeys, list):
                    args = self.model.given.pkeys
                else:
                    args = [self.model.given.pkeys]
                if '_id' in args:
                    args.remove('_id')
            for arg in args:
                key = arg
                if arg in self.model.properties:
                    prop = self.model.properties[arg]
                    page.by.update({
                        key: PageBy(prop)
                    })
                else:
                    raise FieldNotInResource(self.model, property=arg)

        # Disable page if given properties are not possible to access
        for page_by in page.by.values():
            if not isinstance(page_by.prop.dtype, get_allowed_page_property_types()):
                page.is_enabled = False
                break
        self.model.page = page


def get_allowed_page_property_types():
    from spinta.types.datatype import Integer, Number, String, Date, Time, DateTime, PrimaryKey
    return Integer, Number, String, Date, DateTime, Time, PrimaryKey


def get_page_values(env: BaseQueryBuilder, row: dict):
    if isinstance(env.model.backend, ExternalBackend):
        return [row[item.prop.external.name] for item in env.page.page_.by.values()]
    else:
        return [row[item.prop.name] for item in env.page.page_.by.values()]


def merge_with_page_selected_list(select_list: list, page: QueryPage):
    merged_selected = select_list or []
    if page.page_.is_enabled and page.select is not None:
        for select in page.select:
            if select not in merged_selected:
                merged_selected.append(select)
    return merged_selected


def merge_with_page_sort(sort: list, page: QueryPage):
    merged_sort = sort or []
    if page.page_.is_enabled and page.sort:
        merged_sort = page.sort
    return merged_sort


def merge_with_page_limit(limit: int, page: QueryPage):
    if page and page.size and page.page_.is_enabled:
        if limit and limit < page.size:
            return limit
        return page.size
    return limit
