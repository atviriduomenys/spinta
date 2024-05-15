from typing import Any, List

from spinta.components import Page, PageBy, Model, Property, UrlParams
from spinta.core.ufuncs import Env, Expr
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


class QueryParams:
    prioritize_uri: bool = False
    lang_priority: List[str] = None
    expand: Expr = None
    default_expand: bool = False
    lang: List = None
    push: bool = False


class BaseQueryBuilder(Env):
    page: QueryPage
    expand: List[Property] = None
    query_params: QueryParams = None

    def init_query_params(self, params: QueryParams):
        if params is None:
            params = QueryParams()
        self.query_params = params
        self._set_expanded_properties(params)

    # By default, when expand = None we think that nothing is expanded
    # in case we allow default_expand then we set it to empty list
    # if expand is empty list, we assume all are expanded
    # ?expand() will result in [] and ?expand(name) will result in [name]
    def _set_expanded_properties(self, params: QueryParams):
        prop_expr = params.expand
        if prop_expr is None and not params.default_expand:
            self.expand = None
            return

        self.expand = []
        if prop_expr is not None:
            self.expand = self.resolve(prop_expr)


class Star:
    def __str__(self):
        return '*'
