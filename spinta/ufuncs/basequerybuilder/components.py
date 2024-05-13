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


class Star:
    def __str__(self):
        return '*'
