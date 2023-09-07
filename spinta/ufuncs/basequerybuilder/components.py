import base64
import json

from spinta.components import Page
from spinta.core.ufuncs import Env
from spinta.datasets.components import ExternalBackend


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


def encode_page_values(env: BaseQueryBuilder, row: dict):
    if isinstance(env.model.backend, ExternalBackend):
        return base64.urlsafe_b64encode(json.dumps([row[item.prop.external.name] for item in env.page.page_.by.values()]).encode('ascii'))
    else:
        return base64.urlsafe_b64encode(json.dumps([row[item.prop.name] for item in env.page.page_.by.values()]).encode('ascii'))


def merge_with_page_selected_list(select: list, page: QueryPage):
    merged_selected = select or []
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
