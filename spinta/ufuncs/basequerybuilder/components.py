from typing import Any

from spinta.components import Page
from spinta.core.ufuncs import Env


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


def merge_with_page_selected_dict(select: dict, page: QueryPage):
    merged_selected = select or {}
    if page.select:
        for key, item in page.select.items():
            if key not in merged_selected:
                merged_selected[key] = item
    return merged_selected


def merge_with_page_selected_list(select: list, page: QueryPage):
    merged_selected = select or []
    if page.select is not None:
        for select in page.select:
            if select not in merged_selected:
                merged_selected.append(select)
    return merged_selected


def merge_with_page_sort(sort: list, page: QueryPage):
    merged_sort = sort or []
    if page.sort:
        merged_sort = page.sort
    return merged_sort


def merge_with_page_limit(limit: int, page: QueryPage):
    if page and page.size:
        if limit and limit < page.size:
            return limit
        return page.size
    return limit
