from typing import Union, Any

from spinta.backends.constants import BackendFeatures
from spinta.components import Property, Page, UrlParams
from spinta.core.ufuncs import Expr, asttoexpr
from spinta.datasets.components import ExternalBackend
from spinta.types.text.components import Text
from spinta.types.text.helpers import determine_language_property_for_text
from spinta.ufuncs.basequerybuilder.components import BaseQueryBuilder, QueryParams, QueryPage, LiteralProperty
from spinta.ufuncs.helpers import merge_formulas
from spinta.utils.types import is_value_literal


def expandable_not_expanded(env: BaseQueryBuilder, prop: Property):
    # If backend does not support expand, assume it is always expanded
    if not env.backend.supports(BackendFeatures.EXPAND):
        return False

    return prop.dtype.expandable and (env.expand is None or (env.expand and prop not in env.expand))


def get_language_column(env: BaseQueryBuilder, dtype: Text):
    default_langs = env.context.get('config').languages
    prop = determine_language_property_for_text(dtype, env.query_params.lang_priority, default_langs)
    column = env.backend.get_column(env.table, prop)
    return column


def get_column_with_extra(env: BaseQueryBuilder, prop: Property):
    if isinstance(prop.dtype, Text):
        return get_language_column(env, prop.dtype)
    return env.backend.get_column(env.table, prop)


def get_page_values(env: BaseQueryBuilder, row: dict):
    if not env.page.page_.filter_only:
        if isinstance(env.model.backend, ExternalBackend):
            return [row[item.prop.external.name] for item in env.page.page_.by.values()]
        else:
            return [row[item.prop.name] for item in env.page.page_.by.values()]


def merge_with_page_selected_list(select_list: list, page: QueryPage):
    merged_selected = select_list or []
    if page.page_.enabled and page.select is not None:
        for select in page.select:
            if select not in merged_selected:
                merged_selected.append(select)
    return merged_selected


def merge_with_page_sort(sort: list, page: QueryPage):
    merged_sort = sort or []
    if page.page_.enabled and page.sort:
        merged_sort = page.sort
    return merged_sort


def merge_with_page_limit(limit: int, page: QueryPage):
    if page and page.size and page.page_.enabled:
        if limit and limit < page.size:
            return limit
        return page.size
    return limit


def update_query_with_url_params(query_params: QueryParams, url_params: UrlParams):
    query_params.prioritize_uri = url_params.fmt.prioritize_uri
    query_params.lang_priority = url_params.accept_langs
    query_params.lang = url_params.lang
    query_params.expand = url_params.expand


def _create_or_condition(condition_info: list):
    action = condition_info[0]
    page_by = condition_info[1]
    or_null = condition_info[2]

    result = {
        'name': action,
        'args': [{
            'name': 'bind',
            'args': [page_by.prop.name]
        }, page_by.value]
    }

    if or_null != -1:
        result = {
            'name': 'or',
            'args': [
                result,
                {
                    'name': 'eq' if or_null == 1 else 'ne',
                    'args': [{
                        'name': 'bind',
                        'args': [page_by.prop.name]
                    }, None]
                }
            ]}
    return result


def _get_null_action(by: str, needs_null: bool = False):
    if not needs_null:
        return -1

    if by.startswith('-'):
        return 0
    return 1


def get_pagination_compare_query(
    model_page: Page,
) -> Union[Expr, None]:
    filtered = model_page
    item_count = len(filtered.by.keys())
    where_list = []
    for i in range(item_count):
        where_list.append([])
    if not filtered.all_none() or not filtered.first_time:
        for i, (by, page_by) in enumerate(filtered.by.items()):
            for n in range(item_count):
                if n >= i:
                    if n == i:
                        if page_by.value is not None:
                            if by.startswith('-'):
                                where_list[n].append(('lt', page_by, _get_null_action(by)))
                            else:
                                where_list[n].append(('gt', page_by, _get_null_action(by, True)))
                        else:
                            if by.startswith('-'):
                                where_list[n].append(('ne', page_by, _get_null_action(by)))
                    else:
                        where_list[n].append(('eq', page_by, _get_null_action(by)))
    remove_list = []

    for i, (by, value) in enumerate(filtered.by.items()):
        if value.value is None and not by.startswith('-'):
            remove_list.append(where_list[i])
    for item in remove_list:
        where_list.remove(item)

    where_compare = {}
    for where in where_list:
        compare = {}
        for item in where:
            condition = _create_or_condition(item)
            if compare:
                compare = {
                    'name': 'and',
                    'args': [
                        compare,
                        condition
                    ]
                }
            else:
                compare = condition
        if where_compare:
            where_compare = {
                'name': 'or',
                'args': [
                    where_compare,
                    compare
                ]
            }
        else:
            where_compare = compare

    if where_compare:
        where_compare = asttoexpr(where_compare)

    query = where_compare or None
    return query


def add_page_expr(expr: Expr, page: Page):
    return merge_formulas(expr, asttoexpr({
        'name': 'paginate',
        'args': [
            page
        ]
    }))


def process_literal_value(value: Any) -> Any:
    if is_value_literal(value):
        return LiteralProperty(value)
    return value
