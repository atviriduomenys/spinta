from typing import Union

from spinta.components import Page
from spinta.core.ufuncs import ufunc, Expr, asttoexpr, Negative, Bind
from spinta.ufuncs.basequerybuilder.components import BaseQueryBuilder


@ufunc.resolver(BaseQueryBuilder, Expr, name='page')
def _page(env, expr):
    if len(expr.args) != 1:
        raise Exception
    page = expr.args[0]
    if isinstance(page, Page):
        for by, page_by in page.by.items():
            sorted_ = env.call('sort', Negative(page_by.prop.name) if by.startswith("-") else Bind(page_by.prop.name))
            selected = env.call('select', page_by.prop)
            if selected:
                env.page.select.append(selected)
            if sorted_ is not None:
                env.page.sort.append(sorted_)
        env.page.page_ = page
        env.page.select = env.call('select', page)
        env.page.size = page.size
        return env.resolve(_get_pagination_compare_query(page))


def filter_page_values(page: Page):
    new_page = Page()
    for by, page_by in page.by.items():
        if page_by.value:
            new_page.by[by] = page_by
    return new_page


def _get_pagination_compare_query(
    model_page: Page,
) -> Union[Expr, None]:
    filtered = filter_page_values(model_page)
    item_count = len(filtered.by.keys())
    where_list = []
    for i in range(item_count):
        where_list.append([])
    for i, (by, page_by) in enumerate(filtered.by.items()):
        if page_by.value:
            for n in range(item_count):
                if n >= i:
                    if n == i:
                        if by.startswith('-'):
                            where_list[n].append(('lt', page_by))
                        else:
                            where_list[n].append(('gt', page_by))
                    else:
                        where_list[n].append(('eq', page_by))

    where_compare = {}
    for where in where_list:
        compare = {}
        for item in where:
            if compare:
                compare = {
                    'name': 'and',
                    'args': [
                        compare,
                        {
                            'name': item[0],
                            'args': [{
                                'name': 'bind',
                                'args': [item[1].prop.name]
                            }, item[1].value]
                        }
                    ]
                }
            else:
                compare = {
                    'name': item[0],
                    'args': [{
                        'name': 'bind',
                        'args': [item[1].prop.name]
                    }, item[1].value]
                }
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
