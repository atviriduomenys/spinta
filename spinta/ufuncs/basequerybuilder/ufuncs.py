from typing import Union

from spinta.components import Page, Property, PageBy
from spinta.core.ufuncs import ufunc, Expr, asttoexpr, Negative, Bind, Positive, Pair
from spinta.exceptions import FieldNotInResource, InvalidArgumentInExpression
from spinta.ufuncs.basequerybuilder.components import BaseQueryBuilder, LoadBuilder
from spinta.ufuncs.helpers import merge_formulas


@ufunc.resolver(BaseQueryBuilder, Expr, name='paginate')
def paginate(env, expr):
    if len(expr.args) != 1:
        raise InvalidArgumentInExpression(arguments=expr.args, expr='paginate')
    page = expr.args[0]
    if isinstance(page, Page):
        if page.is_enabled:
            for by, page_by in page.by.items():
                sorted_ = env.call('sort', Negative(page_by.prop.name) if by.startswith("-") else Bind(page_by.prop.name))
                if sorted_ is not None:
                    env.page.sort.append(sorted_)
            env.page.page_ = page
            env.page.select = env.call('select', page)
            env.page.size = page.size
            return env.resolve(_get_pagination_compare_query(page))
    else:
        raise InvalidArgumentInExpression(arguments=expr.args, expr='paginate')


@ufunc.resolver(BaseQueryBuilder, Expr, name='page')
def page_(env, expr):
    pass


@ufunc.resolver(LoadBuilder, Expr, name='page')
def page_(env, expr):
    args = env.resolve(expr.args)
    page = Page()
    if len(args) > 0:
        for item in args:
            res = env.resolve(item)
            res = env.call('page_item', res)
            if isinstance(res[1], Property):
                page.by[res[0]] = PageBy(res[1])
            else:
                if res[0] == 'size' and isinstance(res[1], int):
                    page.size = res[1]
                else:
                    raise InvalidArgumentInExpression(arguments=res[0], expr='page')
    else:
        page.is_enabled = False
    return page


@ufunc.resolver(LoadBuilder, Pair, name='page_item')
def page_item(env, field):
    return field.name, field.value


@ufunc.resolver(LoadBuilder, Bind, name='page_item')
def page_item(env, field):
    if field.name in env.model.properties:
        prop = env.model.properties[field.name]
    else:
        raise FieldNotInResource(env.model, property=field.name)
    return prop.name, prop


@ufunc.resolver(LoadBuilder, Positive, name='page_item')
def page_item(env, field):
    return env.call('page_item', Bind(field.name))


@ufunc.resolver(LoadBuilder, Negative, name='page_item')
def page_item(env, field):
    resolved = env.call('page_item', Bind(field.name))
    return f'-{resolved[0]}', resolved[1]


@ufunc.resolver(LoadBuilder, Expr, name='and')
def and_(env, expr):
    args, kwargs = expr.resolve(env)
    args = [a for a in args if a is not None]
    return args


@ufunc.resolver(LoadBuilder, Expr, name='or')
def or_(env, expr):
    args, kwargs = expr.resolve(env)
    args = [a for a in args if a is not None]
    return args


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


def add_page_expr(expr: Expr, page: Page):
    return merge_formulas(expr, asttoexpr({
        'name': 'paginate',
        'args': [
            page
        ]
    }))
