from typing import Union, List

from spinta.components import Page, Property, PageBy
from spinta.core.ufuncs import ufunc, Expr, asttoexpr, Negative, Bind, Positive, Pair, Env
from spinta.exceptions import FieldNotInResource, InvalidArgumentInExpression, CannotSelectTextAndSpecifiedLang
from spinta.types.datatype import DataType, String
from spinta.types.text.components import Text
from spinta.ufuncs.basequerybuilder.components import BaseQueryBuilder, Star
from spinta.ufuncs.basequerybuilder.helpers import get_pagination_compare_query
from spinta.ufuncs.helpers import merge_formulas


@ufunc.resolver(BaseQueryBuilder, Expr, name='paginate')
def paginate(env, expr):
    if len(expr.args) != 1:
        raise InvalidArgumentInExpression(arguments=expr.args, expr='paginate')
    page = expr.args[0]
    if isinstance(page, Page):
        if page.is_enabled:
            if not page.filter_only:
                env.page.select = env.call('select', page)
                for by, page_by in page.by.items():
                    sorted_ = env.call('sort', Negative(page_by.prop.name) if by.startswith("-") else Bind(page_by.prop.name))
                    if sorted_ is not None:
                        if isinstance(sorted_, (list, set, tuple)):
                            env.page.sort += sorted_
                        else:
                            env.page.sort.append(sorted_)
            env.page.page_ = page
            env.page.size = page.size
            return env.resolve(get_pagination_compare_query(page))
    else:
        raise InvalidArgumentInExpression(arguments=expr.args, expr='paginate')


@ufunc.resolver(BaseQueryBuilder, Expr, name='expand')
def expand(env, expr):
    env.expand = []
    if expr.args:
        for arg in expr.args:
            resolved = env.resolve(arg)
            selected = env.call('select', resolved)
            env.expand.append(selected.prop)


@ufunc.resolver(BaseQueryBuilder, str, name='op')
def op_(env, arg: str):
    if arg == '*':
        return Star()
    else:
        raise NotImplementedError


@ufunc.resolver(BaseQueryBuilder, Expr, name='page')
def page_(env, expr):
    pass


@ufunc.resolver(BaseQueryBuilder, DataType, list)
def validate_dtype_for_select(env, dtype: DataType, selected_props: List[Property]):
    raise NotImplemented(f"validate_dtype_for_select with {dtype.name} is not implemented")


@ufunc.resolver(BaseQueryBuilder, Text, list)
def validate_dtype_for_select(env, dtype: Text, selected_props: List[Property]):
    for prop in selected_props:
        if dtype.prop.name == prop.name or prop.parent == dtype.prop:
            raise CannotSelectTextAndSpecifiedLang(dtype)


@ufunc.resolver(BaseQueryBuilder, String, list)
def validate_dtype_for_select(env, dtype: String, selected_props: List[Property]):
    if dtype.prop.parent and isinstance(dtype.prop.parent.dtype, Text):
        parent = dtype.prop.parent
        for prop in selected_props:
            if parent.name == prop.name or prop == dtype.prop:
                raise CannotSelectTextAndSpecifiedLang(parent.dtype)



