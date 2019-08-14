from spinta.backends import Backend
from spinta.commands import load, load_search_params, load_operator_value, prepare
from spinta.components import Context
from spinta.types.type import Array, Type


@load_search_params.register()
def load_search_params(
    context: Context,
    type_: Type,
    backend: Backend,
    query_params: dict,
) -> object:
    value = query_params['value']
    value = load(context, type_, value)
    load_operator_value(context, backend, type_, value, query_params=query_params)
    return prepare(context, type_, backend, value)


@load_search_params.register()
def load_search_params(
    context: Context,
    type_: Array,
    backend: Backend,
    query_params: dict,
) -> object:
    # if type_ is Array - then we need to work with Array's items
    # i.e. we search a string inside an array, thus we must load string
    # type_ and not the array type.
    value = query_params['value']
    value = load(context, type_.items.type, value)
    load_operator_value(context, backend, type_.items.type, value, query_params=query_params)
    return prepare(context, type_, backend, value)
