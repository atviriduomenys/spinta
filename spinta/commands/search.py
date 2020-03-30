from spinta.backends.components import Backend
from spinta.commands import is_object_id, load, load_search_params, load_operator_value, prepare
from spinta.components import Context
from spinta.exceptions import InvalidValue, EmptyStringSearch
from spinta.types.datatype import Array, Integer, DataType, PrimaryKey, String


@load_search_params.register()
def load_search_params(
    context: Context,
    dtype: DataType,
    backend: Backend,
    query_params: dict,
) -> object:
    value = query_params['args'][1]
    value = load(context, dtype, value)
    load_operator_value(context, backend, dtype, value, query_params=query_params)
    return prepare(context, dtype, backend, value)


@load_search_params.register()
def load_search_params(
    context: Context,
    dtype: String,
    backend: Backend,
    query_params: dict,
) -> object:
    value = query_params['args'][1]
    value = load(context, dtype, value)
    operator = query_params['name']
    if operator in ('startswith', 'contains') and value == '':
        raise EmptyStringSearch(dtype, op=operator)
    load_operator_value(context, backend, dtype, value, query_params=query_params)
    return prepare(context, dtype, backend, value)


@load_search_params.register()
def load_search_params(
    context: Context,
    dtype: PrimaryKey,
    backend: Backend,
    query_params: dict,
) -> object:
    value = query_params['args'][1]
    operator = query_params['name']
    if operator in ('startswith', 'contains') and value == '':
        raise EmptyStringSearch(dtype, op=operator)

    if (
        operator not in ('startswith', 'contains') and
        not is_object_id(context, backend, dtype.prop.model, value) or
        value is None
    ):
        raise InvalidValue(dtype)
    load_operator_value(context, backend, dtype, value, query_params=query_params)
    return prepare(context, dtype, backend, value)


@load_search_params.register()
def load_search_params(
    context: Context,
    dtype: Integer,
    backend: Backend,
    query_params: dict,
) -> object:

    # try to convert string search parameter to integer
    # query_params are always string, thus for type loading and validation
    # to work, we need to convert that query value string into appropriate type
    try:
        value = int(query_params['args'][1])
    except ValueError:
        raise InvalidValue(dtype)

    value = load(context, dtype, value)
    load_operator_value(context, backend, dtype, value, query_params=query_params)
    return prepare(context, dtype, backend, value)


@load_search_params.register()
def load_search_params(
    context: Context,
    dtype: Array,
    backend: Backend,
    query_params: dict,
) -> object:
    # if dtype is Array - then we need to work with Array's items
    # i.e. we search a string inside an array, thus we must load string
    # dtype and not the array type.
    value = query_params['args'][1]
    value = load(context, dtype.items.dtype, value)
    load_operator_value(context, backend, dtype.items.dtype, value, query_params=query_params)
    return prepare(context, dtype, backend, value)
