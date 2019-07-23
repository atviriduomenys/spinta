from spinta.commands import load, load_search_params
from spinta.components import Context
from spinta.types.type import Array, Type


@load_search_params.register()
def load_search_params(context: Context, type: Type, value: object):
    return load(context, type, value)


@load_search_params.register()
def load_search_params(context: Context, type: Array, value: object):
    # if type is Array - then we need to work with Array's items
    # i.e. we search a string inside an array, thus we must load string
    # type and not the array type.
    return load(context, type.items.type, value)
