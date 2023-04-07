from spinta.components import Context
from spinta.commands import load
from spinta.types.text.components import Text


# @load.register(Context, Text, object)
# def load(context: Context, dtype: Text, value: object) -> object:
#     # loads value to python native value according to given type
#     print(dtype, type(value))
#     return dtype.load(value)
