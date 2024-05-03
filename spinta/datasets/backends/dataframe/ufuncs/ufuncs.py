from spinta.components import Property
from spinta.core.ufuncs import ufunc
from spinta.datasets.backends.dataframe.commands.query import DaskDataFrameQueryBuilder, Selected
from spinta.datasets.backends.dataframe.ufuncs.components import TabularResource
from spinta.exceptions import UnknownBind
from spinta.types.datatype import Binary
import base64 as b64


@ufunc.resolver(TabularResource)
def tabular(env: TabularResource, **kwargs):
    for key, value in kwargs.items():
        if key == "sep":
            env.seperator = value
        else:
            raise UnknownBind(name=key)


@ufunc.resolver(DaskDataFrameQueryBuilder)
def base64(env: DaskDataFrameQueryBuilder) -> bytes:
    return env.call('base64', env.this)


@ufunc.resolver(DaskDataFrameQueryBuilder, Property)
def base64(env: DaskDataFrameQueryBuilder, prop: Property) -> bytes:
    return env.call('base64', prop.dtype)


@ufunc.resolver(DaskDataFrameQueryBuilder, Binary)
def base64(env: DaskDataFrameQueryBuilder, dtype: Binary) -> Selected:
    # return b64.decodebytes(value)
    item = f'base64({dtype.prop.external.name})'
    print("VALUE: ", env.dataframe[dtype.prop.external.name].values)
    env.dataframe[item] = env.dataframe[dtype.prop.external.name].str.encode('ascii').map(b64.decodebytes, meta=(dtype.prop.external.name, bytes))
    return Selected(
        item=item,
        prop=dtype.prop
    )
