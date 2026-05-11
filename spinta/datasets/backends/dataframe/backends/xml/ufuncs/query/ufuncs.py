from spinta.core.ufuncs import ufunc
from spinta.datasets.backends.dataframe.backends.xml.ufuncs.query.components import XmlQueryBuilder
from spinta.datasets.backends.dataframe.ufuncs.query.components import DaskSelected as Selected
from spinta.types.datatype import Denorm, PrimaryKey, Ref
from spinta.types.text.components import Text
from spinta.utils.data import take


@ufunc.resolver(XmlQueryBuilder, Denorm)
def select(env: XmlQueryBuilder, dtype: Denorm) -> Selected:
    return Selected(item=dtype.prop.place, prop=dtype.prop)


@ufunc.resolver(XmlQueryBuilder, Text)
def select(env: XmlQueryBuilder, dtype: Text) -> Selected:
    prep = {}
    for lang, prop in dtype.langs.items():
        prep[lang] = env.call("select", prop)
    return Selected(prop=dtype.prop, prep=prep)


@ufunc.resolver(XmlQueryBuilder, PrimaryKey)
def select(env: XmlQueryBuilder, dtype: PrimaryKey) -> Selected:
    model = dtype.prop.model
    pkeys = model.external.pkeys
    if not pkeys:
        pkeys = take(model.properties).values()

    result = env.call("select", pkeys[0]) if len(pkeys) == 1 else [env.call("select", prop) for prop in pkeys]
    return Selected(prop=dtype.prop, prep=result)


@ufunc.resolver(XmlQueryBuilder, Ref)
def select(env: XmlQueryBuilder, dtype: Ref) -> Selected:
    prep = {"_id": Selected(item=dtype.prop.external.name, prop=dtype.prop)}
    for prop in dtype.properties.values():
        sel = env.call("select", prop)
        prep[prop.name] = sel
    return Selected(prop=dtype.prop, prep=prep)
