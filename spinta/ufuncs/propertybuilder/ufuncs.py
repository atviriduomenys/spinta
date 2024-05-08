from spinta.components import Property
from spinta.core.ufuncs import ufunc
from spinta.types.datatype import Integer
from spinta.ufuncs.propertybuilder.components import PropertyBuilder


@ufunc.resolver(PropertyBuilder)
def count(env: PropertyBuilder):
    prop = Property()
    prop.name = 'count()'
    prop.place = 'count()'
    prop.title = ''
    prop.description = ''
    prop.model = env.model
    prop.dtype = Integer()
    prop.dtype.type = 'integer'
    prop.dtype.type_args = []
    prop.dtype.name = 'integer'
    prop.dtype.prop = prop
    return prop
