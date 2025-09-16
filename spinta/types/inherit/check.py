from spinta import commands
from spinta.components import Context
from spinta.exceptions import UndefinedPropertyType
from spinta.types.datatype import Inherit


@commands.check.register(Context, Inherit)
def check(context: Context, dtype: Inherit) -> None:
    property = dtype.prop
    if property.is_reserved():
        return None

    property_model = property.model
    property_name = property.name
    if not (base := property_model.base) or not base.parent.properties.get(property_name):
        raise UndefinedPropertyType(property_model, property=property_name)
