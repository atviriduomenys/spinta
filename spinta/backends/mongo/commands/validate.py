from spinta import commands
from spinta.components import Context, Action, DataItem, Property
from spinta.types.datatype import DataType
from spinta.exceptions import UniqueConstraint
from spinta.backends.mongo.components import Mongo


@commands.check_unique_constraint.register()
def check_unique_constraint(
    context: Context,
    data: DataItem,
    dtype: DataType,
    prop: Property,
    backend: Mongo,
    value: object,
):
    model = prop.model
    table = backend.db[model.model_type()]
    # XXX: Probably we should move this out of mongo backend and implement
    #      this on spinta.commands.write. For example, read_existing_data
    #      could try to read existing record if `_id` or any other unique
    #      field is given. Also this would fix case, when multiple
    #      properties are given as unique constraint.
    if prop.name == '_id':
        name = '__id'
    else:
        name = prop.name
    # TODO: Add support for nested properties.
    # FIXME: Exclude currently saved value.
    #        In case of an update, exclude currently saved value from
    #        uniqueness check.
    if data.action in (Action.UPDATE, Action.PATCH):
        if name == '__id' and value == data.saved['_id']:
            return

        result = table.find_one({
            '$and': [{name: value},
                     {'__id': {'$ne': data.saved['_id']}}],
        })
    else:
        result = table.find_one({name: value})
    if result is not None:
        raise UniqueConstraint(prop, value=value)
