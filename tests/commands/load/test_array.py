from spinta import commands
from spinta.components import Model
from spinta.types.datatype import Array
from spinta.types.datatype import Object


# TODO: There are at least 3 exaclty the same functions, move it to
#       spinta.testing.
def create_model(context, schema):
    manifest = context.get("store").manifest
    data = {
        "type": "model",
        "name": "Model",
        **schema,
    }
    model = Model()
    model.eid = "9244f3a6-a672-4aac-bb1c-831646264a51"
    commands.load(context, model, data, manifest)
    commands.link(context, model)
    return model


def show_lists(dtype, parent=()):
    if isinstance(dtype, Model) or dtype.prop.list is None:
        listname = None
    else:
        listname = dtype.prop.list.place
    if isinstance(dtype, (Model, Object)):
        if isinstance(dtype, Model):
            this = {}
        else:
            this = {".".join(parent): listname}
        return {
            **this,
            **{
                k: v
                for prop in dtype.properties.values()
                for k, v in show_lists(prop.dtype, parent + (prop.name,)).items()
            },
        }
    elif isinstance(dtype, Array):
        return {".".join(parent): listname, **show_lists(dtype.items.dtype, parent[:-1] + (parent[-1] + "[]",))}
    elif dtype.prop.name.startswith("_"):
        return {}
    else:
        return {".".join(parent): listname}


def test_array_refs(context):
    model = create_model(
        context,
        {
            "properties": {
                "scalar": {"type": "string"},
                "list": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "foo": {"type": "string"},
                            "bar": {"type": "array", "items": {"type": "string"}},
                        },
                    },
                },
            },
        },
    )
    assert show_lists(model) == {
        "list": None,
        "list[]": "list",
        "list[].bar": "list",
        "list[].bar[]": "list.bar",
        "list[].foo": "list",
        "scalar": None,
    }
