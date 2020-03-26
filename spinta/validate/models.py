from spinta import commands
from spinta.core.context import Context
from spinta.components.node import Model
from spinta.core.write import DataItem


@commands.validate.register()
def validate(context: Context, model: Model, data: dict) -> None:
    pass


@commands.verify.register()
def verify(context: Context, model: Model, data: DataItem) -> None:
    pass
