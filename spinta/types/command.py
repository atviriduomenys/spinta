from spinta.components import Context, Command, CommandList, Node
from spinta.commands import load, error


@load.register()
def load(
    context: Context, command: Command, data: dict, *,
    parent: Node,
    scope: str,
    argname: str = None,
) -> Command:
    config = context.get('config')
    name, args = next(iter(data.items()))
    command.name = name
    command.parent = parent
    command.command = config.commands[scope][name]
    if isinstance(args, str):
        args = {command.command.schema.get('argname', argname): args}
    command.args = args
    return command


@load.register()
def load(
    context: Context, command: CommandList, data: list, *,
    parent: Node,
    scope: str,
    argname: str = None,
) -> CommandList:
    command.parent = parent
    command.commands = [
        load(context, Command(), x, parent=parent, scope=scope, argname=argname)
        for x in data
    ]
    return command


@error.register()
def error(exc: Exception, context: Context, command: Command, data: object, **kwargs):
    error(exc, context, command.parent)
