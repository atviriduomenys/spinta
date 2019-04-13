from spinta.components import Context, Command, CommandList
from spinta.commands import load


@load.register()
def load(
    context: Context, command: Command, data: dict, *,
    scope: str,
    argname: str = None,
) -> Command:
    config = context.get('config')
    name, args = next(iter(data.items()))
    command.name = name
    command.command = config.commands[scope][name]
    if isinstance(args, str):
        args = {command.command.schema.get('argname', argname): args}
    command.args = args
    return command


@load.register()
def load(
    context: Context, command: CommandList, data: list, *,
    scope: str,
    argname: str = None,
) -> CommandList:
    command.commands = [
        load(context, Command(), x, scope=scope, argname=argname)
        for x in data
    ]
    return command
