from spinta.commands import command
from spinta.components import Context


@command()
def replace():
    pass


@replace.register()
def replace(context: Context, *, source: dict, data):
    return source.get(data, data)


@command()
def hint():
    pass


@command()
def self():
    pass


@command()
def chain():
    pass


@command('all')
def all_():
    pass


@command()
def denormalize():
    pass


@command()
def unstack():
    pass


@command('list')
def list_():
    pass


@list_.register()
def list_(context: Context, *, commands, **kwargs):
    result = []
    for commands_ in commands:
        for cmd in commands_:
            result.append(cmd(context, **kwargs))
    return result


@command()
def url():
    pass


@command('range')
def range_():
    pass


@range_.register()
def range_(context: Context, *, start: int = 0, stop: int = 0):
    return range(start, stop + 1)
