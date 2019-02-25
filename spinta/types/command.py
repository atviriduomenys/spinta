from spinta.types import Type
from spinta.commands import Command


class CommandType(Type):
    metadata = {
        'name': 'command',
    }


class CommandListType(Type):
    metadata = {
        'name': 'command_list',
    }


class PrepareCommandList(Command):
    metadata = {
        'name': 'prepare',
        'type': 'command_list',
        'arguments': {
            'obj': {'type': None},
            'prop': {'type': 'string'},
            'value': {'type': None},
        }
    }

    def execute(self):
        value = super().execute()

        if value is None:
            return value

        if not isinstance(value, list):
            value = [value]

        obj = self.load({'type': 'command', 'parent': self.obj}, bare=True)
        value = [
            self.run(obj, {'prepare': {
                **self.args.args,
                'value': command,
            }})
            for command in value
        ]

        return value


class Prepare(Command):
    metadata = {
        'name': 'prepare',
        'type': 'command',
        'arguments': {
            'obj': {'type': None},
            'prop': {'type': 'string'},
            'value': {'type': None},
        }
    }

    def execute(self):
        value = super().execute()

        if value is None:
            return value

        if not isinstance(value, dict):
            self.error("{self.args.prop!r} must be a dict.")

        name, args = next(iter(value.items()))

        if name not in self.store.available_commands:
            self.error(f"Unknown command {name!r}.")

        if isinstance(args, str):
            argument = self.store.available_commands[name]['argument']
            args = {argument: args}

        return {name: args}
