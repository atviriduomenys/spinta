from spinta.types import Type
from spinta.commands import Command


class CommandType(Type):
    metadata = {
        'name': 'command',
    }


class CommandExecutor:

    def __init__(self):
        self.commands = []

    def execute(self):
        for cmd in self.commands:
            cmd.execute()


class Prepare(Command):
    metadata = {
        'name': 'prepare',
        'type': 'command',
    }

    def execute(self):
        value = super().execute()

        if value is None:
            return value

        if not isinstance(value, list):
            value = [value]

        executor = CommandExecutor()

        for cmd in value:
            if not isinstance(cmd, dict):
                self.error(f"Commands must be objects in {{command: {{argument: value}}}} form. Got: {cmd!r}.")

            name, args = next(iter(cmd.items()))

            if name not in self.store.available_commands:
                self.error(f"Unknown command {name!r}.")

            cmd = self.run(self.args.obj, {'prepare.command': {'cmd': cmd}})
            executor.commands.append(cmd)

        return executor
