from spinta.types import Type
from spinta.commands import Command


class CommandType(Type):
    metadata = {
        'name': 'command',
    }


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

        for i, cmd in enumerate(value):
            if not isinstance(cmd, dict):
                parent_cmd = self.find_parent_command()
                if parent_cmd is None:
                    cmd = {'url': {'source': cmd}}
                else:
                    name, _ = next(iter(parent_cmd.items()))
                    cmd = {name: {'source': cmd}}

            if not isinstance(cmd, dict):
                self.error(f"Commands must be objects in {{command: {{argument: value}}}} form. Got: {cmd!r}.")

            name, args = next(iter(cmd.items()))

            if name not in self.store.available_commands:
                self.error(f"Unknown command {name!r}.")

            if not isinstance(args, dict):
                args = {'source': args}

            if name == 'list':
                args['source'] = [
                    self.run(self.obj, {'prepare': {
                        **self.args.args,
                        'value': x,
                    }})
                    for x in args['source']
                ]

            value[i] = {name: args}

        return value

    def find_parent_command(self):
        parent = self.args.obj
        while hasattr(parent, 'parent'):
            parent = parent.parent
            commands = getattr(self.args.obj.parent, self.args.prop, [])
            commands = commands if isinstance(commands, list) else [commands]
            for command in reversed(commands):
                if isinstance(command, dict):
                    return command
