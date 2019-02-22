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

        # Find previous command name.
        if self.obj.parent and hasattr(self.obj.parent, self.args.prop):
            parent = getattr(self.obj.parent, self.args.prop)
            cmd = getattr(self.obj.parent, self.args.prop)[-1]
            previous_command_name, _ = next(iter(cmd.items()))
        else:
            previous_command_name = 'url'

        if value == ['label']:
            print(self.args.prop)
            print(self.obj)
            print(self.obj.parent)
            print(previous_command_name)

        for i, cmd in enumerate(value):
            if isinstance(cmd, dict):
                name, args = next(iter(cmd.items()))
            else:
                name = previous_command_name
                args = cmd

            if name not in self.store.available_commands:
                self.error(f"Unknown command {name!r}.")

            if not isinstance(args, dict):
                if 'argument' not in self.store.available_commands[name]:
                    self.error(f"Command {name!r} does not define single argument name you must set it in available_commands.")
                args = {self.store.available_commands[name]['argument']: args}

            if name == 'list':
                args['commands'] = [
                    self.run(self.obj, {'prepare': {
                        **self.args.args,
                        'value': x,
                    }})
                    for x in args['commands']
                ]

            value[i] = {name: args}
            previous_command_name = name

        return value
