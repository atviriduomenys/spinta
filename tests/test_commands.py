from spinta import command
from spinta import Backend
from testing import migrate


@command(migrate)
def migrate(backend: Backend):
    pass


class A:
    pass


class B:
    pass


class C:
    pass


class CA(C):
    pass


class CommandManager:

    def __init__(self):
        self._components_from_commands = set()
        self._components = {}
        self._commands = {}

    def register(self, command, name, components):
        components = sorted(components)
        if components in self._commands.get(name, {}):
            raise Exception(f"Command {name!r} with components {components!r} is already defined.")
        if name not in self._commands:
            self._commands[name] = {}
        self._commands[name][components]
        self._components_from_commands.update(components)

    def bind(self, component, name=''):
        base = None
        for base in component.mro():
            if base in self._components_from_commands:
                break
        if base is None:
            base = component
        if base in self._components:
            raise Exception(f"Component {component!r} is already bound to {base!r}.")
        self._components[base] = component

    def run(self, command):
        return self._commands


def test_commands():
    components = {
        A: A(),
        B: B(),
        C: CA(),
    }

    commands = CommandManager()
    commands.register('foo', [(A, ''), (C, '')])

    assert commands.run('foo') == 1
