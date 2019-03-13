from spinta.commands import Command


class Replace(Command):
    metadata = {'name': 'replace'}

    def execute(self):
        return self.args.source.get(self.args.data, self.args.data)


class Hint(Command):
    metadata = {'name': 'hint'}

    def execute(self):
        return None


class Self(Command):
    metadata = {'name': 'self'}

    def execute(self):
        return None


class Chain(Command):
    metadata = {'name': 'chain'}

    def execute(self):
        return None


class All(Command):
    metadata = {'name': 'all'}

    def execute(self):
        return None


class Denormalize(Command):
    metadata = {'name': 'denormalize'}

    def execute(self):
        return None


class Unstack(Command):
    metadata = {'name': 'unstack'}

    def execute(self):
        return None


class List(Command):
    metadata = {'name': 'list'}

    def execute(self):
        result = []
        for commands in self.args.commands:
            for command in commands:
                name, args = next(iter(command.items()))
                result.append(
                    self.run(self.obj, {name: {**self.args.args, **args}})
                )
        return result


class Url(Command):
    metadata = {'name': 'url'}

    def execute(self):
        return None


class Range(Command):
    metadata = {'name': 'range'}

    def execute(self):
        return range(self.args.start, self.args.stop + 1)
