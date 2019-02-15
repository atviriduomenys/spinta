from spinta.commands import Command
from spinta.types import Type


class Owner(Type):
    metadata = {
        'name': 'owner',
        'properties': {
            'path': {'type': 'path', 'required': True},
            'manifest': {'type': 'manifest', 'required': True},
            'sector': {'type': 'string'},
            'logo': {'type': 'path'},
        },
    }


class CheckOwner(Command):
    metadata = {
        'name': 'manifest.check',
        'type': 'owner',
    }

    def execute(self):
        self.check_logo()

    def check_logo(self):
        if self.obj.logo:
            path = self.obj.manifest.path / 'media/owners' / self.obj.name / self.obj.logo
            if not path.exists():
                self.error("Can't find media file %s.", path)
