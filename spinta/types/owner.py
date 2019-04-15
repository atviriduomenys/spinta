from spinta.commands import check
from spinta.components import Context, Node


class Owner(Node):
    metadata = {
        'name': 'owner',
        'properties': {
            'sector': {'type': 'string'},
            'logo': {'type': 'path'},
        },
    }

    def __init__(self):
        super().__init__()
        self.logo = None
        self.sector = None


@check.register()
def check(context: Context, owner: Owner):
    if owner.logo:
        path = owner.manifest.path / 'media/owners' / owner.name / owner.logo
        if not path.exists():
            context.error("Can't find media file %s.", path)
