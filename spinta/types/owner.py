from spinta.commands import check, load
from spinta.components import Context, Node, Manifest
from spinta.nodes import load_node


class Owner(Node):
    schema = {
        'title': {'type': 'string'},
        'sector': {'type': 'string'},
        'logo': {'type': 'path'},
        'backend': {'type': 'backend', 'inherit': True, 'required': False},
    }

    def __init__(self):
        super().__init__()
        self.logo = None
        self.sector = None


@load.register()
def load(context: Context, owner: Owner, data: dict, manifest: Manifest):
    return load_node(context, owner, data, manifest)


@check.register()
def check(context: Context, owner: Owner):
    if owner.logo:
        path = owner.manifest.path / 'media/owners' / owner.name / owner.logo
        if not path.exists():
            raise exceptions.FileNotFound(str(path), owner)
