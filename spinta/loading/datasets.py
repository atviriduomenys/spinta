from spinta import commands
from spinta.core.context import Context
from spinta.components.datasets import Owner
from spinta.exceptions.loading import FileNotFound


@commands.check.register()
def check(context: Context, owner: Owner):
    if owner.logo:
        path = owner.manifest.path / 'media/owners' / owner.name / owner.logo
        if not path.exists():
            raise FileNotFound(str(path), owner)
