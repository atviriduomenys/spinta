from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest


@commands.prepare.register(Context, Manifest)
def prepare(context: Context, manifest: Manifest):
    store = context.get('store')

    for keymap in store.keymaps.values():
        commands.prepare(context, keymap)

    for backend in store.backends.values():
        commands.prepare(context, backend, manifest)
