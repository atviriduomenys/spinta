from spinta import commands
from spinta.auth import authorized
from spinta.components import Context, Namespace
from spinta.core.enums import Action
from spinta.manifests.components import Manifest


@commands.authorize.register(Context, Action, Namespace, Manifest)
def authorize(context: Context, action: Action, ns: Namespace, manifest: Manifest):
    authorized(context, ns, action, throw=True)
