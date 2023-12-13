from spinta import commands
from spinta.auth import authorized
from spinta.components import Context, Action, Namespace
from spinta.manifests.components import Manifest


@commands.authorize.register(Context, Action, Namespace, Manifest)
def authorize(context: Context, action: Action, ns: Namespace, manifest: Manifest):
    authorized(context, ns, action, throw=True)
