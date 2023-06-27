from spinta.accesslog import create_accesslog
from spinta.auth import AdminToken, get_client_id_from_name, get_clients_path
from spinta.auth import BearerTokenValidator
from spinta.auth import Token
from spinta.auth import create_client_access_token
from spinta.components import Context


def require_auth(context: Context, client: str = None):
    # TODO: probably commands should also use an existing token in order to
    #       track who changed what.
    if client is None:
        token = AdminToken()
    else:
        if client == 'default':
            config = context.get('config')
            client = get_client_id_from_name(get_clients_path(config), config.default_auth_client)
            client = client if client else config.default_auth_client
        token = create_client_access_token(context, client)
        token = Token(token, BearerTokenValidator(context))
    context.set('auth.token', token)
    context.attach('accesslog', create_accesslog, context, loaders=(
        context.get('store'),
        context.get("auth.token"),
    ))
