from spinta.accesslog import create_accesslog
from spinta.auth import AdminToken, get_default_auth_client_id
from spinta.auth import BearerTokenValidator
from spinta.auth import Token
from spinta.auth import create_client_access_token
from spinta.components import Context


def require_auth(context: Context, client: str = None):
    # TODO: probably commands should also use an existing token in order to
    #       track who changed what.

    # If `client` is not None, make sure, that you run this command after config is initialized
    # And client migrations have happened.
    if client is None:
        token = AdminToken()
    else:
        if client == 'default':
            config = context.get('config')
            client = get_default_auth_client_id(context)
            client = client if client else config.default_auth_client
        token = create_client_access_token(context, client)
        token = Token(token, BearerTokenValidator(context))
    context.set('auth.token', token)
    context.attach('accesslog', create_accesslog, context, loaders=(
        context.get('store'),
        context.get("auth.token"),
    ))
