from typing import List
import sqlalchemy as sa

from spinta import commands
from spinta.auth import get_default_auth_client_id
from spinta.components import Context, Action, Config, Namespace
from spinta.core.enums import Access
from spinta.exceptions import AuthorizedClientsOnly
from spinta.manifests.internal_sql.components import InternalSQLManifest
from spinta.utils.enums import get_enum_by_name
from spinta.utils.scopes import name_to_scope


def get_transaction_connection(context: Context):
    if context.has('transaction.manifest'):
        return context.get('transaction.manifest').connection
    return None


def get_namespace_highest_access(context: Context, manifest: InternalSQLManifest, namespace: str):
    conn = get_transaction_connection(context)
    highest = None
    if conn is not None:
        table = manifest.table
        results = conn.execute(sa.select(table.c.access, sa.func.min(table.c.mpath).label('mpath')).where(
            sa.and_(
                table.c.mpath.startswith(namespace),
                sa.or_(
                    table.c.dim == 'ns',
                    table.c.dim == 'dataset',
                    table.c.dim == 'model',
                    table.c.dim == 'property'
                ),
            )
        ).group_by(table.c.access))

        for result in results:
            if result['access'] is not None:
                enum = get_enum_by_name(Access, result['access'])
                if highest is None or enum > highest:
                    highest = enum
    else:
        objs = manifest.get_objects()
        if namespace in objs['ns']:
            highest = objs['ns'][namespace].access
    return highest if highest is not None else manifest.access


def internal_authorized(
    context: Context,
    name: str,
    access: Access,
    action: Action,
    parents: List[str],
    *,
    throw: bool = False,
):
    config: Config = context.get('config')
    token = context.get('auth.token')

    # Unauthorized clients can only access open nodes.
    unauthorized = token.get_client_id() == get_default_auth_client_id(context)

    open_node = access >= Access.open
    if unauthorized and not open_node:
        if throw:
            raise AuthorizedClientsOnly()
        else:
            return False

    # Private nodes can only be accessed with explicit node scope.
    scopes = [name]

    # Protected and higher level nodes can be accessed with parent nodes scopes.
    if access > Access.private:
        scopes.extend(parents)

    if not isinstance(action, (list, tuple)):
        action = [action]
    scopes = [
        internal_scope_formatter(context, scope, act)
        for act in action
        for scope in scopes
    ]

    # Check if client has at least one of required scopes.
    if throw:
        token.check_scope(scopes, operator='OR')
    else:
        return token.valid_scope(scopes, operator='OR')


def internal_scope_formatter(
    context: Context,
    name: str,
    action: Action,
) -> str:
    config = context.get('config')

    return name_to_scope(
        '{prefix}{name}_{action}' if name else '{prefix}{action}',
        name,
        maxlen=config.scope_max_length,
        params={
            'prefix': config.scope_prefix,
            'action': action.value,
        },
    )


@commands.authorize.register(Context, Action, Namespace, InternalSQLManifest)
def authorize(context: Context, action: Action, ns: Namespace, manifest: InternalSQLManifest):
    parents = [parent.name for parent in ns.parents()]
    return internal_authorized(
        context,
        ns.name,
        get_namespace_highest_access(
            context,
            manifest,
            ns.name
        ),
        action,
        parents,
        throw=True
    )
