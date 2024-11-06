import functools
import hashlib
import re
from typing import Any, Union, List
from typing import Dict

from spinta.components import Context, Action

scope_re = re.compile(r'[^a-z0-9]+', flags=re.IGNORECASE)


@functools.lru_cache(maxsize=1000)
def _sanitize(scope: str):
    return scope_re.sub('_', scope).lower()


def name_to_scope(
    template: str,
    name: str,
    *,
    maxlen: int = None,
    params: Dict[str, Any] = None,
) -> str:
    """Return scope by given template possibly shortened on name part.
    """
    scope = template.format(name=name, **params)
    if maxlen and len(scope) > maxlen:
        surplus = len(scope) - maxlen
        name = name[:len(name) - surplus - 8] + hashlib.sha1(name.encode()).hexdigest()[:8]
        scope = template.format(name=name, **params)
    return _sanitize(scope)


def get_scopes_from_context(context: Context, node: Any, action: Action) -> Union[List[str], None]:
    """Return list of scopes for given context, node (Namespace, Model, Property) and action.
    """
    # Import in function to avoid circular import
    from spinta.auth import get_default_auth_client_id
    default_client_id = get_default_auth_client_id(context)

    token = context.get('auth.token')
    client_id = token.get_client_id()

    if client_id != default_client_id:
        try:
            scopes = context.get("config").scope_formatter(context, node, action)
        except:
            return None
        return [scopes]
    return None
