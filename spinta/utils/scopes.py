import hashlib
import re

scope_re = re.compile(r'[^a-z0-9]+', flags=re.IGNORECASE)


def name_to_scope(
    template: str,
    name: str,
    *,
    maxlen: int = None,
    params: dict = None,
) -> str:
    """Return scope by given template possibly shortened on name part.
    """
    scope = template.format(name=name, **params)
    if maxlen and len(scope) > maxlen:
        surplus = len(scope) - maxlen
        name = name[:len(name) - surplus - 8] + hashlib.sha1(name.encode()).hexdigest()[:8]
        scope = template.format(name=name, **params)
    scope = scope_re.sub('_', scope).lower()
    return scope
