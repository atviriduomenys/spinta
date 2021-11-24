import functools
import hashlib
import re
from typing import Any
from typing import Dict

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


