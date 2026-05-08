from spinta.components import ExtraMetaData
from spinta.core.enums import Access, Visibility, Status
from spinta.core.ufuncs import Expr, Env
from spinta.manifests.components import Manifest


class ScopeLoader(Env):
    manifest: Manifest


class ScopeGiven:
    access: str = None


class Scope(ExtraMetaData):
    name: str
    prepare: Expr
    access: Access = None
    level: str = None
    status: Status | None = None
    visibility: Visibility | None = None
    count: int | None = None
    eli: str | None = None
    uri: str | None = None
    title: str = None
    description: str = None
    schema = {
        "name": {"type": "string"},
        "prepare": {"type": "spyna"},
        "access": {
            "type": "string",
            "choices": Access,
            "inherit": "model.access",
            "default": "private",
        },
        "level": {"type": "string"},
        "status": {
            "type": "string",
            "choices": Status,
            "default": "develop",
        },
        "visibility": {"type": "string", "choices": Visibility, "default": "private"},
        "count": {"type": "integer"},
        "eli": {"type": "string"},
        "uri": {"type": "string"},
        "title": {"type": "string"},
        "description": {"type": "string"},
    }

    def __init__(self):
        self.given = ScopeGiven()
