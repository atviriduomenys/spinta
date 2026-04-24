from spinta.components import ExtraMetaData
from spinta.core.enums import Access
from spinta.core.ufuncs import Expr


class ScopeGiven:
    access: str = None


class Scope(ExtraMetaData):
    name: str
    prepare: Expr
    access: Access = None
    eli: str = None
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
        "title": {"type": "string"},
        "description": {"type": "string"},
        "eli": {"type": "string"},
    }

    def __init__(self):
        self.given = ScopeGiven()
