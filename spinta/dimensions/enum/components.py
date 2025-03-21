from __future__ import annotations

from typing import Dict

from spinta.components import Model, ExtraMetaData
from spinta.core.enums import Access, Status, Visibility
from spinta.core.ufuncs import Env
from spinta.core.ufuncs import Expr
from spinta.dimensions.lang.components import LangData
from spinta.utils.schema import NA


class EnumFormula(Env):
    model: Model


class EnumValueGiven:
    access: str = None


class EnumItem(ExtraMetaData):
    source: str
    prepare: Expr
    access: Access = None
    title: str
    description: str
    given: EnumValueGiven
    lang: LangData
    level: str
    status: Status | None = None
    visibility: Visibility | None = None
    eli: str | None = None
    count: int | None = None

    schema = {
        'name': {'type': 'string'},
        'source': {'type': 'string'},
        'prepare': {'type': 'spyna', 'default': NA},
        'access': {
            'type': 'string',
            'choices': Access,
            'inherit': 'model.access',
            'default': 'protected',
        },
        'title': {'type': 'string'},
        'description': {'type': 'string'},
        'lang': {'type': 'object'},
        'level': {'type': 'string'},
        'status': {
            'type': 'string',
            'choices': Status,
            'default': 'develop',
        },
        'visibility': {
            'type': 'string',
            'choices': Visibility,
            'default': 'private'
        },
        'eli': {'type': 'string'},
        'count': {'type': 'integer'},
    }

    def __init__(self):
        self.given = EnumValueGiven()


EnumValue = Dict[str, EnumItem]
Enums = Dict[str, EnumValue]
