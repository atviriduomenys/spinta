from __future__ import annotations

from typing import Dict

from spinta.components import Model
from spinta.components import Node
from spinta.core.enums import Access
from spinta.core.ufuncs import Env
from spinta.core.ufuncs import Expr
from spinta.utils.schema import NA


class EnumFormula(Env):
    model: Model


class EnumValueGiven:
    access: str = None


class EnumItem(Node):
    source: str
    prepare: Expr
    access: Access
    title: str
    description: str
    given: EnumValueGiven

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
    }

    def __init__(self):
        self.given = EnumValueGiven()


EnumValue = Dict[str, EnumItem]
Enums = Dict[str, EnumValue]
