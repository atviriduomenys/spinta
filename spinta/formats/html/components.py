import dataclasses
from enum import Enum
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from spinta.formats.components import Format


class Html(Format):
    content_type = 'text/html'
    accept_types = {
        'text/html',
    }
    params = {}
    streamable = False


class Color(str, Enum):
    change = '#B2E2AD'
    null = '#f5f5f5'


@dataclasses.dataclass
class Cell:
    value: str
    link: Optional[str] = None
    color: Optional[Color] = None

    def as_dict(self) -> Dict[str, Any]:
        data = {
            'value': self.value,
            'link': self.link,
            'color': str(self.color.value) if self.color else None,
        }
        return {
            k: v
            for k, v in data.items()
            if k == 'value' or v is not None
        }


ComplexCell = Union[
    Cell,
    Dict[str, 'ComplexCell'],
    List['ComplexCell'],
]
