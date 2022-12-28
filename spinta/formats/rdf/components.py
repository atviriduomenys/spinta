import dataclasses
from typing import Optional, Dict, Any

from spinta.formats.components import Format


class Rdf(Format):
    content_type = 'application/rdf+xml'
    accept_types = {
        'application/rdf+xml',
    }
    params = {}


@dataclasses.dataclass
class Cell:
    value: str
    about: Optional[str] = None
    prefix: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        data = {
            'value': self.value,
            'about': self.about,
            'prefix': self.prefix
        }
        return {
            k: v
            for k, v in data.items()
            if k == 'value' or v is not None
        }
