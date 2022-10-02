from typing import Dict
from typing import Optional

from spinta.components import Property
from spinta.types.datatype import DataType


class Text(DataType):
    schema = {
        'langs': {'type': 'object'},
    }

    langs: Optional[Dict[str, Property]]
