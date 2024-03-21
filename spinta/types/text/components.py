from typing import Dict
from typing import Optional

from spinta.components import Property
from spinta.exceptions import PropertyNotFound
from spinta.types.datatype import DataType


class Text(DataType):
    schema = {
        'langs': {'type': 'object'},
        'hidden': {'type': 'bool', 'default': True}
    }
    requires_source = False
    langs: Optional[Dict[str, Property]]
