from typing import Any

from spinta.components import Component


class DataModel(Component):
    """Data model component"""
    schema = {
        "data": {"type": "dict", "required": True}
    }
    data: dict[str, Any]

    def __dict__(self):
        return self.data