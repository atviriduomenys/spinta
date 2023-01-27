from typing import Optional

from spinta.components import Component


class KeyMap(Component):
    name: str = None

    def encode(self, name: str, value: object, primary_key:str, parent_table:str) -> Optional[str]:
        raise NotImplementedError

    def decode(self, name: str, key: str) -> object:
        raise NotImplementedError
