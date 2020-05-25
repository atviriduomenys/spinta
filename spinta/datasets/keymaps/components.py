from typing import Optional

from spinta.components import Component


class KeyMap(Component):
    name: str = None

    def encode(self, name: str, value: object) -> Optional[str]:
        raise NotImplementedError

    def decode(self, name: str, key: str) -> object:
        raise NotImplementedError
