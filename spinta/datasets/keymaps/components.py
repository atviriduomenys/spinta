from typing import Optional, Any

from spinta.components import Component


class KeyMap(Component):
    name: str = None

    def encode(self, name: str, value: Any, primary_key: Optional[str] = None) -> Optional[str]:
        raise NotImplementedError

    def decode(self, name: str, key: str) -> object:
        raise NotImplementedError
