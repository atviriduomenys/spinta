from typing import Optional

from spinta.components import Component


class KeyMap(Component):
    type: str = None
    dsn: str = None
    name: str = None

    @staticmethod
    def detect_from_url(url: str) -> bool:
        return False

    def encode(self, name: str, value: object) -> Optional[str]:
        raise NotImplementedError

    def decode(self, name: str, key: str) -> object:
        raise NotImplementedError
