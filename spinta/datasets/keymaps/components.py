from datetime import datetime
from typing import Optional, Any

from spinta.components import Component


class KeyMap(Component):
    name: str = None

    def encode(self, name: str, value: Any, primary_key: Optional[str] = None) -> Optional[str]:
        raise NotImplementedError

    def decode(self, name: str, key: str) -> object:
        raise NotImplementedError

    def contains(self, name: str, value: Any) -> bool:
        raise NotImplementedError

    def has_synced_before(self) -> bool:
        raise NotImplementedError

    def get_last_synced_id(self, name: str) -> object:
        raise NotImplementedError

    def update_sync_data(self, name: str, cid: Any, time: datetime):
        raise NotImplementedError

    def synchronize(self, name: str, value: Any, primary_key: str):
        raise NotImplementedError
