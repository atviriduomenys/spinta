from typing import Any


class PriorityKey:
    id: Any = None
    name: str = None
    source: Any = None

    def __init__(self, _id=None, name=None, source=None):
        self.id = _id
        self.name = name
        self.source = source

    def __eq__(self, other):
        if isinstance(other, PriorityKey):
            if self.id and other.id:
                if self.id == other.id:
                    return True
            if self.name and other.name:
                if self.name == other.name:
                    return True
            if self.source and other.source:
                if isinstance(other.source, tuple) or isinstance(self.source, tuple):
                    if set(self.source).issubset(other.source) or set(other.source).issubset(self.source):
                        return True
                else:
                    if self.source == other.source:
                        return True
        return False

    def __str__(self):
        return f"PriorityKey(id: {self.id}, name: {self.name}, source: {self.source})"

    def __hash__(self):
        return hash(str(self))
