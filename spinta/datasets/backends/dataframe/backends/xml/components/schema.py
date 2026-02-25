from typing import TypedDict
from typing import Optional
from typing import List


class ObjectData(TypedDict):
    _type: str
    _id: str
    _revision: str

class NotAvailable:
    def __repr__(self):
        return "<NA>"

    def __bool__(self):
        return False


NA = NotAvailable()