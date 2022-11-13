from typing import Optional

from spinta.types.datatype import DataType


class Money(DataType):
    _amount: int = None  # Money amount
    _currency: Optional[str] = None  # Currency (tree letter code).
