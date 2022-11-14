from typing import Optional

from spinta.types.datatype import DataType


class Money(DataType):
    amount: int = None  # Money amount
    currency: Optional[str] = None  # Currency (tree letter code).
