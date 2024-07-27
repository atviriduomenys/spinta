from typing import Optional

import decimal

from spinta.types.datatype import DataType


class Money(DataType):
    amount: Optional[decimal.Decimal] = None  # Money amount
    currency: Optional[str] = None  # Currency (tree letter code).
