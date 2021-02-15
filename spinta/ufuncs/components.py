from __future__ import annotations

from typing import List
from typing import Optional

from spinta.components import Property
from spinta.types.datatype import DataType
from spinta.types.datatype import Ref


class ForeignProperty:
    """Representation of a reference.

    When querying `/city?select(country.code)`, `left` points to `country` and
    `right` points to `code`. `chain` will be:

        [
            ForeignProperty(country:Ref, code:String),
        ]

    Multiple references can be joined like this:

        /city?select(country.continent.planet.name)

    `chain` will look like this:

        [
            ForeignProperty(country:Ref, continent:Ref),
            ForeignProperty(continent:Ref, name:String),
        ]

    """

    left: Ref
    right: DataType
    chain: List[ForeignProperty]
    name: str

    def __init__(
        self,
        fpr: Optional[ForeignProperty],
        left: Ref,
        right: DataType,
    ):
        if fpr is None:
            self.name = left.prop.place
            self.chain = [self]
        else:
            self.name = self / left.prop
            self.chain = fpr.chain + [self]

        self.left = left
        self.right = right

    def __repr__(self):
        place = self / self.right.prop
        return f'<{place}:{self.right.prop.name}>'

    def __truediv__(self, prop: Property) -> str:
        return f'{self.name}->{prop.place}'
