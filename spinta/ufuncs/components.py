from __future__ import annotations

from typing import List
from typing import Optional

from spinta.components import Property
from spinta.core.ufuncs import Expr
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
    right: Optional[DataType]
    chain: List[ForeignProperty]
    name: str

    def __init__(
        self,
        fpr: Optional[ForeignProperty],
        left: Ref,
        right: DataType = None,
        *,
        # Used internally by swap method and only when fpr is None.
        _name: str = None,
        _chain: List[ForeignProperty] = None,
    ):
        if fpr is None:
            self.name = left.prop.place if _name is None else _name
            self.chain = [self] if _chain is None else (_chain + [self])
        else:
            self.name = fpr / left.prop
            self.chain = fpr.chain + [self]

        self.left = left
        self.right = right

    def __repr__(self):
        if self.right:
            place = self / self.right.prop
            return f'<{place}:{self.right.prop.name}>'
        else:
            return f'<{self.name}:None>'

    def __truediv__(self, prop: Property) -> str:
        return f'{self.name}->{prop.place}'

    def push(self, right: Optional[Property] = None):
        """Push another property to the chain

        Creates new ForeignProperty instance, by pushing another property to the
        chain. After this operation right will be pushed to left, and a new
        right will be placed instead of old one.
        """
        if isinstance(self.right, Ref):
            return ForeignProperty(self, self.right, right.dtype)
        elif self.right is None:
            return self.swap(right)
        else:
            raise RuntimeError(
                f"Can't push {right} to {self}, because right is a "
                f"{type(self.right)}, but it should be a Ref."
            )

    def swap(self, right: Optional[Property] = None):
        """Swap right with a new property

        Creates new ForeignProperty instance, preserving all properties, but
        change right to a new given right.
        """
        return ForeignProperty(
            None,
            self.left,
            right.dtype,
            _name=self.name,
            _chain=list(self.chain[:-1]),
        )

    def get_bind_expr(self) -> Expr:
        expr = self.right.get_bind_expr()
        for fpr in reversed(self.chain[:-1]):
            expr = Expr('getattr', fpr.right.get_bind_expr(), expr)
        return Expr('getattr', self.chain[0].left.get_bind_expr(), expr)
