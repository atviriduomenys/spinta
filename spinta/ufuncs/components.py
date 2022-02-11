from __future__ import annotations

from typing import List
from typing import Optional

from spinta.components import Property
from spinta.core.ufuncs import Expr
from spinta.core.ufuncs import Unresolved
from spinta.exceptions import IncompatibleForeignProperties
from spinta.types.datatype import DataType
from spinta.types.datatype import Ref


class ForeignProperty(Unresolved):
    """Representation of a reference.

    When querying `/City?select(country.code)`, `left` points to `country` and
    `right` points to `code`. `chain` will be:

        [
            ForeignProperty(City, country:Country -> code:String),
        ]

    Multiple references can be joined like this:

        /City?select(country.continent.planet.name)

    `chain` will look like this:

        [
            ForeignProperty(
                City,
                country:Country ->
                continent:ref
            ),
            ForeignProperty(
                City,
                country:Country ->
                continent:Continent ->
                planet:ref
            ),
            ForeignProperty(
                City,
                country:Country ->
                continent:Continent ->
                planet:Planet ->
                name:string
            ),
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
        name = type(self).__name__
        model = self.chain[0].left.prop.model.name
        place = self / (self.right.prop if self.right else None)
        if self.right:
            return f'{name}({model}, {place}:{self.right.name})'
        else:
            return f'{name}({model}, {place})'

    def __truediv__(self, prop: Optional[Property]) -> str:
        if prop:
            return f'{self.name} -> {prop.place}'
        else:
            return f'{self.name} -> None'

    def join(self, fpr: ForeignProperty) -> ForeignProperty:
        """Join two ForeignProperty instances into one

        For example these two instances:

            ForeignProperty(City, country:Country -> None)
            ForeignProperty(Country, continent:Continent -> name:string)

        Would return:

            ForeignProperty(City, country:Country -> continent:Continent -> name:string)

        """
        tail = self
        head = fpr.chain[0]

        if tail.right is None:
            if tail.left.model.name != head.left.prop.model.name:
                raise IncompatibleForeignProperties(tail.left, right=head.left)
            tail = tail.push(head.left.prop)
        else:
            if tail.right != head.left:
                raise IncompatibleForeignProperties(tail.right, right=head.left)

        chain = list(tail.chain + head.chain[:-1])

        return ForeignProperty(
            None,
            fpr.left,
            fpr.right,
            _name=self / fpr.left.prop,
            _chain=chain,
        )

    def push(self, right: Optional[Property] = None) -> ForeignProperty:
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

    def swap(self, right: Optional[Property] = None) -> ForeignProperty:
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
        if self.right:
            expr = self.right.get_bind_expr()
        else:
            expr = None
        for fpr in reversed(self.chain[:-1]):
            if expr:
                expr = Expr('getattr', fpr.right.get_bind_expr(), expr)
            else:
                fpr.right.get_bind_expr()
        if expr:
            return Expr('getattr', self.chain[0].left.get_bind_expr(), expr)
        else:
            return self.chain[0].left.get_bind_expr()
