from dataclasses import dataclass
from typing import Any, TypedDict

import sqlalchemy as sa
from sqlalchemy.engine.base import Engine as SaEngine
from sqlalchemy.engine.row import RowProxy

from spinta.components import Property
from spinta.core.ufuncs import Env
from spinta.utils.data import take
from spinta.utils.schema import NA


@dataclass
class Engine:
    dsn: str  # sqlalchemy engine dsn
    schema: str = None
    encoding: str = NA

    def create(self) -> SaEngine:
        return sa.create_engine(self.dsn, **take({
            'encoding': self.encoding,
        }))


class SqlResource(Env):
    dsn: str

    def init(self, dsn: str):
        return self(dsn=dsn)


class SqlResultBuilder(Env):
    this: Any           # A selected value.
    prop: Property      # Model property.
    data: RowProxy      # Whole row from database.

    def init(self, this: Any, prop: Property, data: RowProxy):
        return self(
            this=this,
            prop=prop,
            data=data,
        )


class Selected:
    # Item index in select list.
    item: int = None
    # Model property if a property is selected.
    prop: Property = None
    # A value or an Expr for further processing on selected value.
    prep: Any = NA

    def __init__(
        self,
        item: int = None,
        prop: Property = None,
        # `prop` can be Expr or any other value.
        prep: Any = NA,
    ):
        self.item = item
        self.prop = prop
        self.prep = prep

    def __repr__(self):
        return self.debug()

    def __eq__(self, other):
        if isinstance(other, Selected):
            return self.item == other.item and self.prop == other.prop and self.prep == other.prep
        return False

    def debug(self, indent: str = ''):
        prop = self.prop.place if self.prop else 'None'
        if isinstance(self.prep, Selected):
            return (
                f'{indent}Selected('
                f'item={self.item}, '
                f'prop={prop}, '
                f'prep=...)\n'
                   ) + self.prep.debug(indent + '  ')
        elif isinstance(self.prep, (tuple, list)):
            return (
                f'{indent}Selected('
                f'item={self.item}, '
                f'prop={prop}, '
                f'prep={type(self.prep).__name__}...)\n'
            ) + ''.join([
                p.debug(indent + '- ')
                if isinstance(p, Selected)
                else str(p)
                for p in self.prep
            ])
        else:
            return (
                f'{indent}Selected('
                f'item={self.item}, '
                f'prop={prop}, '
                f'prep={self.prep})\n'
            )


class FileSelected(TypedDict):
    name: Selected      # File name
    content: Selected   # File content
