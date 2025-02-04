import dataclasses
from typing import List, Any, Dict, Union

from spinta.backends import Backend
from spinta.components import Page, Property
from spinta.core.ufuncs import Env, Expr
from spinta.types.datatype import DataType, Object
from spinta.utils.schema import NA


class QueryPage:
    page_: Page
    size: int
    select: list
    sort: list

    def __init__(self, size = None, select = None, sort = None, page = None):
        self.size = size
        self.select = [] if select is None else select
        self.sort = [] if sort is None else sort
        # By default, we set pagination to disabled
        # It gets updated if there is pagination query, which should set to most up-to-date page
        self.page_ = Page(enabled=False) if page is None else page


class QueryParams:
    prioritize_uri: bool = False
    lang_priority: List[str] = None
    expand: Expr = None
    default_expand: bool = False
    lang: List = None
    push: bool = False


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


class BaseQueryBuilder(Env):
    backend: Backend
    page: QueryPage
    expand: List[Property] = None
    query_params: QueryParams = None
    # `resolved` is used to map which prop.place properties are already
    # resolved, usually it maps to Selected, but different DataType's can return
    # different results.
    resolved: Dict[str, Selected]
    selected: Dict[str, Selected] = None

    def init_query_params(self, params: QueryParams):
        if params is None:
            params = QueryParams()
        self.query_params = params
        self._set_expanded_properties(params)

    # By default, when expand = None we think that nothing is expanded
    # in case we allow default_expand then we set it to empty list
    # if expand is empty list, we assume all are expanded
    # ?expand() will result in [] and ?expand(name) will result in [name]
    def _set_expanded_properties(self, params: QueryParams):
        prop_expr = params.expand
        if prop_expr is None and not params.default_expand:
            self.expand = None
            return

        self.expand = []
        if prop_expr is not None:
            self.expand = self.resolve(prop_expr)


class Star:
    def __str__(self):
        return '*'


class Func:
    pass


@dataclasses.dataclass
class ReservedProperty(Func):
    dtype: DataType
    param: str


@dataclasses.dataclass
class NestedProperty(Func):
    # Used to mark object nesting
    # for example object -> datatype

    left: Union[Object]
    right: Any


@dataclasses.dataclass
class ResultProperty(Func):
    # Used when result is calculated at ResultBuilder level
    # for example: checksum()
    expr: Expr


@dataclasses.dataclass
class LiteralProperty(Func):
    # Used when returning literal value
    value: Any


@dataclasses.dataclass
class Flip(Func):
    dtype: DataType
