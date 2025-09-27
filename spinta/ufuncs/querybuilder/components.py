from __future__ import annotations

import dataclasses
from typing import List, Any, Dict, Union

from spinta.backends import Backend
from spinta.components import Page, Property, Model
from spinta.core.ufuncs import Env, Expr
from spinta.types.datatype import DataType, Object
from spinta.ufuncs.components import ForeignProperty
from spinta.ufuncs.propertyresolver.components import PropertyResolver
from spinta.utils.schema import NA


class QueryPage:
    page_: Page
    size: int
    select: list
    sort: list

    def __init__(self, size=None, select=None, sort=None, page=None):
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
    url_params: dict

    def __init__(self, url_params: dict | None = None) -> None:
        self.url_params = url_params or {}


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

    def debug(self, indent: str = ""):
        prop = self.prop.place if self.prop else "None"
        if isinstance(self.prep, Selected):
            return (f"{indent}Selected(item={self.item}, prop={prop}, prep=...)\n") + self.prep.debug(indent + "  ")
        elif isinstance(self.prep, (tuple, list)):
            return (f"{indent}Selected(item={self.item}, prop={prop}, prep={type(self.prep).__name__}...)\n") + "".join(
                [p.debug(indent + "- ") if isinstance(p, Selected) else str(p) for p in self.prep]
            )
        else:
            return f"{indent}Selected(item={self.item}, prop={prop}, prep={self.prep})\n"


class Star:
    def __str__(self):
        return "*"


class QueryBuilder(Env):
    backend: Backend
    model: Model
    page: QueryPage
    expand: List[Property] | Star | None = None
    query_params: QueryParams = None
    # `resolved` is used to map which prop.place properties are already
    # resolved, usually it maps to Selected, but different DataType's can return
    # different results.
    resolved: Dict[str, Selected]
    selected: Dict[str, Selected] = None

    property_resolver: PropertyResolver = None

    def init_query_params(self, params: QueryParams | None):
        if params is None:
            params = QueryParams()
        self.query_params = params
        self._set_expanded_properties(params)

    def _set_expanded_properties(self, params: QueryParams):
        # `None`, nothing is expanded
        # `Star()`, everything is expanded
        # `[...]`, list of expanded properties
        # ?expand() results in `Star()`, `?expand("name")` results in `["name"]`

        prop_expr = params.expand
        if prop_expr is None and not params.default_expand:
            self.expand = None
            return

        self.expand = Star()
        if prop_expr:
            self.expand = self.resolve(prop_expr)

    def resolve_property(self, *args, **kwargs) -> Property:
        if self.property_resolver is None:
            resolver = PropertyResolver(self.context)
            resolver = resolver.init(model=self.model, ufunc_types=True)
            self.property_resolver = resolver
        result = self.property_resolver.resolve_property(*args, **kwargs)
        return result


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
    prop: Property | Func | ForeignProperty
    # If multiple flips are called, we can ignore it if count is even
    count: int = 1

    @property
    def required(self) -> bool:
        return self.count % 2 != 0
