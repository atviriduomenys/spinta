from __future__ import annotations

import contextlib
import warnings

from typing import TYPE_CHECKING

from spinta.core.ufuncs import asttoexpr
from spinta.datasets.backends.dataframe.backends.csv.components import Csv
from spinta.datasets.backends.dataframe.backends.json.components import Json
from spinta.datasets.backends.dataframe.backends.xml.components import Xml

if TYPE_CHECKING:
    from spinta.components import UrlParams
    from spinta.core.ufuncs import Expr


def urlparams_to_expr(
    params: UrlParams,
) -> Expr:
    """Convert UrlParams to ufunc Expr.

    This is compatibility function. Currently all query handling is done using
    UrlParams, but in future UrlParams should be replaced by ufunc Expr. This
    way getall and getone commands will receive just one Expr parameter
    containing all the query information.
    """

    ast = []

    if params.query:
        ast += params.query

    if params.select:
        ast.append(
            {
                "name": "select",
                "args": [arg if isinstance(arg, dict) else {"name": "bind", "args": [arg]} for arg in params.select],
            }
        )

    if params.sort:
        ast.append(
            {
                "name": "sort",
                "args": [arg if isinstance(arg, dict) else {"name": "bind", "args": [arg]} for arg in params.sort],
            }
        )

    if params.limit:
        ast.append({"name": "limit", "args": [params.limit]})

    if params.offset:
        ast.append({"name": "offset", "args": [params.offset]})

    if params.bbox is not None:
        ast.append({"name": "bbox", "args": params.bbox})

    if len(ast) == 0:
        ast = {
            "name": "select",
            "args": [],
        }
    elif len(ast) == 1:
        ast = ast[0]
    else:
        ast = {"name": "and", "args": ast}

    return asttoexpr(ast)


# Backwards compatibility `xml` backend class.
# It will eventually be deprecated fully.
class XmlDeprecated(Xml):
    type: str = "xml"

    @contextlib.contextmanager
    def begin(self):
        yield

    def __init__(self):
        super().__init__()
        warnings.warn("'xml' backend type is deprecated, use 'dask/xml'.", FutureWarning)


# Backwards compatibility `json` backend class.
# It will eventually be deprecated fully.
class JsonDeprecated(Json):
    type: str = "json"

    @contextlib.contextmanager
    def begin(self):
        yield

    def __init__(self):
        super().__init__()
        warnings.warn("'json' backend type is deprecated, use 'dask/json'.", FutureWarning)


# Backwards compatibility `csv` backend class.
# It will eventually be deprecated fully.
class CsvDeprecated(Csv):
    type: str = "csv"

    @contextlib.contextmanager
    def begin(self):
        yield

    def __init__(self):
        super().__init__()
        warnings.warn("'csv' backend type is deprecated, use 'dask/csv'.", FutureWarning)
