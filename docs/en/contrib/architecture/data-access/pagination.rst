.. default-role:: literal

****
Page
****

Pagination in Spinta is implemented with a set of helper classes in
`spinta/components.py`: `PageInfo`, `PageBy`, `Page` and `ParamsPage`.
They implement keyset (cursor) pagination — pages are bounded by the
last seen sort-key values, not by numeric offsets, which is why the
page key is a base64-encoded list of sort-key values (see
:doc:`/contrib/architecture/manifests/logical/models` for where keys come from).

- `PageInfo` — static per-model pagination configuration, stored as
  `Model.page`. Fields: `model`, `enabled` (whether pagination is
  enabled for this model at all), `size` (default page size) and
  `keys` — dict of pagination key properties by sort-key name
  (`{"-_id": ..., "value": ...}`; a leading `-` means descending
  order).
- `PageBy` — one pagination key binding: `prop` (the `Property`, see
  :doc:`/contrib/architecture/manifests/logical/properties`) and `value` (the last seen value of that property).
- `Page` — the mutable pagination state of one request: `by` — dict of
  `PageBy` by key name, `size`, `enabled`, `filter_only` (keys are used
  only for filtering, not exposed in `_page`), `first_time` (internal
  flag for the first page), plus `model`. Methods: `add_prop()`,
  `update_value()`, `clear()`, `update_values_from_page_key()` (from a
  decoded `_page` value), `update_values_from_list()` (from the
  request's page values) and `get_repr_for_error()`.
- `ParamsPage` — the `page(...)` operator parsed from the request URL,
  stored as `UrlParams.page`: `values` (decoded page key values),
  `size`, `is_enabled`.

How pagination is configured
****************************

The `PageInfo` is built at model load time by
`spinta/ufuncs/loadbuilder/components.py:LoadBuilder.load_page()`:

- starts with `PageInfo(model, enabled=config.enable_pagination)`
  (the `enable_pagination` config option, default `True`);
- if the manifest's `prepare` formula for the external source resolves
  to a `PageInfo` (the `page()` ufunc, see below), it is used;
- otherwise the keys default to `_id`, or, for external models, to the
  given primary keys (`model.given.pkeys` from the `pk` column);
- if any key property is of a type that cannot be compared for
  pagination (`get_allowed_page_property_types()` in
  `spinta/ufuncs/loadbuilder/helpers.py` allows `Integer`, `Number`,
  `String`, `Date`, `DateTime`, `Time`, `PrimaryKey`, `UUID`), the
  page is disabled;
- `config.default_page_size` (default `100000`) is used when no size
  is given.

In manifests pagination keys can also be given with the `page()`
formula: `page(key, size)` — implemented as a `LoadBuilder` resolver
ufunc in `spinta/ufuncs/loadbuilder/ufuncs.py` (`page_`, `page_item`),
which accepts property binds (`page(code)`) and `size(size)`/`disable`
arguments; `page()` with no arguments disables pagination.

At model link time `_link_model_page()` (`spinta/types/model.py`)
disables pagination if the backend does not support the
BackendFeatures.PAGINATION` feature (see :doc:`/contrib/architecture/data-access/backends/index`),
removes unsupported `_id` keys for external backends, and makes sure
`_id` is always among the keys for internal backends.

How a request uses pagination
*****************************

#. URL parsing: the ``page(...)`` path operator is parsed into
   `ParamsPage` in `spinta/urlparams.py` (`_prepare_urlparams_from_path`),
   with base64-decoded page values (``decode_page_values()`` — JSON
   array encoded with URL-safe base64, see `spinta/utils/encoding.py`)
   and optional ``size`` and ``disable`` arguments.
#. `create_page(PageInfo, UrlParams)` (`spinta/types/model.py`)
   builds the request's `Page`: it decides enabledness with
   `pagination_enabled()` (backend must support pagination; an
   explicit ``disable`` in the request wins), copies sort values from
   ``?_sort`` (only properties of allowed types), and fills key values
   from the given page key. A `Page` without URL params is also
   created by `create_page(PageInfo)` (e.g. in tests and CLI helpers).
#. `getall(Context, Model, Request)` (`spinta/commands/read.py`) calls
   `getall(Context, Model, Page)` when pagination is enabled: page size
   is taken from `get_page_size(config, model, page)` (request size, or
   model size, or `config.default_page_size`) plus one extra row to
   detect whether a next page exists, the query is wrapped with the
   `paginate` ufunc expression (`add_page_expr()` in
   `spinta/ufuncs/querybuilder/helpers.py`) and results are yielded
   through `get_paginated_values()`, which guards against short page
   sizes (`TooShortPageSize`), key repetitions and infinite loops
   (`InfiniteLoopWithPagination`) and advances the key values.
#. Backends produce a `_page` key value per row (e.g.
   `spinta/backends/postgresql/commands/read.py` sets `res["_page"] =
   get_page_values(env, row)`) — the last row's value is encoded into
   the page key returned to the client.
#. The response renderer (`spinta/formats/json/components.py:Json`)
   strips `_page` from rows and emits ``{"_data": [...], "_page":
   {"next": "<encoded key>"}}``; other formats rename the column to
   `_page.next` (`spinta/formats/helpers.py:rename_page_col`).

The `page` property type is `PageType` (`spinta/types/datatype.py`) on
the reserved `_page` property (see :doc:`/contrib/architecture/manifests/logical/properties`).

The `limit`/`offset` request parameters (``?limit(n[, offset])``) are
separate from pagination: `_enforce_limit()`
(`spinta/utils/response.py`) caps non-streamable formats at 100 rows
and marks `params.limit_enforced` — clients then get the enforced limit
in the response metadata.

External specifications
***********************

- UAPI (https://github.com/atviriduomenys/uapi, local extract
  `.tmp/sources/uapi/UAPI.md`, "Page" section): `_page` in combination
  with `_limit` and `_sort`; `_page` is a base64-encoded (URL-safe,
  `+/=` replaced with `-_.`) JSON value of the last sort key; the
  response carries `_next` (UAPI draft) or `_page.next` (current
  Spinta, verify in `spinta/formats/json/components.py`); pagination
  may be enabled by default by the agent, which is indicated in the
  response. Current Spinta implements this scheme with the classes
  above (see `spinta/commands/read.py` and
  `spinta/ufuncs/querybuilder/ufuncs.py:paginate`).
- DSA (https://dsa.readthedocs.io/, local extract `.tmp/sources/dsa/
  DSA.md`) has no pagination dimension — pagination is Spinta's API
  concern, not a manifest concern.

See also
********

- :doc:`/contrib/architecture/manifests/logical/models` — `Model.page` (`PageInfo`) and where keys come from.
- :doc:`/contrib/architecture/manifests/logical/properties` — the `_page` reserved property and pagination key
  properties.
- `spinta/commands/read.py` — the request-time pagination loop.
