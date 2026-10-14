:orphan:

.. default-role:: literal

*****
Env
*****

`spinta/core/ufuncs.py:Env` is an evaluation context of user formulas
(ufuncs, see :doc:`/contrib/architecture/multiple-dispatch/ufuncs/index` and :doc:`ufunc`). An environment holds the
state, on which an abstract syntax tree (`Expr`, see :doc:`expr`) is
interpreted, and carries references to the resolver and executor registries
used for interpretation.

Core properties:

- `this` — the current object the formula is evaluated against (e.g. a
  `Property` value being prepared).
- `context` — the global Spinta `Context`, giving access to configuration,
  manifest, backends, etc.

The rest of the state is kept in a scope dict. `Env` provides:

- `Env(context)` — initialization; resolver/executor registries default to
  `config.resolvers` and `config.executors` (populated by
  `load(Context, Config)`, see `spinta/types/config.py`).
- `env(**scope)` and `env.update(**scope)` — add state. Calling an
  environment returns a new instance of the same class with an extended
  scope; `update` mutates the current scope.
- `env.property_name` — scope entries are exposed as attributes via
  `__getattr__`.
- `env.resolve(expr)` — resolves an `Expr` into Python objects using
  registered resolvers (see :doc:`ufunc`). Non-`Expr` values are returned
  as is. If no resolver matches, `default_resolver` is called.
- `env.call(name, *args, **kwargs)` — calls a resolver by name, without
  resolving arguments first.
- `env.execute(expr)` — the second evaluation phase: executes an expression
  using registered executors. Unknown names raise `UnknownMethod`.
- `env.default_resolver(expr, *args, **kwargs)` — fallback when no resolver
  matches; by default returns the expression with resolved arguments
  attached.

Env subclasses
**************

All concrete environments are `Env` subclasses, usually adding state and
resolvers on top:

- `spinta.ufuncs.querybuilder.components.QueryBuilder` — generic query
  builder (`select`, `sort`, `limit`, pagination, etc.), extended by backend
  specific subclasses.
- `spinta.backends.postgresql.ufuncs.query.components.PgQueryBuilder` —
  builds `sqlalchemy` queries for the internal PostgreSQL backend.
- `spinta.datasets.backends.sql.ufuncs.query.components.SqlQueryBuilder` —
  builds `sqlalchemy` queries for external SQL sources, with dialect
  specific subclasses (`PostgreSQLQueryBuilder`, `MySQLQueryBuilder`,
  `MSSQLQueryBuilder`, `SqliteQueryBuilder`, `OracleQueryBuilder`,
  `MariaDBQueryBuilder`, `SASQueryBuilder`, ...).
- `spinta.ufuncs.resultbuilder.components.ResultBuilder` and backend
  specific subclasses (`PgResultBuilder`, `SqlResultBuilder`) — transform
  rows returned by backends.
- `spinta.ufuncs.loadbuilder.components.LoadBuilder` — resolves `prepare`
  formulas at manifest load time.
- `spinta.dimensions.param.components.ParamBuilder` and `ParamLoader` —
  resolve `param` formulas and sources.
- `spinta.ufuncs.propertyresolver.components.PropertyResolver` — resolves
  property names into `Property` instances.
- `spinta.datasets.backends.dataframe.backends.soap.ufuncs.components.
  SoapQueryBuilder` — query builder for the SOAP adapter.

Which query builder is used for a given backend is configured in
`spinta/config.py:CONFIG['components']['querybuilders']` (and
`resultbuilders`) and picked via `Backend.query_builder_class`.

Typical usage
*************

Real example from `spinta/backends/postgresql/commands/read.py`:

.. code-block:: python

    builder = backend.query_builder_class(context)
    builder.update(model=model)
    env = builder.init(backend, table, params)
    expr = env.resolve(query)
    where = env.execute(expr)
    qry = env.build(where)

External specifications
***********************

- The DSA tabular manifest specification defines the formula language that
  environments evaluate: https://dsa.readthedocs.io/ ("Formulas" section;
  local extract `.tmp/sources/dsa/DSA.md`). An environment in Spinta
  corresponds to the evaluation context of a DSA formula — the current row
  (`self`), property bindings, parameters, etc.
- In UAPI, environments are created per request, to turn query string
  expressions into backend queries: https://github.com/atviriduomenys/uapi
  (local extract: `.tmp/sources/uapi/UAPI.md`).

See also
********

- :doc:`/contrib/architecture/multiple-dispatch/ufuncs/index` — the ufuncs explanation page.
- :doc:`expr` — the expression tree.
- :doc:`ufunc` — resolvers and executors.
