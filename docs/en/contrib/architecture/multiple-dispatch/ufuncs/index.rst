.. default-role:: literal

######
Ufuncs
######

In C4 terms (see :doc:`/contrib/architecture/index`), ufuncs are one of the
cross-cutting :ref:`components <contrib-components>` (the expression
language layer) shared by the web app and the CLI containers.

Functions or user functions (`ufuncs` for short) are functions that are
exposed to users. User functions can be given in URLs, for example
`/my/Data?select(x, y)&sort(-z)`, in `prepare` statements of manifest files,
in `param` formulas or in other places.

User provided formulas are parsed into an abstract syntax tree (spyna) and
then interpreted using functions registered with the `ufunc.resolver` or
`ufunc.executor` decorators from `spinta/core/ufuncs.py`.

Classes involved here are described in more detail in dedicated concept
pages:

- :doc:`/contrib/architecture/multiple-dispatch/ufuncs/expr` — the expression tree (`Expr`).
- :doc:`/contrib/architecture/multiple-dispatch/ufuncs/env` — evaluation environments (`Env`).
- :doc:`/contrib/architecture/multiple-dispatch/ufuncs/ufunc` — resolver/executor functions and the
  `UFuncRegistry` registries.

How ufuncs are registered and loaded
************************************

Ufuncs live in plain Python modules and are collected from a module list
configured in `spinta/config.py:CONFIG['ufuncs']`. By default it contains
`spinta.ufuncs`, `spinta.backends.postgresql.ufuncs`,
`spinta.datasets.backends.sql.ufuncs` and
`spinta.datasets.backends.dataframe.ufuncs`.

At startup, `load(Context, Config)` in `spinta/types/config.py` does the
following:

#. `ufunc.resolver.collect(rc.get('ufuncs'))` imports every module in the
   list. Importing is enough — the decorators add collected functions to the
   registry as a side effect of the import.
#. `register_soap_ufuncs(...)` additionally registers SOAP adapter resolvers
   from external Python files listed in `soap_adapter_modules` (they are
   registered with `UFuncRegistry.register()`, not with decorators).
#. `config.resolvers = ufunc.resolver.ufuncs()` and
   `config.executors = ufunc.executor.ufuncs()` convert the collected
   `(name, func, types)` tuples into `Ufunc` dispatcher objects. `Ufunc`
   is a subclass of `spinta/dispatcher.py:Command`, so the same multiple
   dispatch mechanics as for commands (see :doc:`/contrib/architecture/multiple-dispatch/commands`) is used.
#. `Env.__init__` (see :doc:`/contrib/architecture/multiple-dispatch/ufuncs/env`) picks up these registries from
   `config.resolvers` / `config.executors`, unless explicit registries are
   given (this is used in tests, see `spinta/testing/ufuncs.py`).

`UFuncRegistry` (in `spinta/core/ufuncs.py`) is a small collection class
with three operations:

- `UFuncRegistry()` decorator: `@ufunc.resolver(Env, *types)` — registers
  the decorated function under its own name (or under `name=` / for several
  names at once with `names=`).
- `UFuncRegistry.collect(modules)` — imports the given modules, so that
  decorated functions register themselves.
- `UFuncRegistry.register(name, func, types)` — registers a single function
  programmatically (used by plugin-style loaders, e.g. SOAP adapters).
- `UFuncRegistry.ufuncs()` — builds and returns a `dict` of name →
  `Ufunc` dispatcher, ready to be used by `Env` instances.

Abstract syntax tree
********************

User provided formulas are parsed using the `spinta.spyna` module. For
example `select(x, y)` will be parsed into:

.. code-block:: python

   {
       'name': 'select',
       'args': [
           {'name': 'bind', 'args': ['x']},
           {'name': 'bind', 'args': ['y']},
       ]
   }

This is done by the `spinta.spyna.parse` function.

Then there is a `spinta.core.ufuncs.asttoexpr` function, responsible for
converting dicts and lists into `spinta.core.ufuncs.Expr` instances:

.. code-block:: python

   from spinta.core.ufuncs import Expr

   Expr(
       'select',
       Expr('bind', 'x'),
       Expr('bind', 'y'),
   )

`Expr` class adds some additional utilities to make working with AST easier.
For example:

- `str(Expr('bind', 'x'))` returns unparsed version, in this case it will be
  just `x`.

- `bind = Expr('bind'); bind('x')` — you can call `Expr` instances like
  normal functions, and these calls return another `Expr` instance with call
  arguments. Keyword arguments are supported too.

- `Expr('bind', 'x').resolve(env)` — this is a bit more complicated,
  `Expr.resolve` method will resolve all arguments of a given expression on
  a given environment and returns resolved arguments as an `args, kwargs`
  tuple.

  For example, in this case if `Expr('bind', 'x')` is resolved on a
  `PgQueryBuilder` environment, then `x` will be resolved into a table
  column.

There are two `Expr` subclasses, added by `asttoexpr` depending on the
`type` field of the spyna AST: `ShortExpr` (short form of an expression,
e.g. `a=2` instead of `eq(bind('a'), 2)`) and `MethodExpr` (method call
form, e.g. `a.b()` instead of `b(a)`). Both are described in
:doc:`/contrib/architecture/multiple-dispatch/ufuncs/expr`.

Environments
************

Environment is used as a formula evaluation context, where an initial state
is given and modified during abstract syntax tree evaluation.

Basically each AST node is interpreted on a given environment.

All environments are subclasses of the `spinta.core.ufuncs.Env` class. By
default the `Env` class has these properties:

- `this` — represents the current object.
- `context` — represents global Spinta context, where you can access various
  global instances.

All other properties are added by specific environments. Here are a few
specific environments:

- `spinta.ufuncs.querybuilder.components.QueryBuilder` — a generic query
  builder, extended by backend specific subclasses.
- `spinta.backends.postgresql.ufuncs.query.components.PgQueryBuilder` —
  builds a `sqlalchemy` query for the internal PostgreSQL backend.
- `spinta.datasets.backends.sql.ufuncs.query.components.SqlQueryBuilder` —
  builds a `sqlalchemy` query for external SQL databases. Backend dialect
  specific subclasses are configured per backend, e.g.
  `PostgreSQLQueryBuilder`, `MySQLQueryBuilder`, `MSSQLQueryBuilder`,
  `SqliteQueryBuilder`, etc.
- `spinta.ufuncs.resultbuilder.components.ResultBuilder` and its backend
  specific subclasses (`PgResultBuilder`, `SqlResultBuilder`) — transform
  results returned by a backend, this is used to perform additional, more
  complex transformations that are not supported natively by a backend.
- `spinta.ufuncs.loadbuilder.components.LoadBuilder` — resolves `prepare`
  formulas of manifest nodes at manifest load time.
- `spinta.dimensions.param.components.ParamBuilder` and `ParamLoader` —
  resolve `param` source and formula values.
- `spinta.ufuncs.propertyresolver.components.PropertyResolver` — resolves
  property names given in formulas into `Property` instances.

The full list of environments is configured in
`spinta/config.py:CONFIG['components']` groups `querybuilders` and
`resultbuilders`. Which concrete query builder is used for a given backend
is chosen by the backend itself: `Backend.query_builder_class` is set from
`CONFIG['components']['querybuilders'][backend.query_builder_type]`.

Usually environments are initialized and used like this (real example from
`spinta/backends/postgresql/commands/read.py`):

.. code-block:: python

    # Get query builder class for this backend.
    builder = backend.query_builder_class(context)

    # Set initial state.
    builder.update(model=model)

    # Create a new Env instance, initialized with backend specific state.
    env = builder.init(backend, table, params)

    # Resolve expression arguments on a given context, into Python objects.
    expr = env.resolve(query)

    # Execute final expression with resolved arguments.
    where = env.execute(expr)

    # Build the final sqlalchemy query from resolved and executed parts.
    qry = env.build(where)

All environments are initialized with an initial state, then the given AST
is resolved on a given context into Python objects and finally, the AST node
(or expression) is executed with resolved arguments.

For example if we have a formula like this: `print(2 + 2)`, then:

.. code-block:: python

   ast = Expr('print', Expr('add', 2, 2))
   env = Env(context)
   expr = env.resolve(ast)  # -> Expr('print', 4)
   env.execute(expr)

Here `env.resolve(ast)` resolves arguments by recursively calling
`env.resolve(ast)` on each AST argument.

If `ast` is not an `Expr`, then it is returned as is, but if `ast` is an
`Expr`, then it tries to find a resolver function registered with the
`@ufunc.resolver()` decorator, calls that function with resolved `ast`
arguments and returns its result.

A similar thing can be done manually by calling `env.call('add', 2, 2)`, the
`call` method finds a resolver function named `add` and calls it with
arguments `2` and `2`, without resolving arguments before the call.

See :doc:`/contrib/architecture/multiple-dispatch/ufuncs/env` for the full list of `Env` methods.

Resolver functions
******************

Resolver functions are called by `Env.resolve` and `Env.call` methods.
Resolver functions are responsible for interpreting given arguments on a
given context.

There are two ways, how a resolver function can be registered:

1. Register a function that is responsible for resolving arguments manually:

   .. code-block:: python

     from spinta.core.ufuncs import ufunc

     @ufunc.resolver(Env, Expr)
     def add(env: Env, expr: Expr):
         args, kwargs = expr.resolve(env)
         return sum(args)

   This is mainly used, when you need to take multiple arguments and do
   something with them. Like in this example, the function can be called
   like this: `add(2, 2, 2, 2)`.

   If such a function is registered, then it will always be called, ignoring
   all other functions with the same name and different argument types.
   If you need to call another function dispatched by argument types, then
   you need to do it manually, because automatic dispatch by argument types
   will be turned off (see `autoargs` below).

2. Register a function that is dispatched by arguments automatically
   resolved before the function is called:

   .. code-block:: python

     from spinta.core.ufuncs import ufunc

     @ufunc.resolver(Env, int, int)
     def add(env: Env, a: int, b: int):
         return a + b

     @ufunc.resolver(Env, str, str)
     def add(env: Env, a: str, b: str):
         return int(a) + int(b)

   In this case, this function will only be called, if the function is
   called with two arguments and both arguments are of the `int` type.

   This way, you can define multiple `add` functions, for different argument
   types.

Resolver functions, or ufuncs, are always dispatched by argument types, that
means, the same function can do different things on different environments.

So common functionality should be implemented on top of `Env`, but
environment specific functionality should be implemented on a specific `Env`
subclass.

Autoargs and the `Env, Expr` case
---------------------------------

The `Ufunc` dispatcher (unlike regular commands) has `autoargs = True` by
default. When `Env.resolve` calls a resolver with `autoargs` enabled, it
first resolves all expression arguments recursively and calls the function
with the resolved values.

If a function is registered with `(Env, Expr)` as its types, then `autoargs`
is turned off for that name. In this case the function receives the whole
unresolved `Expr` and must resolve its arguments manually with
`expr.resolve(env)` or `env.resolve(...)`. This is how variadic functions
like `select` or `sort` are implemented, see
`spinta/ufuncs/querybuilder/ufuncs.py`.

If no resolver is registered for a given name and argument types, the
`default_resolver` is called — by default it just returns the expression
with resolved arguments attached (`expr(*args, **kwargs)`), so that the
result can be further processed by executors. Concrete environments can
override `default_resolver` (e.g. `SqlQueryBuilder` raises `UnknownMethod`
there, because all its resolvers must be explicit).

Executor functions
******************

Executor functions are registered with `@ufunc.executor` and are called by
`Env.execute`. Executors implement the final evaluation step: they are
called with already resolved arguments and return the final result. This
two-phase design lets resolvers turn property names and literals into
intermediate objects first, and executors (where needed) turn them into the
final result — for example comparison operators are resolved to
`Func` values by `QueryBuilder` resolvers and executed to final `sqlalchemy`
expressions later.

Executors are used sparsely — most environments implement everything with
resolvers and `default_resolver`. A typical real example is
`spinta/dimensions/param/ufuncs.py`, where `@ufunc.executor(ParamBuilder,
NotAvailable)` defines how an expression is evaluated when some arguments
are not available.

Resolver arguments: `Bind`, `GetAttr` and friends
*************************************************

To make resolvers composable, arguments are not resolved directly into
final values. Instead, resolvers return intermediate, backend independent
types (defined in `spinta/core/ufuncs.py` and
`spinta/ufuncs/components.py`):

- `Bind` — a property name binding, e.g. the `x` in `select(x)`.
- `GetAttr` — an attribute access chain, e.g. `a.b.c` (used for accessing
  nested or foreign properties).
- `Negative` / `Positive` — bindings with sign, e.g. sort directions in
  `sort(-z)`.
- `Pair` — a named argument, e.g. `lang: "lt"`.
- `ForeignProperty` — a property of a joined (foreign) model.
- `ReservedProperty` — reserved property access, e.g. properties of `Ref`
  or `File` data types that don't map directly to a database column.

The general resolver structure in `spinta/ufuncs/querybuilder/ufuncs.py`
(which is shared by backends) is built around these types: `_resolve_getattr`
dispatches `Bind`/`GetAttr` chains into `ForeignProperty`, `ReservedProperty`
or `LiteralProperty` values, and then per-type resolvers like
`select(env, ForeignProperty, DataType)` do the backend specific work.

.. _contrib-functions-concepts:

Concepts
********

- :doc:`/contrib/architecture/multiple-dispatch/ufuncs/expr` — `Expr`, `ShortExpr`, `MethodExpr` and related
  helper types.
- :doc:`/contrib/architecture/multiple-dispatch/ufuncs/env` — `Env` and its concrete builder subclasses.
- :doc:`/contrib/architecture/multiple-dispatch/ufuncs/ufunc` — `Ufunc`, `UFuncRegistry`, `ufunc.resolver` /
  `ufunc.executor`.

.. toctree::
   :maxdepth: 1

   Expr <expr>


See also
********

- :doc:`/contrib/architecture/multiple-dispatch/commands` — the command
  dispatcher sharing the same multiple dispatch machinery.
- :doc:`/contrib/architecture/multiple-dispatch/ufuncs/ufunc` — the
  `Ufunc` dispatcher and `UFuncRegistry` in detail.
- :doc:`/contrib/architecture/multiple-dispatch/ufuncs/env` — `Env` and
  its concrete builder subclasses in detail.
