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

- `this` — the node whose `prepare` formula is being resolved (see
  :ref:`contrib-ufuncs-this` below).
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
- `spinta.datasets.backends.dataframe.ufuncs.query.components.DaskDataFrameQueryBuilder` —
  builds queries for dataframe/file-based datasets (CSV, JSON, etc.).
- `spinta.ufuncs.resultbuilder.components.ResultBuilder` and its backend
  specific subclasses (`PgResultBuilder`, `SqlResultBuilder`) — transform
  results returned by a backend, this is used to perform additional, more
  complex transformations that are not supported natively by a backend.
- `spinta.ufuncs.loadbuilder.components.LoadBuilder` — resolves `prepare`
  formulas of manifest nodes at manifest load time.
- `spinta.ufuncs.linkbuilder.components.LinkBuilder` — links cross-node
  references after manifest nodes are loaded.
- `spinta.ufuncs.requestparamsbuilder.components.RequestParamsBuilder` —
  interprets incoming request URL parameters.
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

.. _contrib-ufuncs-this:

The `this` value
----------------

`this` is the node of which a `prepare` formula is being resolved. It is
set when the environment is created or extended, usually with `env(this=...)`
or `env.update(this=...)`. For example, when the `prepare` formula of a
resource is resolved:

.. code-block:: python

   builder = LinkBuilder(context, resource, resource.dataset)
   builder(this=resource).resolve(resource.prepare)

Here `this` is set to `resource`, because the `prepare` of `resource` is being
resolved.

Given this manifest:

.. code-block:: text

   d | r | b | m | property     | type    | ref     | source     | prepare | level | access
   example                      |         |         |            |         |       |
     | resource1                | sql     |         |            | f1()    |       |
     |   |   | City             |         | id      | COUNTRIES  | f2()    |       |
     |   |   |   | id           | integer |         | ID         | f3()    |       |

each formula receives `this` as follows:

- `f1()` — `env(this=Resource(resource1))`
- `f2()` — `env(this=Model(City))`
- `f3()` — `env(this=Property(id))`

When chained methods are used, `this` for each method is the result of the
previous method. For example, with this manifest:

.. code-block:: text

   d | r | b | m | property     | type    | ref     | source     | prepare     | level | access
   example                      |         |         |            |             |       |
     | resource1                | sql     |         |            | a().b().c() |       |
     |   |   | City             |         | id      | COUNTRIES  |             |       |
     |   |   |   | id           | integer |         | ID         |             |       |

`this` is set as follows:

- `a()` — `env(this=Resource(resource1))`, `a()` returns `SqlBackend()`
- `b()` — `env(this=SqlBackend())`, `b()` returns `sa.Engine()`
- `c()` — `env(this=sa.Engine())`

`Env` does not change `this` on its own while resolving a chain. Chained
calls are resolved on the same environment, so a resolver that needs the
result of the previous call as `this` must pass it on explicitly, for
example with `env(this=previous_result)`, as done in
`spinta/ufuncs/querybuilder/ufuncs.py`.

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
   will be turned off (see :ref:`multiple-dispatch-and-autoargs` below).

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

If no resolver is registered for a given name and argument types, the
`default_resolver` is called — by default it just returns the expression
with resolved arguments attached (`expr(*args, **kwargs)`), so that the
result can be further processed by executors. Concrete environments can
override `default_resolver` (e.g. `SqlQueryBuilder` raises `UnknownMethod`
there, because all its resolvers must be explicit).

`Expr` resolution
*****************

Formulas (`Expr`) must **never** be evaluated manually by inspecting ASTs or
writing custom tree-walkers in backend commands. All formula resolution goes
through `ufunc` resolvers on dedicated `*Builder` (`Env`) classes.

Formulas must be resolved **as early as possible** in the lifecycle. If an
expression can be evaluated at load time (e.g. `2 + 2`), store the result, not
the `Expr`. If it depends on later information (user input, data source
values), leave the `Expr` unresolved and pass it to the next builder.

Resolution order:

1. **`LoadBuilder`** — earliest stage, during `commands.load`.

   - Used for metadata preparation that needs no external context.
   - Resource parameter formulas are resolved here:

     - `header()` -> populates `resource.http.headers[name]`
     - `query()` -> populates `resource.url.query[name]`

   - Consumed formulas return `None` so they are not retained in
     `param.formulas`.
   - Values requiring late resolution (e.g. `creds()`) remain `Expr` inside the
     dict.

2. **`LinkBuilder`** — runs after all manifest nodes are loaded and can reference
   each other.

   - Used to link cross-node references.
   - Property `prepare = param()` -> links the source-less property to its
     resource param, setting `Property.param`. See
     :ref:`external-data-source-params` for how the link is used.

3. **`RequestParamsBuilder`** — runs when an HTTP request arrives, before
   touching data sources.

   - Interprets incoming request URL parameters (see
     :ref:`incoming-and-outgoing-requests`).

4. **`QueryBuilder`** (backend-specific, e.g. `DaskDataFrameQueryBuilder`,
   `SqlQueryBuilder`)

   - Translates unresolved `Expr` filters/prepares into external data source
     queries (for outgoing requests).
   - Accesses pre-resolved metadata directly (`resource.http.headers`,
     `resource.url.query`, `prop.param`) instead of traversing
     `resource.params` or re-evaluating formulas.

5. **`ResultBuilder`** — runs after data is returned from the data source to
   post-process rows.

.. _multiple-dispatch-and-autoargs:

Multiple dispatch and `autoargs`
--------------------------------

`ufunc` resolvers are built on `multipledispatch`. Dispatch is based on the
runtime types and arity (number of positional arguments):

- **Fixed-arity dispatch (`autoargs = True`, the default)**:
  By default, `autoargs = True` on every `Ufunc`. When evaluating
  `Expr(name, *args, **kwargs)`, `Env.resolve()` recursively resolves all
  arguments first (`args, kwargs = expr.resolve(self)`), and then calls
  `ufunc(self, *args, **kwargs)`.

  The dispatcher receives already-resolved Python objects and matches their
  runtime types against registered signatures (e.g. `str`, `int`, `Bind`).

  Because `multipledispatch` registers exact signatures from
  `@ufunc.resolver(EnvClass, ArgType1, ...)`, Python's `*args` does **not**
  create a catch-all or optional-argument signature in the dispatcher. For
  functions with a known, fixed set of arities or alternative argument types,
  define **separate resolver methods** for each signature:

  .. code-block:: python

     @ufunc.resolver(LinkBuilder)
     def param(env: LinkBuilder) -> None:
         ...  # param() with 0 formula arguments (uses prop.name)

     @ufunc.resolver(LinkBuilder, (str, Bind))
     def param(env: LinkBuilder, name: str | Bind) -> None:
         ...  # param('name') or param(name) with 1 argument

  - Tuples of types like `(str, Bind)` can be used in the decorator to accept
    multiple alternative types for an argument.
  - Omit `name="foo"` from `@ufunc.resolver` if the Python function name
    already matches the resolver name. Use `name=` only for reserved Python
    keywords (e.g. `and_`, `or_`) or explicit aliases.

- **Handling variadic arguments by disabling `autoargs` (`autoargs = False`)**:
  When a ufunc must accept an arbitrary or variable number of arguments
  (`*args` and/or `**kwargs`) — such as logical `and(...)` and `or(...)`
  ufuncs, or container functions like `list(...)` — defining separate
  fixed-arity signatures for every possible argument count is impractical.

  In these cases, disable `autoargs` by registering the resolver with `Expr`
  as its argument type:

  .. code-block:: python

     @ufunc.resolver(EnvClass, Expr, name="...")
     def func(env: EnvClass, expr: Expr):
         ...

  When **any** resolver for a given ufunc name registers `(EnvClass, Expr)`:

  - Automatic argument resolution is disabled globally for that ufunc name
    across all environments (`dispatcher.autoargs = False`).
  - `Env.resolve()` bypasses automatic argument resolution and invokes
    `ufunc(self, expr)` directly, passing the raw unevaluated `Expr`.
  - The resolver receives the unevaluated `Expr` and must resolve arguments
    manually via `args, kwargs = expr.resolve(env)`.
  - The resolver can then iterate over or manipulate the variadic `args` list
    dynamically (e.g. filtering out `None` values, combining multiple SQL
    conditions, or delegating).

  **Example 1: Variadic resolvers in `LoadBuilder`**

  In `spinta/ufuncs/loadbuilder/ufuncs.py`, `and` and `or` expressions can take
  any number of arguments. The resolvers accept `expr: Expr`, resolve the
  arguments manually with `expr.resolve(env)`, filter out `None` values, and
  return the list of active conditions:

  .. code-block:: python

     from spinta.core.ufuncs import Expr, ufunc
     from spinta.ufuncs.loadbuilder.components import LoadBuilder


     @ufunc.resolver(LoadBuilder, Expr, name="and")
     def and_(env: LoadBuilder, expr: Expr):
         args, kwargs = expr.resolve(env)
         args = [a for a in args if a is not None]
         return args


     @ufunc.resolver(LoadBuilder, Expr, name="or")
     def or_(env: LoadBuilder, expr: Expr):
         args, kwargs = expr.resolve(env)
         args = [a for a in args if a is not None]
         return args

  **Example 2: Variadic resolvers in `SqlQueryBuilder`**

  In `spinta/datasets/backends/sql/ufuncs/query/ufuncs.py`, backend query builders
  translate variable numbers of filter conditions into SQLAlchemy clauses. The
  resolvers manually resolve arguments and combine them using `sa.and_(*args)`
  or `sa.or_(*args)`:

  .. code-block:: python

     import sqlalchemy as sa
     from spinta.core.ufuncs import Expr, ufunc
     from spinta.datasets.backends.sql.components import SqlQueryBuilder


     @ufunc.resolver(SqlQueryBuilder, Expr, name="and")
     def and_(env: SqlQueryBuilder, expr: Expr):
         args, kwargs = expr.resolve(env)
         args = [env.call("_resolve_unresolved", arg) for arg in args if arg is not None]
         if len(args) > 1:
             return sa.and_(*args)
         elif args:
             return args[0]


     @ufunc.resolver(SqlQueryBuilder, Expr, name="or")
     def or_(env: SqlQueryBuilder, expr: Expr):
         args, kwargs = expr.resolve(env)
         args = [env.call("_resolve_unresolved", arg) for arg in args if arg is not None]
         if len(args) > 1:
             return sa.or_(*args)
         elif args:
             return args[0]

- **When to use `autoargs = True` vs `(EnvClass, Expr)` (`autoargs = False`)**:

  - Use standard typed signatures (`autoargs = True`, the default) whenever the
    ufunc has fixed or predictable arities (e.g. 0-arg, 1-arg, or 2-arg
    functions like `param()`, binary comparison operators, or type casts). This
    allows `multipledispatch` to automatically route calls based on runtime
    argument types without boilerplate resolution code.
  - Use `(EnvClass, Expr)` (`autoargs = False`) mostly when implementing
    **variadic ufuncs** accepting arbitrary arguments (`*args` and/or
    `**kwargs`), such as `and`, `or`, or `list`, or when an environment must
    explicitly control or defer argument evaluation.

Rules for adding resolvers
--------------------------

- **Never** evaluate `Expr` with custom `if/elif` loops over `expr.name` or
  `expr.args`. Define a resolver with `@ufunc.resolver(BuilderClass, ...)`.
- **Beware of `autoargs`**: if *any* resolver for a name registers
  `(EnvSubclass, Expr)` as its signature, `autoargs` is disabled globally for
  that name across all environments. If you want arguments resolved
  automatically by child resolvers, do not use `Expr` in the resolver
  signature.
- **Do not use `*args` or `**kwargs`** in resolver signatures expecting
  `multipledispatch` to match variable argument counts. For fixed arities, write
  distinct resolver functions for each arity and type signature. For variadic
  ufuncs (like `and` or `or`), register `(EnvClass, Expr)` and resolve arguments
  manually via `args, kwargs = expr.resolve(env)`.
- Stash resolved values on the appropriate object (`Resource.http`,
  `Resource.url`, `Property.param`), not in ad-hoc dicts on `Param`.

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
- :doc:`/contrib/architecture/data-access/backends/external-data-sources` —
  how `param`, `param()`-linked properties, `input()` and `header()` formulas
  are used when reading external data sources.
