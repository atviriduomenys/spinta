:orphan:

.. default-role:: literal

******
Ufunc
******

Ufuncs are user functions — resolver and executor functions that interpret
user provided formulas. They are collected from plain Python modules with
the `UFuncRegistry` registries defined in `spinta/core/ufuncs.py`:

- `ufunc.resolver` — registry of resolver functions, called by
  `Env.resolve` and `Env.call` (see :doc:`env`).
- `ufunc.executor` — registry of executor functions, called by
  `Env.execute`.

Both registries are instances of `spinta.core.ufuncs.UFuncRegistry`.

Registration
************

Functions are registered with a decorator:

.. code-block:: python

    from spinta.core.ufuncs import ufunc

    @ufunc.resolver(Env, int, int)
    def add(env: Env, a: int, b: int):
        return a + b

The first type in the signature is the environment type the function is
registered for — the same function name can have different implementations
for different environments (e.g. generic `Env` resolvers in
`spinta/ufuncs/common.py` and `spinta/ufuncs/querybuilder/ufuncs.py`, and
backend specific ones in `spinta/backends/postgresql/ufuncs/query/ufuncs.py`).

The decorator accepts:

- positional types — argument types used for multiple dispatch; first type
  is always the environment type;
- `name='...'` — register under a different name than the function name
  (used e.g. to register several overloads under `getattr`, see
  `spinta/ufuncs/querybuilder/ufuncs.py`);
- `names=['a', 'b']` — register the same function under several names at
  once (the name is injected as the second argument via `_inject_name`).

Dispatching
***********

When `UFuncRegistry.ufuncs()` is called (during `load(Context, Config)`, see
`spinta/types/config.py`), collected `(name, func, types)` tuples are turned
into `Ufunc` dispatchers, one per name. `Ufunc` is a subclass of
`spinta/dispatcher.py:Command`, so multiple dispatch works exactly like for
commands (see :doc:`/contrib/architecture/multiple-dispatch/commands`): the implementation is picked by the full
tuple of argument types, and a resolver registered with `(Env, Expr)` will
shadow other implementations of the same name.

Autoargs
********

The `Ufunc` dispatcher sets `autoargs = True` by default. This means, when
an expression is resolved, its arguments are resolved automatically and the
resolver is called with already resolved values.

If a resolver is registered with `(Env, Expr)` types, `autoargs` is turned
off for that name: the resolver receives the whole unresolved `Expr` and
must resolve its arguments itself (`expr.resolve(env)`). This is used by
variadic functions like `select` or `sort`.

Loading and collection
**********************

- `UFuncRegistry.collect(modules)` — imports the given modules; decorated
  functions register themselves as a side effect of the import. The module
  list comes from `spinta/config.py:CONFIG['ufuncs']`.
- `UFuncRegistry.register(name, func, types)` — registers a single function
  programmatically; used by plugin-style loaders, e.g.
  `spinta/adapters/soap_plugins.py:register_soap_ufuncs` registers SOAP
  adapter resolvers from external Python files listed in
  `soap_adapter_modules`.
- `UFuncRegistry.ufuncs()` — builds `dict` of name → `Ufunc` dispatchers;
  results are stored in `config.resolvers` and `config.executors`.

Resolver vs executor
********************

Resolvers (called by `Env.resolve`) interpret arguments on a given
environment — they usually return intermediate values, which are further
resolved by other resolvers (see the intermediate types in :doc:`expr`).

Executors (called by `Env.execute`) implement the final evaluation step and
are called with already resolved arguments. They are used sparsely — most
environments do everything with resolvers and `default_resolver`. A typical
executor example: `@ufunc.executor(ParamBuilder, NotAvailable)` in
`spinta/dimensions/param/ufuncs.py`.

External specifications
***********************

- The function names and semantics (position, named and keyword arguments,
  method chaining `a.b()`, operators) come from the DSA tabular manifest
  specification, "Formulas" section: https://dsa.readthedocs.io/ (local
  extract `.tmp/sources/dsa/DSA.md` — it lists value functions, context
  functions, selection functions, etc.).
- In UAPI, these functions are exposed through query string parameters of
  data requests (filter, select, sort, ...):
  https://github.com/atviriduomenys/uapi (local extract:
  `.tmp/sources/uapi/UAPI.md`).

See also
********

- :doc:`/contrib/architecture/multiple-dispatch/ufuncs/index` — the ufuncs explanation page.
- :doc:`expr` — the expression tree.
- :doc:`env` — environments on which ufuncs are dispatched.
