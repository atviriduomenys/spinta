.. default-role:: literal

****
Expr
****

`spinta/core/ufuncs.py:Expr` is a node of an abstract syntax tree produced
by `spinta.core.ufuncs.asttoexpr()`. User formulas (e.g. `select(x, y)`,
`eq(a, 2)` or `a.b()`) are parsed from text with `spinta.spyna.parse()` into
spyna dicts of `{name, args}` shape, then `asttoexpr` converts those dicts
into `Expr` trees.

An `Expr` has three attributes:

- `name` — name of the function being called, e.g. `'select'`.
- `args` — tuple of positional arguments (each can be another `Expr` or a
  literal value).
- `kwargs` — dict of named arguments.

Instance methods:

- `Expr.todict()` — converts the expression back to a spyna dict.
- `str(Expr(...))` — unparses the expression back to text.
- `Expr(...)(...)` — calling an instance returns a new `Expr` with the given
  call arguments, e.g. `bind = Expr('bind'); bind('x')`.
- `Expr.resolve(env)` — resolves all arguments recursively on a given
  environment (see :doc:`env`), returns an `(args, kwargs)` tuple. Named
  arguments produced by resolvers as `Pair` instances are moved into
  `kwargs`.

Expr subclasses
***************

`asttoexpr` chooses the `Expr` subclass based on the `type` field of the
spyna AST node:

- `Expr` — a plain function call, e.g. `select(x, y)`.
- `ShortExpr` (`type: expression`) — a short form of an expression. For
  example `a=2` is a short form of `eq(bind('a'), 2)`.
- `MethodExpr` (`type: method`) — a method call expression. For example
  `a.b()` instead of `b(a)`.
- `NoOp` — an empty expression (`name='noop'`, no arguments).

There is also a convenience helper: `bind = functools.partial(Expr, 'bind')`.

Related helper types
********************

When resolvers (see :doc:`ufunc`) interpret an `Expr`, they don't have to
return final values — they can return intermediate types that are further
resolved by other resolvers. These types live in `spinta/core/ufuncs.py`
and `spinta/ufuncs/components.py`:

- `Unresolved` — base marker class for not yet fully resolved values.
- `Bind` — a name binding, e.g. `x` in `select(x)`.
- `GetAttr` — an attribute access chain, e.g. `a.b.c`.
- `Negative` / `Positive` — `Bind` subclasses with a sign, e.g. sort
  direction in `sort(-z)`.
- `Pair` — a name-value pair, e.g. a named argument `lang: 'lt'`.

External specifications
***********************

- The expression language is specified in the DSA tabular manifest
  specification, "Formulas" section:
  https://dsa.readthedocs.io/ (see also the local extract
  `.tmp/sources/dsa/DSA.md`). The DSA spec defines the grammar as a
  Python-like expression language compiled to a uniform `{name, args}` AST —
  this AST is what Spinta calls spyna, and `Expr` is its object form.
- In UAPI, expressions appear in query string parameters (filter, select,
  sort, limit, etc.): https://github.com/atviriduomenys/uapi (local extract:
  `.tmp/sources/uapi/UAPI.md`). Note: current Spinta implements the older
  UAPI URL style, not the 2.0 draft path-based filter syntax.

See also
********

- :doc:`env` — environments on which expressions are resolved.
- :doc:`ufunc` — functions that interpret expressions.
- :doc:`/contrib/architecture/multiple-dispatch/ufuncs/index` — the ufuncs explanation page.
