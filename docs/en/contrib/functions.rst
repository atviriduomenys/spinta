.. default-role:: literal

#########
Functions
#########

Functions or user functions (`ufuncs` for short) are functions, that are
exposed to users. User functions can be given in the URLs, for example
`/my/Data?select(x, y)&sort(-z)`, in `prepare` statements of manifest files or
other places.

User provided formulas are paresed into abstract syntax tree and then
interpreted using functions registered with a
`spinta.core.ufuncs.ufunc.resolver` or `spinta.core.ufuncs.ufunc.resolver`
decorator.

In order to make your function registered, make sure, that module is added to
`spinta.config.py:CONFIG['commands']['modules']`. All modules added to config
are imported automatically at import time and on import all `@ufunc.resolver`
and `@ufunc.executro` functions are registered.


Abstract syntax tree
********************

User provided formulas are parsed using `spinta.spyna` module. For example
`select(x, y)` will be parsed into:

.. code-block:: python

   {
       'name': 'select',
       'args': [
           {'name': 'bind', 'args': ['x']},
           {'name': 'bind', 'args': ['y']},
       ]
   }

This is done by `spinta.spyna.parse` function.

Then there is a `spinta.core.ufuncs.asttoexpr` function, responsible for
converting dicts and lists into `spinta.core.ufuncs.Expr` instances:

.. code-block:: python

   from spinta.core.ufuncs import Expr

   Expr(
       'select'
       Expr('bind', 'x'),
       Expr('bind', 'y'),
   )

`Expr` class adds some additional utilities to make working with AST easier.
For example:

- `str(Expr('bind', 'x'))` returns unparsed version, in this case it will be
  just `x`.

- `bind = Expr('bind'); bind('x')` - you can call `Expr` instances, like a
  normal functions, and these calls returns another `Expr` instance with call
  arguments. Keyword arguments are supported too.

- `Expr('bind', 'x').resove(env)` - this is a bit more complicated,
  `Expr.resolve` method will resolve all arguments of a given expresion on a
  given environment and returns resolved arguments as `args, kwargs` tuple.

  For example, in this case if `Expr('bind', 'x')` is called on a
  `PgQueryBuilder` environment, then `x` will be resolved into a table column.


Environments
************

Environment is used as formula evaluation context, where an initial state is
given and modified during abstract syntax tree evaluation.

Basically each AST node is interpreted on a given envoronment.

All environments are subclasses of `spinta.core.ufuncs.Env` class. By default
`Env` class these properties:

- `this` - represents current object.
- `context` - represents global Spinta context, where you can access various
  global instances.

All other properties are added by specific environemtns. Here are a few
specific environments:

- `PgQueryBuilder` - builds a `sqlalchemy` query for internal PostgreSQL backend.

- `SqlQueryBuilder` - builds a `sqlalchemy` query for external SQL databases.

- `SqlQueryBuilder` - builds arguments for `sqlalchemy` engine.

- `SqlResultBuilder` - transforms results returned by a backend, this is used
  to add additional more complex transformations that are not suppored natively
  by a backend.

And there are many more.

As you can see, some environements are very similar and share common
functioonality, adding only some environment specific features.

Usually environments are initialized like this:

.. code-block:: python

    # Create an Env sintace.
    env = SqlQueryBuilder(context)

    # Set initial state.
    env = evn.update(
        backend=backend,
        model=model,
        table=table,
    )

    # Resolve expression arguments on a given context, into Python objects.
    expr = env.resolve(ast)

    # Execute final expression with resolved arguments.
    qry = env.execute(expr)

All environemnts are initialized with an initial state, then given AST is
resolved on a given context into Python objects and finally, AST node (or
expression) is executed with resolved arguments.

For example if we have a formula like this: `print(2 + 2)`, then:

.. code-block:: python

   ast = Expr('print', Expr('add', 2, 2))
   env = Env(context)
   expr = env.resolve(ast)  # -> Expr('print', 4)
   env.execute(expr)        # -> print(4)

Here `env.resolve(ast)` resolves arguments by recursively calling
`env.resolve(ast)` on each AST argument.

If `ast` is not an `Expr`, then it returns it as is, but if `ast` is an `Expr`,
then it tries to find a resolver function registered with `@ufunc.resolver()`
decorator, calls that function with `ast` as arguments and returns its result.

Similar thing can be done manually by calling `env.call('add', 2, 2)`, `call`
method find a resolver function named `add` and calls it with arguments `2` and
`2`, without resolving arguments before call.


Resolver functions
******************

Resolver functions are called by `Env.resolve` and `Env.call` methods. Resolver
functions are responsible for interpreting given arguments on a given context.

There are two ways, how a resolver function can be registered:

1. Register a function that is responsible for resolving arguments manually:

   .. code-block:: python

     from spinta.core.ufuncs import ufunc

     @ufunc.resolver(Env, Expr)
     def add(env: Env, expr: Expr):
         args, kwargs = expr.resolve(env)
         return sum(args)

   This is mainly used, when you need to take multiple arguments and do
   something with it. Like in this example, function can be called like this
   `add(2, 2, 2, 2)`.

   If such function is registered, then it will always be called ignoring all
   other functions with the same name and different arguments. If you need to
   call another function dispatched by argument types, then you need to do it
   manually, because automatic dispatch by arguments types will be turned off.

2. Register a function that is dispatched by arguments automatically resolved
   before function is called:

   .. code-block:: python

     from spinta.core.ufuncs import ufunc

     @ufunc.resolver(Env, int, int)
     def add(env: Env, a: int, b: int):
         return a + b

     @ufunc.resolver(Env, str, str)
     def add(env: Env, a: int, b: int):
         return int(a) + int(b)

   In this case, this function will only be called, if function is called with
   two arguments and both arguments are of `int` type.

   This way, you can define multiple `add` withcions, for different argument
   types.

Resolver functions, or ufuncs are always dispathed by argument types, that
means, same function can do different things, on different environments.

So common functionally should be implemented on top of `Env`, but environemnt
specific functionality should be emplemented on a specific `Env` subclass.
