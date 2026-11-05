.. default-role:: literal

#########
Commands
#########

In C4 terms (see :doc:`/contrib/architecture/index`), the commands registry is one of
the cross-cutting :ref:`components <contrib-components>` (the glue layer)
shared by the web app and the CLI containers.

Commands are internal Spinta functions, that are dispatched using multiple
dispatch, depending on argument types. For example, the command `load` has
separate implementations for each type of object being loaded:

- `load(Context, Config)`
- `load(Context, Store)`
- `load(Context, Manifest)`
- `load(Context, Model)`
- `load(Context, Model, Backend)`

Commands are the main mechanism, that glues Spinta components together:
manifests are loaded with `load`, validated with `check`, backends are
initialized with `wait`, data is read with `getall` and `getone`, written with
`insert`, `update` and `push`, and so on. If you want to understand how a
particular feature works, the fastest way is to find the commands it
implements and read their overloads.

This page explains how the command system works. For a functional reference of
the most important commands, see :doc:`/reference/commands`.

Where commands live
*******************

Definition of commands is spread across the code base:

- `spinta/commands/` — generic, backend-independent commands (`load`, `check`,
  `link`, `getall`, `getone`, `authorize`, ...). The public registry, from
  which commands are imported and called, is `spinta/commands/__init__.py`
  (imported elsewhere as `from spinta import commands` or
  `from spinta.commands import load`).
- Per-component packages — backend- and type-specific overloads:

  - `spinta/backends/postgresql/commands/` — PostgreSQL backend overloads
    (also `spinta/backends/fs/` and `spinta/backends/memory/`).
  - `spinta/types/` — data type and node overloads, e.g. `spinta/types/model.py`,
    `spinta/types/datatype.py`, `spinta/types/store.py`.
  - `spinta/manifests/` and `spinta/datasets/` — manifest and dataset
    overloads.

Defining a command
******************

A command is declared with the `@command()` decorator from
`spinta/dispatcher.py`. Each command has exactly one name, given implicitly by
the decorated function name (or explicitly via `@command(name="...")`):

.. code-block:: python

   from spinta.dispatcher import command


   @command()
   def prepare():
       """Configure component before loading.

       This is a very first thing that happens to a component.
       """


The decorated function body is only a declaration — it is just a docstring
carrying stub. From that point on, `prepare` refers to a `Command` object (a
subclass of `multipledispatch.dispatcher.Dispatcher`), which is stored in the
global `_commands` registry in `spinta/dispatcher.py`.

An optional `schema` argument of `@command()` can describe command arguments,
for commands that are read from manifests (see `Command` of manifests below).

Registering implementations
***************************

Implementations (overloads) are registered with the `Command.register()`
method. Two styles are used:

.. code-block:: python

   # Explicit signature — argument types are given to `register`:
   @commands.getall.register(Context, Model, PostgreSQL)
   async def getall(context: Context, model: Model, backend: PostgreSQL):
       ...

   # Implicit signature — argument types are taken from the function
   # annotations (positional parameters only):
   @commands.load.register()
   def load(context: Context, store: Store) -> Store:
       ...

`Command.__getitem__` makes commands callable through subscriptions and
raises `NotImplementedError` if no matching signature is found:

.. code-block:: python

   from spinta import commands

   func = commands.getall[Context, Model, PostgreSQL]  # get concrete overload

How dispatch works
******************

`Command` extends `multipledispatch.dispatcher.Dispatcher`, the same mechanism
as Python's `functools.singledispatch`, but dispatching on all positional
argument types at once. When a command is called:

1. The tuple of types of the given positional arguments is computed.
2. `Dispatcher.dispatch()` resolves the most specific matching registered
   signature (more specific signatures win over more general ones).
3. The resolved function is called with the original arguments.

It is a hard requirement that all positional arguments of a command
implementation are type-annotated: without annotations the dispatch can not
tell which overload to use. Keyword-only arguments (`*args` after `*`) are not
part of the dispatch signature and are passed through to the resolved
function.

Overloads can match on any type, most commonly:

- first parameter is always `Context` (which carries `rc`, `config` and
  `store`),
- then the node or component being processed (`Manifest`, `Store`, `Model`,
  `Property`, ...),
- then the backend (`PostgreSQL`, `FileSystem`, `Memory`) or the medium
  (`Request`, starlette response objects) when applicable.

Autodiscovery of command modules
********************************

1. **Command registry autodiscovery.** `spinta/core/context.py:create_context()`
   calls `load_commands(rc.get("commands", "modules", cast=list))` — a list of
   module import paths from raw configuration. `load_commands` imports each
   configured module and, if it is a package, recursively imports all its
   submodules (`spinta/commands`, being a package, imports everything under
   `spinta/commands/` on import). Each `@command()` decorator call at import
   time adds the command to the global `_commands` registry in
   `spinta/dispatcher.py`, and each `Command.register()` adds an overload.

   The default module list is defined in `spinta/config.py:CONFIG`:

   .. code-block:: python

      CONFIG = {
          "commands": {
              "modules": [
                  "spinta.cli",
                  "spinta.backends",
                  "spinta.formats",
                  "spinta.commands",
                  "spinta.types",
                  "spinta.urlparams",
                  "spinta.manifests",
                  "spinta.datasets",
                  "spinta.dimensions",
                  "spinta.naming",
                  "spinta.ufuncs",
              ],
              ...
          },
          ...
      }

2. **Named service functions.** Later, when
   `spinta/types/config.py:load(Context, Config)` converts raw configuration
   into a `Config` instance, every key under `commands`, except `modules`, is
   read as an `importstr` — for example
   `commands.service.range = spinta.commands.helpers:range_` — and stored in
   `Config.commands`. These are plain named functions, not multiple dispatch
   commands, see `Named service functions and declarative commands`_ below.

A practical example of the first step: the PostgreSQL backend registers its
overloads in `spinta/backends/postgresql/commands/`, which gets imported
automatically, because the `spinta.backends` package is listed in the default
module list and `load_commands` imports all submodules of a listed package.

Calling commands
****************

Commands are always referenced through the registry module `spinta/commands`,
never by importing a concrete implementation:

.. code-block:: python

   from spinta import commands

   commands.load(context, manifest)
   rows = commands.getall(context, model, backend, query=query)

This way the caller does not need to know which backend the model uses — the
dispatch mechanism picks the right implementation.

Debugging dispatch resolution
=============================

`Command.print_methods()` prints all registered overloads of a command in
resolution order with source file and line numbers:

.. code-block:: python

   from spinta.commands import getall

   getall.print_methods()

   # ---
   #    getall(Context, Model, Page)        spinta/commands/read.py:108
   #    getall(Context, Model, Request)     spinta/commands/read.py:36
   #    getall(Context, Model, PostgreSQL)  spinta/backends/postgresql/commands/read.py:35

   # `print_methods` can also be given arguments, then the matching overload
   # is marked with `*`:

   getall.print_methods(context, model, backend)

Named service functions and declarative commands
*************************************************

Two auxiliary mechanisms exist beside the `Dispatcher`-based commands:

1. **Named service functions.** As shown above, every raw config key under
   `commands`, except `modules`, is loaded by
   `spinta/types/config.py:load(Context, Config)` into `config.commands` as
   an import path, for example:

   .. code-block:: python

      CONFIG = {
          "commands": {
              ...
              "service": {
                  "range": "spinta.commands.helpers:range_",
              },
          },
          ...
      }

   `config.commands` is a two-level dict: `config.commands[scope][name]` is a
   plain Python callable, resolved with a dictionary lookup — no multiple
   dispatch is involved here.

2. **Declarative commands.** Two small classes in `spinta/components.py`,
   `Command` and `CommandList`, are *not* dispatch commands either. They are
   runtime wrappers that hold a command `name`, `args` and a reference to the
   configured function (resolved with the same
   `config.commands[scope][name]` lookup):

   - `commands.Command` — a single command with `name` and `args`,
   - `commands.CommandList` — a list of such commands.

   They are loaded from manifest-like data by two `load` overloads in
   `spinta/types/command.py`:

   .. code-block:: python

      @load.register(Context, Command, dict)
      def load(context, command, data, *, parent, scope, argname=None): ...

      @load.register(Context, CommandList, list)
      def load(context, command_list, data, *, parent, scope, argname=None): ...

   The `@command(schema=...)` argument of the command decorator describes
   command arguments, used by this mechanism to map a single string argument
   to a named argument (`argname`).

   Note, that `prepare` formulas given in manifests (dataset, resource and
   property `prepare` dimensions) are *not* handled by this mechanism — they
   are parsed into spyna AST and interpreted with ufuncs, see
   :doc:`/contrib/architecture/multiple-dispatch/ufuncs/index`.

A practical example
*******************

As an example, consider reading data of a single object. High level, what
happens is:

.. code-block:: text

   API request:  GET /gov/vpk/Reports/123
     -> commands.getone(context, model, Request, ...)        (spinta/commands/read.py)
        -> commands.getall(context, model, backend, ...)     (dispatched by backend type)
                                                           -> spinta/backends/postgresql/commands/read.py
        -> commands.prepare_data_for_response(...)           (spinta/commands/read.py)
        -> render(context, ..., format, ...)                 (spinta/renderer.py)

Each `->` step is a dispatch decision: `getall` can be served by PostgreSQL,
file system, memory or an external SQL backend, and `render` is dispatched by
the requested response format — all with the same call syntax
`commands.getall(...)`, `commands.render(...)`.

Also see
********

- :doc:`/reference/commands` — commands reference (functional overview
  of the startup and request handling commands).
- `spinta/dispatcher.py` — the `command()` decorator and `Command` class.
- `spinta/commands/__init__.py` — command declarations and docstrings.
- `spinta/config.py:CONFIG['commands']['modules']` — the module list loaded
  by autodiscovery.
- :doc:`/contrib/architecture/multiple-dispatch/ufuncs/index` — user functions (`ufuncs`), a separate, formula-oriented
  dispatch mechanism built on top of the same config autodiscovery.
