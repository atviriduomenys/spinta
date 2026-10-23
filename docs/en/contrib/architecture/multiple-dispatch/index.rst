.. default-role:: literal

#################
Multiple dispatch
#################

Spinta dispatches work with multiple dispatch: the implementation of
a command or a ufunc is selected at call time by the types of its
positional arguments. Two mechanisms are built on top of the same
`multipledispatch`-based machinery (see `spinta/dispatcher.py`):

- **Commands** (`Command` objects, see :doc:`commands`) — internal
  service functions like `commands.load`, `commands.link`,
  `commands.check`, `commands.getall`, `commands.getone`,
  `commands.push`, `commands.wait`, ... declared as `@command()`
  decorated stubs in `spinta/commands/__init__.py` and implemented as
  overloads registered per backend, manifest or node type.

- **Ufuncs** (`Ufunc` objects, see :doc:`ufuncs/index`) — user-facing
  functions (expressions) interpreted on `Env` environments, registered
  with `@ufunc.resolver` / `@ufunc.executor` and collected from the
  modules listed in `spinta/config.py:CONFIG['ufuncs']`.

Both are collected at startup: `spinta/core/context.py:create_context()`
imports the modules listed in `CONFIG['commands']['modules']`, and
`load(Context, Config)` (`spinta/types/config.py`) builds the `Command`
and `Ufunc` dispatchers from the collected overloads.

The `spinta` CLI adds a third, *non*-dispatching naming convention: plain
Typer handlers in `spinta/cli/` that shell out to these commands (also
see :doc:`/contrib/architecture/user-interfaces/cli`).

.. toctree::
   :maxdepth: 1

   Commands <commands>
   Ufuncs <ufuncs/index>
