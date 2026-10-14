.. default-role:: literal

#####
CLI
#####

In C4 terms (see :doc:`/contrib/architecture/index`), the `spinta` command line tool is
one of Spinta's :ref:`containers <contrib-containers>`; this page documents
its :ref:`components <contrib-components>`.

The `spinta` command line interface is the user-facing layer of Spinta. It is
implemented entirely in `spinta/cli/` and built with `Typer
<https://typer.tiangolo.com/>`_. This page explains how the CLI is structured,
how command handlers are implemented, which helpers are shared between them
and how CLI commands relate to the internal command system (see
:doc:`/contrib/architecture/multiple-dispatch/commands`).

The command structure of the CLI and how it differs from the internal
dispatcher commands:

- CLI commands are user-facing, plain Python functions registered with Typer.
  They take no typed component arguments and do no multiple dispatch.
- Dispatcher commands (see :doc:`/contrib/architecture/multiple-dispatch/commands`) are internal functions implemented
  as overloads of a common name, dispatched on argument types
  (`load(Context, Manifest)`, `load(Context, Model, PostgreSQL)`, ...).
- CLI handlers *drive* dispatcher commands: they create and configure a
  `Context` from CLI options, load the store and manifest with
  `prepare_manifest()`, and then call commands such as `getall`, `write`,
  `migrate` or `sync_keymap` to do the actual work.

Entry point and application structure
*************************************

The console script entry point is declared in `pyproject.toml` as
`spinta = "spinta.cli.main:app"`, where `app` is the root
``typer.Typer()`` instance from `spinta/cli/main.py`.

Commands are registered in `main.py` via a small helper from
`spinta/cli/helpers/typer.py`::

   def add(app, name, item, **kwargs):
       if isinstance(item, Typer):
           app.add_typer(item, name=name, **kwargs)
       else:
           app.command(name, **kwargs)(item)

`add()` accepts either a plain function (registered as a command) or another
``Typer`` instance (registered as a command *group*). Currently two groups
exist:

- ``spinta keymap`` — a ``Typer`` app defined in `spinta/cli/keymap.py`
  (currently only ``spinta keymap sync``);
- `spinta cli pii` — a ``Typer`` app defined in `spinta/cli/pii.py` for
  managing person identifying information.

The root application callback (``@app.callback``) is executed before every
sub-command. It handles the global options and — importantly — creates the
Spinta `Context` and stores it in ``ctx.obj``, so every handler can pick it
up::

   @app.callback(invoke_without_command=True)
   def main(ctx: TyperContext, option, env_file, version, tb, log_file, log_level):
       ...
       ctx.obj = ctx.obj or create_context("cli", args=option, envfile=env_file)

The callback maps ``-o option.name=value`` onto configuration options (same
format as `SPINTA_` environment variables), ``--env-file`` loads a `.env`
file and ``--tb`` selects between `pretty` and `native` exception rendering.
``ctx.obj`` is the shared `Context` instance for the whole CLI invocation.

Registered commands
===================

Commands registered in `spinta/cli/main.py`, with their handler functions in
`spinta/cli/`:

+---------------------------+---------------------------------------------+
| User command              | Handler                                     |
+===========================+=============================================+
| `spinta config`           | ``cli.config.config``                       |
+---------------------------+---------------------------------------------+
| `spinta check`            | ``cli.config.check``                        |
+---------------------------+---------------------------------------------+
| `spinta key`              | ``cli.auth.key``                            |
+---------------------------+---------------------------------------------+
| `spinta token`            | ``cli.auth.token``                          |
+---------------------------+---------------------------------------------+
| `spinta client`           | ``cli.auth.client``                         |
+---------------------------+---------------------------------------------+
| `spinta init`             | ``cli.init.init``                           |
+---------------------------+---------------------------------------------+
| `spinta inspect`          | ``cli.inspect.inspect``                     |
+---------------------------+---------------------------------------------+
| `spinta pii`              | ``cli.pii.app`` (command group)             |
+---------------------------+---------------------------------------------+
| `spinta copy`             | ``cli.manifest.copy``                       |
+---------------------------+---------------------------------------------+
| `spinta comment`          | ``cli.comment.comment``                     |
+---------------------------+---------------------------------------------+
| `spinta uncomment`        | ``cli.uncomment.uncomment``                 |
+---------------------------+---------------------------------------------+
| `spinta show`             | ``cli.show.show``                           |
+---------------------------+---------------------------------------------+
| `spinta bootstrap`        | ``cli.migrate.bootstrap``                   |
+---------------------------+---------------------------------------------+
| `spinta sync`             | ``cli.migrate.sync``                        |
+---------------------------+---------------------------------------------+
| `spinta migrate`          | ``cli.migrate.migrate``                     |
+---------------------------+---------------------------------------------+
| `spinta import`           | ``cli.data.import_``                        |
+---------------------------+---------------------------------------------+
| `spinta export`           | ``cli.data.export_``                        |
+---------------------------+---------------------------------------------+
| `spinta pull`             | ``cli.pull.pull``                           |
+---------------------------+---------------------------------------------+
| `spinta push`             | ``cli.push.push``                           |
+---------------------------+---------------------------------------------+
| `spinta run`              | ``cli.server.run``                          |
+---------------------------+---------------------------------------------+
| `spinta wait`             | ``cli.server.wait``                         |
+---------------------------+---------------------------------------------+
| `spinta upgrade`          | ``cli.upgrade.upgrade``                     |
+---------------------------+---------------------------------------------+
| `spinta admin`            | ``cli.admin.admin``                         |
+---------------------------+---------------------------------------------+
| `spinta keymap`           | ``cli.keymap.keymap`` (command group)       |
+---------------------------+---------------------------------------------+
| `spinta getall`           | ``cli.get.getall``                          |
+---------------------------+---------------------------------------------+

A functional reference of the most commonly used commands (`getall`, `copy`,
etc.) lives in :doc:`/reference/cli`. For a quick usage summary of any
command, run ``spinta <command> --help``.

The command context: `ctx.obj` is a Spinta `Context`
****************************************************

CLI handlers do not receive Spinta components as their argument types (that
is the domain of dispatcher commands). Instead every handler takes a
``TyperContext`` as its first argument. The communication contract between
handlers is:

- ``ctx.obj`` — the root Spinta `Context` created by the root callback
  (``create_context("cli", ...)``). It already has `rc` and `config` set;
  command modules are also loaded at this point (`create_context()` calls
  ``load_commands(...)``, see :doc:`/contrib/architecture/multiple-dispatch/commands`).
- Many handlers (e.g. `spinta config`, `spinta check`, `spinta run`) call
  ``configure_context(ctx.obj, ...)`` from `spinta/core/context.py` to
  *fork* this root context into a ``configure`` context. This applies the CLI
  manifestations given in command arguments as `RawConfig` overrides
  (manifests, `mode`, `backend`, `backend_type`) so the later
  `load(Context, Store)` picks them up. Configuration handling is described
  in :doc:`/contrib/architecture/configuration/index`.
- Data-modifying handlers additionally go through ``require_auth()``
  (from `spinta/cli/helpers/auth.py`) which sets `auth.token` to an
  `AdminToken` or a client token, and attaches `accesslog`, so CLI writes are
  audited and authorized the same way as API requests (see :doc:`/contrib/architecture/auth/index`).

So the "Command" component of a CLI command is the `Context`: handlers build
it, fork it, attach state to it (``transaction``, ``accesslog``, ...), and
pass it into dispatcher commands.

Store loading helpers (`spinta/cli/helpers/store.py`)
*****************************************************

Nearly every command that needs loaded manifests or backends uses the same
pipeline, implemented in `spinta/cli/helpers/store.py` as a stack of small
functions, each calling dispatcher commands:

- ``load_config(context, ...)`` — runs ``commands.load(context, config)``
  (populates `Config` registries from `RawConfig`, see :doc:`/contrib/architecture/configuration/index`), may
  create the config directory, auth server keys and the default client
  (``ensure_config_dir=True``, used by `spinta check`, `spinta bootstrap`,
  ``spinta upgrade``, `spinta admin`), then runs
  ``commands.check(context, config)``.
- ``load_store(context, ...)`` — additionally runs
  ``commands.load(context, store)``: creates backends, keymaps and the
  accesslog on `Store` (see :doc:`/contrib/architecture/configuration/store`).
- ``load_manifest(context, ...)`` — runs
  ``commands.load(context, store.manifest)`` +
  ``commands.link(...)`` + ``commands.check(...)`` with knobs like
  ``load_internal``, ``full_load`` and ``rename_duplicates``.
- ``prepare_manifest(context, ...)`` — the full pipeline also used by
  `spinta run` and unit tests: ``load_manifest()`` +
  ``commands.wait(context, store)`` (waits until backends are up) +
  ``commands.prepare(context, store.manifest)`` (creates database tables).
- ``attach_backends()`` / ``attach_keymaps()`` — attach
  ``transaction`` / ``keymap.<name>`` state to the current context for
  data-modifying commands (import, export, pull, push, keymap sync).

This is the CLI mirror of the startup pipeline described in
:doc:`/contrib/architecture/wired-together` ("How everything is wired together").

Manifest path conversion (`spinta/cli/helpers/manifest.py`)
===========================================================

Commands that accept manifests as free arguments (`spinta check`, `spinta
copy`, `spinta run`, `spinta import`, ...) first convert them with
``convert_str_to_manifest_path()``. It parses the ``type:path`` prefix (or
checks against `CONFIG['components']['manifests']` and reuses
``parse_manifest_path()``, the same detection logic as in `load(Context,
Store)`, see :doc:`/contrib/architecture/manifests/index`).

Shared data helpers (`spinta/cli/helpers/data.py`)
**************************************************

Export- and import-related commands share generic data plumbing:

- ``count_rows()`` — counts rows per model (used for progress bars), running
  a ``select(count())`` query through ``commands.getall``;
- ``read_model_data()`` / ``iter_model_rows()`` — iterate rows of a model
  with pagination, limits, error handling and progress bars;
- ``process_stream()`` — consumes a `DataStream` produced by `write`, passing
  it through an exporter `Format` and printing or writing it to a file.

Other helper modules:

- `spinta/cli/helpers/errors.py` — `ErrorCounter`, stops long-running data
  operations after a configurable number of failures;
- `spinta/cli/helpers/message.py` — ``cli_error()`` and ``cli_message()``
  print diagnostics to `STDERR` (with an optional `tqdm` progress bar);
- `spinta/cli/helpers/auth.py` — ``require_auth()``, described above;
- `spinta/cli/helpers/export/` — formatter and model-filtering logic used by
  `spinta export` (``export_data()``, ``validate_and_return_formatter()``);
- `spinta/cli/helpers/push/` — state machine for `spinta push` (read source,
  sync remote changelog, write dependant nodes in the right order).

Script system: `spinta upgrade` and `spinta admin`
**************************************************

`spinta upgrade` and `spinta admin` share one generic script engine in
`spinta/cli/helpers/script/`:

- `Script` components — classes derived from ``ScriptBase``
  (`spinta/cli/helpers/script/components.py`), each with a ``run()``
  function, an optional ``check()`` function, a list of ``required`` scripts
  run before it, ``targets`` (e.g. `postgresql` backend, `sqlalchemy` keymap)
  and ``tags``. Scripts are registered in module-level instances of
  ``script_registry`` (a custom small registry, *not* the dispatcher command
  registry):

  - upgrade scripts — `UPGRADE_SCRIPT_TYPE` in
    `spinta/cli/helpers/upgrade/registry.py` (e.g. `clients`, `backends`,
    `keymaps`, `redirect`);
  - admin scripts — `ADMIN_SCRIPT_TYPE` in
    `spinta/cli/helpers/admin/registry.py` (e.g. `deduplicate`, `changelog`,
    `enum_list`, `citus_distribution`, `add_local_ids`, `remove_local_ids`).
- ``run_specific_script()`` / ``run_all_scripts()``
  (`spinta/cli/helpers/script/core.py`) sort scripts by their `required`
  dependencies, run each ``check()``, report the `ScriptStatus` result via
  ``cli_message()`` and either skip (nothing to do), pass, force or fail.
  ``ScriptStatusCache`` saves check results for repeated runs.
- `spinta admin` additionally supports reading an input file (``-i``) and
  writing results to an output path (``-o``) for data repair scripts.

The ``check()`` functions are often the same functions used by `spinta
migrate` / `spinta bootstrap`, so the commands suggest what upgrades are
needed in the same way.

How a typical handler works
***************************

A minimal, real example — `spinta wait` from `spinta/cli/server.py`::

   def wait(ctx: TyperContext, seconds: Optional[int] = Argument(None)):
       context = ctx.obj
       store = load_store(context)
       if not commands.wait(context, store, seconds=seconds, verbose=True):
           raise Exit(code=1)

And the full user-to-flow sequence of `spinta run`:

1. the root callback creates the root context in ``ctx.obj``;
2. ``convert_str_to_manifest_path()`` parses manifest arguments;
3. ``configure_context()`` forks the context with the manifests, `mode`
   (`internal`/`external`), ``--backend`` and ``--backend_type``;
4. ``prepare_manifest()`` loads the store and manifest, waits for backends
   and prepares database tables;
5. ``spinta.api.init(context)`` builds the Starlette app and ``uvicorn``
   serves it (details in :doc:`/contrib/architecture/user-interfaces/api/index`).

CLI commands vs dispatcher commands
***********************************

- CLI handlers live in `spinta/cli/` and are user-visible; dispatcher
  commands live in `spinta/commands/` and in per-component packages
  (`spinta/types/`, `spinta/backends/*/commands/`, `spinta/manifests/`,
  `spinta/datasets/`) and are the plumbing underneath.
- CLI handlers are ordinary Typer functions dispatched *once* based on the
  parsed command name — no multiple dispatch on argument types.
- Dispatcher commands are looked up through the `spinta.commands` registry
  and dispatched by the full tuple of positional argument types
  (`Context`, node, backend, ...).
- Handlers reuse the dispatcher layer: ``load_store()``, ``getall()``,
  ``write``, ``wait``, ``sync``, ``migrate`` and friends are the same
  commands the API and tests use. When writing a new CLI command, prefer
  this pattern: build or fork the context, load the store/manifest, then
  call existing commands rather than reimplementing logic.

See also
********

- :doc:`/contrib/architecture/multiple-dispatch/commands` — the internal dispatcher command system used by the CLI.
- :doc:`/contrib/architecture/configuration/index` — how `RawConfig` is assembled from CLI options and env
  files, and how `load(Context, Config)` converts it to `Config`.
- :doc:`/contrib/architecture/manifests/index` — manifests, `ManifestPath` detection and the
  ``type:path`` prefix accepted by CLI commands.
- :doc:`/contrib/architecture/auth/index` — OAuth 2.0 clients, keys and tokens managed by
  `spinta key` / `spinta client` / `spinta token`.