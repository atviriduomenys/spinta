.. default-role:: literal

##############
Configuration
##############

In C4 terms (see :doc:`/contrib/architecture/index`), configuration is one of the
cross-cutting :ref:`components <contrib-components>` (the core layer)
shared by the web app and the CLI containers.

Spinta configuration works in two stages:

1. `RawConfig` (`spinta/core/config.py`) — reads raw configuration values
   from several layered sources (`.env` files, environment variables, CLI
   arguments, YAML files and Python dicts), keeps track of where each value
   came from and how values should be cast to Python types.
2. `Config` (`spinta/components.py`) — the runtime configuration component,
   with typed attributes, registries of component classes and collected
   ufuncs. It is built from `RawConfig` by `commands.load(Context, Config)`.

Raw config keys are represented as dotted names, e.g.
`backends.default.dsn`, which internally are converted to tuples, e.g.
`('backends', 'default', 'dsn')`. The same key can be written as the
environment variable `SPINTA_BACKENDS__DEFAULT__DSN` (double underscore
separates nesting levels).

This page explains the configuration system. The classes themselves are
described in dedicated concept pages:

- :doc:`/contrib/architecture/configuration/rawconfig` — the `RawConfig` reader and its sources
  (`ConfigSource`, `PyDict`, `Path`, `CliArgs`, `EnvVars`, `EnvFile`).
- :doc:`/contrib/architecture/configuration/config` — the runtime `Config` component.

How configuration is read
*************************

Configuration is read by `spinta/core/config.py:read_config()`:

.. code-block:: python

   def read_config(args=None, envfile=None):
       rc = RawConfig()
       rc.read(
           [
               Path("spinta", "spinta.config:CONFIG"),
               EnvFile("envfile", envfile or ".env"),
               EnvVars("envvars", os.environ),
               CliArgs("cliargs", args or []),
           ]
       )
       # Inject extension provided defaults
       configs = rc.get("config", cast=list, default=[])
       if configs:
           rc.read([Path(c, c) for c in configs], after="spinta")
       return rc

Sources are appended to `RawConfig.sources` in the given order and values
are looked up from the last source backwards (see `RawConfig._get_config_value`
in :doc:`/contrib/architecture/configuration/rawconfig`). So the effective precedence, from lowest to
highest, is:

1. Built-in defaults — `spinta/config.py:CONFIG`, read via a `Path` source
   named `spinta` (an import string pointing to the `CONFIG` dict).
2. Additional configuration sources given via the `config` option (e.g.
   `SPINTA_CONFIG=config.yml` or `spinta -o config=config.yml`). The option
   accepts a comma separated list of YAML file paths and Python import
   strings. These sources are inserted right after the built-in defaults
   (`after="spinta"`).
3. `.env` file from the current directory (or the file given with
   `spinta --env-file ...`), source name `envfile`.
4. `SPINTA_*` environment variables, source name `envvars`.
5. `-o key=value` CLI arguments, source name `cliargs` (must be given
   immediately after `spinta`, before any subcommand).

Every source has a name, and it is shown as the value origin in the
`spinta config` output.

Default configuration and schema
********************************

Two files describe the whole configuration key space:

- `spinta/config.py:CONFIG` — a Python dict with default values for every
  configuration option, including component registries (`commands`,
  `ufuncs`, `components`, `exporters`), and the `environments` section.
- `spinta/config.yml` — a schema, loaded as `spinta/core/config.py:SCHEMA`.
  It declares the type of every option (`string`, `bool`, `integer`, `path`,
  `url`, `importstr`, ...), nested objects, and defaults.

The schema is used by `RawConfig` to:

- resolve key types and defaults when no source defines a value (e.g. the
  default of `always_show_id` comes from the schema, not from a source);
- describe dynamic key groups — options such as `backends`, `manifests`,
  `keymaps`, `components` are objects with arbitrary keys
  (`keys: {type: string}` in the schema), where each value is an object with
  a `type` reference into the corresponding component registry, e.g.
  `components.backends`;
- select type specific options via `switch: type` / `case:` blocks — for
  example an `s3` backend accepts `bucket`, `region`, `access_key_id` and
  `secret_access_key` options, while `sql` backends accept `dsn`;
- distinguish environment qualified keys (see below).

Unknown keys are allowed, they just do not get schema defaults.

Environments
************

`spinta/config.py:CONFIG['environments']` contains named environments —
`dev` and `test` by default — each overriding a subset of options (backends,
manifests, `config_path`, etc.). Tests add more via `spinta/testing/config.py`.

An environment is selected with the `env` option, e.g.
`SPINTA_ENV=dev`. When an environment is active, `rc.get()` first looks up
the value under `environments.<env>.<key>` in every source and only then
falls back to the plain key. So with `SPINTA_ENV=dev`,
`rc.get("backends", "default", "dsn")` returns the `dev` backend DSN from
`CONFIG['environments']['dev']`, unless a higher precedence source defines
`backends.default.dsn` directly.

Environment specific values can also be given through environment variables:
`SPINTA_<ENV>__<KEY>` (e.g. `SPINTA_MYENV__SERVER_URL=...`) is stored as
`environments.<env>.<key>` and becomes active when `SPINTA_ENV=<env>` is
selected. This works also for environments not defined in `CONFIG`.

Keys and values
***************

`RawConfig` exposes values through a small API (see :doc:`/contrib/architecture/configuration/rawconfig`
for details):

- `rc.get(*key, default=, cast=, required=, exists=, origin=)` — a single
  value; `cast` converts the raw string value (e.g. `importstr` imports a
  Python object by dotted path, `int` casts to an integer); `origin=True`
  also returns the source name the value came from.
- `rc.keys(*key)` — lists sub-keys of a config object, e.g.
  `rc.keys("backends")`.
- `rc.getall(*key)` — iterates all leaf values under a key.
- `rc.dump(*names, fmt=KeyFormat.cfg)` — prints a table of
  origin / name / value; used by the `spinta config` CLI command. With
  `spinta config --fmt env` names are printed as `SPINTA_` environment
  variables.

Because arbitrary nested keys can arrive from environment variables and CLI
arguments, a parent key can "announce" its children by being set to a comma
separated list: `SPINTA_BACKENDS=pg,fs` makes `rc.keys("backends")` return
`['pg', 'fs']`, after which `SPINTA_BACKENDS__PG__DSN=...` sets values for
the new `pg` backend. Setting the parent key also replaces the previous key
list, which is how keys are removed.

From `RawConfig` to `Config`
****************************

At startup, `spinta/core/context.py:create_context()`:

#. reads the raw configuration with `read_config()`;
#. loads command modules (`load_commands(rc.get("commands", "modules"))`,
   see :doc:`/contrib/architecture/multiple-dispatch/commands`);
#. creates the `Context` and instantiates the core components from the
   `components.core` registry: `Context`, `Config` and `Store`, and puts
   `rc` into the context.

Later, when a store is loaded (`spinta/cli/helpers/store.py:load_config`,
`load_store`), the typed configuration is built:

#. `commands.load(Context, Config)` (`spinta/types/config.py`) converts raw
   values into `Config` attributes: `config.commands`, ufunc registries
   (`ufunc.resolver.collect(...)`, `config.resolvers`, `config.executors`,
   see :doc:`/contrib/architecture/multiple-dispatch/ufuncs/index`), `config.components` (all component classes), 
   `config.exporters`, `config.AccessLog`, paths (`config_path`,
   `data_path`, `credentials_file`), auth and scope options, pagination,
   health thresholds, logging options and more.
#. `commands.check(Context, Config)` validates the configuration (e.g. that
   `default_auth_client` exists in the clients directory).
#. `commands.load(Context, Store)` (`spinta/types/store.py`) reads keymaps,
   backends and manifests from `rc` (`rc.keys("keymaps")`,
   `rc.keys("backends")`, `rc.get("manifest")`, `rc.get("manifests", name)`)
   and instantiates the corresponding components.

Note that backends, keymaps and manifests live on the `Store` component, not
on `Config` — `Config` only holds the component classes used to create them
(`config.components["backends"]`, etc.).

Fork and lock
*************

`RawConfig` can be forked: `rc.fork(sources)` returns a new `RawConfig`
sharing the same source list, with additional sources appended. Changes in a
fork never affect the original.

This is used by `spinta/core/config.py:configure_rc()`, called from
`spinta/core/context.py:configure_context()` before most CLI commands and
when serving API schemas. It builds a config dict with default
`backends.default` (memory, if no backends are configured), default keymap,
the `manifests.default` entry selected by `manifest`, `ensure_backends` and
command specific options, then forks the raw config. That is why the
`spinta config` output shows values with the `fork` origin.

`RawConfig` can also be locked with `rc.lock()`. A locked configuration
rejects any further `read()` calls with the exception "Configuration is
locked, use `rc.fork()` if you need to change configuration." Tests lock the
session raw config in `spinta/testing/pytest.py` to make sure tests do not
mutate the shared configuration.

Adding a new configuration option
*********************************

A new option usually touches four places:

#. a default value in `spinta/config.py:CONFIG`;
#. a schema entry in `spinta/config.yml` (type and default);
#. a `config.<name> = rc.get(...)` assignment in `spinta/types/config.py`;
#. a typed attribute on the `Config` class in `spinta/components.py`.

For dynamic key groups (like backends), only the schema entry and the
component registry (`components.<group>`) are needed — values are read
directly from `rc` where they are used.

External specifications
***********************

Configuration itself is not covered by the UAPI or DSA specifications — it
is Spinta's own layer. A few external concepts are related:

- The Twelve-Factor App, "Config" section (https://12factor.net/config) —
  configuration from the environment; the `SPINTA_*` environment variable
  layer follows this principle.
- XDG Base Directory Specification
  (https://specifications.freedesktop.org/basedir-spec/latest/) — default
  locations of `config_path` (`$XDG_CONFIG_HOME/spinta`, i.e.
  `~/.config/spinta`) and `data_path` (`~/.local/share/spinta`).
- RFC 6749 (https://www.rfc-editor.org/info/rfc6749, local extract
  `.tmp/sources/ietf/RFC6749.md`) — OAuth 2.0 roles: the `token_issuer` and
  `resource_server` options name the authorization server and resource
  server of the deployment, and `config_path/clients` files hold OAuth
  client registrations.

See also
********

- :doc:`/contrib/architecture/configuration/rawconfig` — the `RawConfig` reader component.
- :doc:`/contrib/architecture/configuration/config` — the runtime `Config` component.
- :doc:`/manual/configuration/index` — user-facing configuration
  guide.
- :doc:`/reference/components` — autodoc-generated `RawConfig` and
  `Config` API.
- `spinta/core/config.py`, `spinta/config.py`, `spinta/config.yml`,
  `spinta/types/config.py` — the implementation.

.. toctree::
   :maxdepth: 1

   Raw Config <rawconfig>
   Config <config>
   Context <context>
   Store <store>
