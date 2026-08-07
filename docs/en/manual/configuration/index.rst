.. default-role:: literal

.. _configuration:

Configuration
#############

Spinta can be configured using multiple configuration sources: YAML
configuration files, `.env` file, environment variables and command line
arguments. Sources are read in a specific order and each subsequent source
overrides values from all previous sources, see :ref:`configuration-sources`.
Configuration values are organized in a hierarchy of options, see
:ref:`configuration-syntax`.


Quick start
***********

The fastest way to configure Spinta is to create a YAML configuration file and
inspect the result with `spinta config`. A full configuration example, with a
keymap, a backend, a manifest and an access log, might look like this:

.. code-block:: yaml

    env: production
    default_auth_client: default

    keymaps:
      default:
        type: sqlalchemy
        dsn: sqlite:////path/to/data/keymap.db

    backends:
      default:
        type: postgresql
        dsn: postgresql://admin:admin123@localhost:5432/spinta

    manifest: default
    manifests:
      default:
        type: csv
        path: /path/to/data/manifest.csv
        backend: default
        keymap: default
        mode: internal

    accesslog:
      type: file
      file: /path/to/data/accesslog.json

The recommended location for the main configuration file is
`~/.config/spinta/config.yaml`. Point `SPINTA_CONFIG` to it and inspect the
result::

  export SPINTA_CONFIG=~/.config/spinta/config.yaml
  spinta config backends

Output::

  Origin                        Name                   Value
  ----------------------------  ---------------------  -------------------------------------------------
  ~/.config/spinta/config.yaml  backends.default.type  postgresql
  ~/.config/spinta/config.yaml  backends.default.dsn   postgresql://admin:admin123@localhost:5432/spinta

`spinta config` lists all configuration values and tells the source of origin
of each value. You can also filter listed options by providing a list of
prefixes, see :ref:`inspecting-config` for details.

.. toctree::
   :maxdepth: 1

   auth
   backend
   keymap
   manifest
   soap-custom-adapters
   front-page


.. _inspecting-config:

Inspecting configuration
************************

You can inspect current configuration by using following command::

    spinta config

This command will list current configuration values and will also tell source of
origin of each configuration value.

You can filter listed configuration options by providing list of prefixes, for
example::

    spinta config backends manifests

You can show configuration options names as environment variables by adding `-f
env` argument::

    spinta config -f env backends manifests


.. _configuration-sources:

Configuration sources
*********************

Spinta can be configured using multiple configuration sources. Sources are
read in the following order, each subsequent source overrides values from all
previous sources:

1. Default Spinta configuration `spinta.config:CONFIG`, you can't change this
   without change Spinta's code.

2. Configuration files specified with `config` option (for example
   `spinta -o config=~/.config/spinta/config.yaml`).

3. `.env` file containing environment variables with the `SPINTA_` prefix.

4. Environment variables with the `SPINTA_` prefix.

5. Command line arguments passed to `spinta` command with `-o option=value`
   argument.

This means, that values from command line arguments have the highest
precedence, then environment variables, `.env` file, configuration files and
finally default Spinta configuration, which has the lowest precedence.

Each source only overrides values it sets, all other values are left as is.
For example, if a configuration file sets `env: production` and environment
variables set `SPINTA_ENV=testing`, then the `env` value will be `testing`,
but all other values from the configuration file will still be used.

Each configuration source is described in more detail in a separate section:

- :ref:`config-file`
- :ref:`env-file`
- :ref:`env-vars`
- :ref:`cli-args`


.. _configuration-syntax:

Configuration syntax
********************

Spinta configuration values are organized in a hierarchy of options. Usually
hierarchy levels are separated by a `.` or by a `__`. `__` is used for
environment variables, since `.` is not allowed in environment variables names.

Configuration values can be of two types: simple and complex.

Simple values
^^^^^^^^^^^^^

Simple values are scalar values, like strings, numbers or booleans. Environment
variables and command line arguments can only have simple values. Complex
values can be simulated with simple values, using dotted notation:

.. code-block:: yaml

    backends.pg.type: postgresql
    backends.sql.type: sql

Here `backends.pg.type` is a simple value for the `backends.pg.type` option,
but `backends` and `backends.pg` are interpreted as lists of suboption names.
`backends` contains keys `pg` and `sql`.

Simple values override only values of leaf options. Structure of options is
merged, not replaced. For example, if base configuration defines:

.. code-block:: yaml

    backends:
      one:
        type: sql
      two:
        type: pg

And the override source sets:

.. code-block:: sh

    SPINTA_BACKENDS__ONE__TYPE=sqlite

Then the result will contain both `backends.one` and `backends.two` backends,
and only the `backends.one.type` value will be overridden.

YAML configuration files can also use simple values with dotted notation, in
that case they are merged the same way as environment variables or command line
arguments:

.. code-block:: yaml

    backends.two.type: pg
    backends.two.dsn: pg@example.com

Because simple values override only individual options, they can be used to
override a single value inside an existing complex structure, without
replacing the whole structure. For example, if base configuration defines a
`default` backend:

.. code-block:: yaml

    backends:
      default:
        type: postgresql
        dsn: postgresql://user:pass@host:5432/spinta

Then a configuration file can override just the `backends.default.type` value:

.. code-block:: yaml

    backends.default.type: sqlite

In this case, `backends.default.dsn` will be left as is, only the
`backends.default.type` value will be overridden. The `backends.default`
structure is merged, not replaced.

Complex values
^^^^^^^^^^^^^^

Complex values are values containing suboptions. When a complex value is
assigned to a configuration key (option), it overrides the whole structure of
suboptions of that key.

For example, if base configuration defines two backends:

.. code-block:: yaml

    backends:
      one:
        type: sql
      two:
        type: pg

And the override source defines `backends` as a complex value with a single
backend:

.. code-block:: yaml

    backends:
      two:
        type: pg

Then the result will contain only the `backends.two` option, because the whole
`backends` structure was replaced. `backends.one` will be removed.

Structure of a complex value is controlled by a list of suboption names. The
list itself is a value of the complex key, so it can be reset or removed by
setting the key to an empty value or to a list of keys to keep. This works even
from sources that can only set simple values, like environment variables.

For example, to reset all existing backends and define a new one:

.. code-block:: sh

    SPINTA_BACKENDS=
    SPINTA_BACKENDS__TWO__TYPE=pg
    SPINTA_BACKENDS__TWO__DSN=pg@example.com

The empty value of `backends` resets the list of backends, then `two` is added
back. The result will contain only the `backends.two` backend.

If you set a key to an empty value without adding any keys back, then all keys
in that subtree are removed recursively. Keys removed from all levels are
removed and are no longer available. For example, if base configuration defines
two backends:

.. code-block:: yaml

    backends:
      one:
        type: sql
        dsn: sql@example.com
      two:
        type: pg
        dsn: pg@example.com

And the override source sets:

.. code-block:: sh

    SPINTA_BACKENDS=

Then all `backends.one` and `backends.two` options are removed, and no backends
will be available.

Instead of an empty value, you can set the key to a list of keys to keep::

    SPINTA_BACKENDS=pg,fs

This will remove all configuration options except `backends.pg` and
`backends.fs`. In this case, `backends.sql` will be removed and will not be
available at any level.



.. _config_path:

Configuration directory
***********************

In addition to main configuration, there are other configuration files, for
example client credentials, token authorization keys, client access and other
files. All these additional files are stored in `$XDG_CONFIG_HOME/spinta`__
directory, usually it is `~/.config/spinta` directory.

__ https://specifications.freedesktop.org/basedir-spec/latest/ar01s03.html

Path to this directory can be changed via `config_path` configuration option.
If `config_path` is not set, then the default XDG config path is used, that is
`$XDG_CONFIG_HOME/spinta/`, usually `~/.config/spinta/`.

`config_path` is not a configuration value source, it only points to a
directory where Spinta looks for and stores auxiliary data.

The `config_path` directory is used to store the following data:

- `clients` - client credentials used for authorization.
- `keys` - keys used to sign and validate authorization tokens.
- `helpers` - auxiliary data, like keymaps.
- `credentials.cfg` - default location of the credentials file.

`config_path` should be used when you need to change where Spinta looks for and
stores this auxiliary data. For example, when running Spinta in a Docker
container, where the default `~/.config/spinta` directory is not persistent, or
when running multiple Spinta instances that need to share the same client
credentials and keys.


.. _config-file:

YAML configuration files
************************

`config` option tells Spinta, which additional configuration files to read.
Configuration files are read right after the default Spinta configuration, but
before the `.env` file, environment variables and command line arguments.
`config` option can contain list of comma separated values. Each value can be a
path to `.yaml` file or it can be a python dotted path like
`myapp.config:CONFIG`, pointing to a dict.

The recommended location for the main configuration file is
`{config_path}/config.yaml`, usually it is `~/.config/spinta/config.yaml`.

For example we can create a configuration file at
`~/.config/spinta/config.yaml`:

.. code-block:: yaml

    env: production
    default_auth_client: default

    backends:
      default:
        type: postgresql
        dsn: postgresql://user:pass@host:5432/spinta

    manifest: default
    manifests:
      default:
        type: csv
        path: /path/to/manifest.csv
        backend: default
        keymap: default
        mode: external

    accesslog:
      type: file
      file: /path/to/accesslog.json

Backends, manifests and keymaps are described in more detail in
:ref:`backend-configuration`, :ref:`manifest-configuration` and
:ref:`keymap-configuration`.

And use it to configure Spinta::

  export SPINTA_CONFIG=~/.config/spinta/config.yaml
  spinta config backends

Output::

  Origin                        Name                   Value
  ----------------------------  ---------------------  ----------
  ~/.config/spinta/config.yaml  backends.default.type  postgresql

Configuration files are the recommended way to configure Spinta. Use them for
the main, long-lived part of the configuration, since they are easier to read,
review and maintain than other configuration sources, and they are the only
source that supports complex values.


.. _env-file:

`.env` file
***********

Spinta tries to read `.env` file from current directory if such file exists.
`.env` file simply contains list of environment variables.

Empty lines and lines starting with `#` are ignored. Only variables with the
`SPINTA_` prefix are read by Spinta.

Example `.env` file:

.. code-block:: sh

    AUTHLIB_INSECURE_TRANSPORT=0
    SPINTA_CONFIG=~/.config/spinta/config.yaml

`.env` file is handy during development, when you need to set a bunch of
environment variables and don't want to export them all manually. It is not
meant for production use, since `.env` file is usually not deployed to
production servers. For production use environment variables instead.


.. _env-vars:

Environment variables
*********************

All environment variables must use `SPINTA_` prefix and hierarchy levels must
be separated with `__`. For example:

.. code-block:: sh

    export SPINTA_BACKENDS__FOO__TYPE=postgresql
    spinta config backends

Output::

  Origin   Name               Value
  -------  -----------------  ----------
  envvars  backends.foo.type  postgresql

Environment variables are a good choice for deployment specific values, such
as credentials, URLs and other values that differ between development,
staging and production environments. They are not stored in configuration
files, so they can be set per environment, per process or managed by your
deployment tooling.


.. _cli-args:

Command line arguments
**********************

All spinta commands have `-o` command line argument. With `-o` you can set
configuration values using dotted notation, for example::

  > spinta -o backends.foo.type=postgresql config backends
  Origin   Name               Value
  -------  -----------------  ----------
  cliargs  backends.foo.type  postgresql

`-o` must be used immediately after `spinta` command and before any subcommands.

You can use `-o` multiple times, to set multiple configuration options.

Command line arguments are useful for temporary, one-off overrides, for example
when debugging or when running a single command with a slightly different
configuration. For anything persistent, use configuration files or environment
variables.
