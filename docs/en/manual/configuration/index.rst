.. default-role:: literal

.. _configuration:

Configuration
#############

Spinta combines settings from several sources. When the same setting is
specified more than once, the value loaded later takes precedence. The standard
loading order is:

1. Spinta's built-in default settings.
2. The `.env` file in the current directory, if it exists.
3. Environment variables.
4. Command-line settings.

Additional configuration files do not have a fixed place in this order. A file
is loaded immediately before the source that names it. For example, if the
`.env` file names an additional configuration file, the order is:

1. Spinta's built-in default settings.
2. The additional configuration file.
3. The `.env` file.
4. Environment variables.
5. Command-line settings.

`config_path` is a setting that identifies Spinta's configuration-data
directory. It is not a configuration source and does not automatically load
configuration files from that directory. Use the `config` setting to load an
additional configuration file.


.. toctree::
   :maxdepth: 1

   auth
   backend
   manifest
   soap-custom-adapters
   front-page

Static configuration keys
*************************

YAML files and Python dictionaries are dynamic configuration sources. When
they define named entries, such as backends, their entries are combined with
entries from other dynamic sources. For example, if one YAML file defines the
`primary` backend and another defines the `archive` backend, both backends are
available after the files are loaded.

Environment variables and command-line settings are static-key sources. When
they explicitly provide the names below a setting, those names become the
complete list of active entries at that level. For example, this configuration
file defines two backends:

.. code-block:: yaml

    backends:
      primary:
        type: postgresql
      archive:
        type: postgresql

Setting either of the following selects only the `primary` backend:

.. code-block:: sh

    SPINTA_BACKENDS=primary
    spinta -o backends=primary config backends

The `archive` backend is then excluded, even though it remains in the YAML
file. Settings inside `primary` can still be read from the YAML file unless the
environment variable or command-line setting overrides them.

This distinction is important for deployments. YAML files and dictionaries can
compose a shared configuration, while environment variables and command-line
settings can deliberately select the exact named entries that are active. This
prevents unused or unintended backends, models, and similar entries from being
enabled by a lower-priority configuration file. Use `spinta config` to inspect
the effective values and their origins.

.. _config-file:

Configuration file
******************

An additional configuration file is named with the `config` setting. This
setting can be supplied from the `.env` file, environment variables, or the
command line. The file is loaded immediately before the source that supplies
its path, so that source can override values from the file.

`config` option can contain list of comma separated values. Each value can be a
path to `.yml` file or it can be a python dotted path like
`myapp.config:CONFIG`, pointing to a dict.

For example we can create an `/tmp/custom.yml` configuration file:

.. code-block:: yaml

    env: production
    default_auth_client: default

    keymaps:
      default:
        type: sqlalchemy
        dsn: sqlite:////path/to/keymap.db

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

And use it to configure Spinta::

  export SPINTA_CONFIG=/tmp/custom.yml
  spinta config backends

Output::

  Origin           Name               Value
  ---------------  -----------------  -----
  /tmp/custom.yml  backends.default.type  postgresql

Nested configuration files
==========================

Configuration files can include other configuration files using the same
setting. Included files are loaded in the order in which they are listed, then
the file that includes them is loaded. Thus, a value in the including file
overrides the same value in an included file.

For example, this structure separates model-related settings from Citus
distribution settings:

.. code-block:: text

    .env
    config.yml
    models.yml
    citus.yml
    citus_generated.yml

The `.env` file points to the main configuration file:

.. code-block:: sh

    SPINTA_CONFIG=config.yml

The main configuration file includes the two specialised files:

.. code-block:: yaml
    :caption: config.yml

    config:
      - models.yml
      - citus.yml

`models.yml` can contain backend and model property-type settings. `citus.yml`
can contain manually maintained Citus distribution settings and include
automatically generated distribution settings:

.. code-block:: yaml
    :caption: citus.yml

    config:
      - citus_generated.yml

The resulting loading order is:

1. Spinta's built-in default settings.
2. `models.yml`.
3. `citus_generated.yml`.
4. `citus.yml`.
5. `config.yml`.
6. `.env`.
7. Environment variables.
8. Command-line settings.

The following simplified example shows how the model settings are combined.
It illustrates configuration merging only; it is not a complete manifest.

.. code-block:: yaml
    :caption: models.yml

    models:
      dataset/Country:
        backend: default
        properties:
          code: string
      dataset/City:
        backend: default
        properties:
          name: string

.. code-block:: yaml
    :caption: citus.yml

    config:
      - citus_generated.yml
    models:
      dataset/Country:
        distribution: schema
      dataset/Place:
        distribution: copy

.. code-block:: yaml
    :caption: citus_generated.yml

    models:
      dataset/Place:
        distribution: undistributed
      dataset/Origin:
        distribution: copy

To inspect the effective model settings and the file from which each value was
taken, run:

.. code-block:: sh

    spinta config models

The output will contain the combined settings, for example:

.. code-block:: text

    Origin               Name                                          Value
    -------------------  --------------------------------------------  -------------
    models.yml           models.dataset/Country.backend                default
    models.yml           models.dataset/Country.properties.code        string
    citus.yml            models.dataset/Country.distribution           schema
    models.yml           models.dataset/City.backend                   default
    models.yml           models.dataset/City.properties.name           string
    citus.yml            models.dataset/Place.distribution             copy
    citus_generated.yml  models.dataset/Origin.distribution            copy

In this example, the manually maintained distribution setting for
`dataset/Country` supplements the model settings from `models.yml`. The `copy`
setting for `dataset/Place` overrides the `undistributed` setting from
`citus_generated.yml`, while `dataset/Origin` retains the generated `copy`
setting.

Keymap
******

Keymap is used to map external identifiers with internal identifiers. Storage (Backend) can be configured.
By default Sqlite is used like on configuration example above, but it can be changed to other,
faster and more robust storages. Here full list of options:

- SQLite database with SQLAlchemy backend configuration:

  .. code-block:: yaml

      keymaps:
        default:
          type: sqlalchemy
          dsn: sqlite:////path/to/keymap.db

- Redis persistent storage with Redis, configured like:

  .. code-block:: yaml

      keymaps:
        default:
          type: redis
          dsn: redis://redis-address:6379/1

  Redis (valkey redis fork) docker run configuration can be found under project docker-compose.yml (root directory).
  **IMPORTANT! Redis must be enabled in persistent mode (the `--appendonly yes --appendfsync always` parameter in docker-compose).**
  There are several persistent modes (see the Redis/Valkey documentation).
  Recommended approach (`--appendonly yes --appendfsync always`) provides the most durability and the least performance compared to the others.

Environment variables
*********************

All environment variables must use `SPINTA_` prefix and hierarchy levels must
be separated with `__`. For example::

  SPINTA_BACKENDS__FOO__TYPE=postgresql spinta config backends

Output::

  Origin   Name               Value
  -------  -----------------  -----
  envvars  backends.foo.type  postgresql


`.env` file
***********

Spinta tries to read `.env` file from current directory if such file exists.
`.env` file simply contains list environemnt variables.

Empty lines and lines starting with `#` are ignored.

Example `.env` file:

.. code-block:: sh

    UTHLIB_INSECURE_TRANSPORT=0
    SPINTA_CONFIG=config.yml


.. _config_path:

Configuration directory
***********************

In addition to the main configuration file, Spinta uses a configuration-data
directory for files such as client credentials, token verification keys, and
client access data. By default this directory is `$XDG_CONFIG_HOME/spinta`__,
usually `~/.config/spinta`.

__ https://specifications.freedesktop.org/basedir-spec/latest/ar01s03.html

The `config_path` setting changes this directory. It does not make Spinta load
all files from the directory as configuration files.


Command line arguments
**********************

All spinta commands have `-o` command line argument. With `-o` you can set
configuration values using dotted notation, for example::

  > spinta -o backends.foo.type=postgresql config backends
  Origin   Name               Value
  -------  -----------------  -----
  cliargs  backends.foo.type  postgresql

`-o` must be use immediately after `spinta` command and before any subcommands.

You can use `-o` multiple times, to set multiple configuration options.


Configuration syntax
********************

Spinta configuration values are organized in a hierarchy of options. Usually
hierarchy levels are separated by a `.` or by a `__`. `__` is used for
environment variables, since `.` is not allowed in environment variables names.

Configuration options containing suboptions are interpreted as list of
suboption names. For example if we have following configuration::

  backends.pg.type=postgresql
  backends.sql.type=sql
  backends.fs.type=fs

`backends` value is a list containing `pg`, `sql`, and `fs`, called keys. If you want to remove
existing keys, you can set `backends`, like this::

  backends=pg,fs

This will remove all configuration options except `backends.pg` and
`backends.fs`. In this case, `backends.sql` will be removed.


Inspecting configuration
************************

You can inspect current configuration by using following command::

  spinta config

This command will list current configuration values and will also tell source of
origin of each configuration value.

You can filter listed configuration options by providing list of prefixes, for
example::

  spinta config backends manifests

Since Spinta is usually configured using environment variables, you can show
configuration option names in environment-variable form by adding `--fmt env`::

  spinta config --fmt env backends manifests

By default, `spinta config` shows only the effective value of each setting. To
also see values that were overridden by another source, add `--all-sources`::

  spinta config --all-sources backends manifests

For example, suppose `config.yml` contains:

.. code-block:: yaml

    backends:
      primary:
        type: postgresql

and the environment sets `SPINTA_BACKENDS__PRIMARY__TYPE=memory`. Without
`--all-sources`, only the effective value is shown::

  Origin   Name                    Value
  -------  ----------------------  ------
  envvars  backends.primary.type   memory

With `--all-sources`, both values are shown, which makes the override clear::

  Origin      Name                    Value
  ----------  ----------------------  ----------
  config.yml  backends.primary.type   postgresql
  envvars     backends.primary.type   memory
