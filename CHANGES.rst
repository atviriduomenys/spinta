.. default-role:: literal

Changes
#######

0.1.6 (2020-09-11)
==================

Backwards incompatible features:

- `spinta migrate` command was renamed to `spinta bootstrap`. `spinta migrate`
  command still exists, but now it does real migrations.

- All environment variables now must use `__` to separate configuration name
  nested parts. You can list all configuration options using this command::

    > spinta config

    Origin             Name                  Value
    -----------------  --------------------  -------------
    app.config:CONFIG  backends.default.dsn  postgresql://

  By using `-f env` command line argument you can turn configuration option
  names into environment variable names::

    > spinta config -f env

    Origin             Name                            Value
    -----------------  ------------------------------  ----------
    app.config:CONFIG  SPINTA_BACKENDS__DEFAULT__TYPE  postgresql

  Previously `SPINTA_BACKENDS__DEFAULT__TYPE` was
  `SPINTA_BACKENDS_DEFAULT_TYPE`, bit this name is no longer recognized.

- Configuration option `backends.*.backend` was replaced by `backends.*.type`.
  And `backends.*.backend` now is moved to `components.backends.*`. For example
  previoulsy it looked like this::

    backends.default.backend=spinta.backends.postgresql:PostgreSQL

  Now must be written like this::

    components.backends.postgresql=spinta.backends.postgresql:PostgreSQL
    backends.default.type=postgresql

- Previously Spinta had multiple manifests, now only one default manifest
  exists and it is specified like this::

    manifest               = default
    manifests.default.type = internal
    manifests.default.sync = yaml
    manifests.yaml.type    = yaml

  Here we have two manifess `default` and `yaml`, but only one manifest named
  `default` is enabled. Default manifest is specified using `manifest`
  configuration option.

  Only one manifest can be used, the one specified by `manifest` configuration
  option.

  But multiple manifest can be configured. In the example above, `default`
  manifest is synced from `yaml` manifest. That menas, when `spinta sync`
  command is run it synces `default` manifest from another manifest specified
  in `manifests..sync` configuration option.

  From code perspective, all code liek `store.manifests['default']` is now
  replaced with `store.manifest`, because now only one active manifest is
  available. There can be multiple backends, bet other backends must be synced
  to the default one.

- Previously there was only one manifest type, YAML files based manifest. Now
  multiple manifest types were introduced and currently implemented two
  manifest types `internal` and `yaml`.

  `internal` manifest is stored in `manifests..backend` database, in `_schema`
  and `_schema/version` models.

  `yaml` manifest is same manifest as was used previously.

  Yeach manifest type can do multiple manifest specific activities, liek
  loading manifest into memory, running migrations, synchronizing manifest from
  specified sources and etc.

  Now default manifest usualy should be `internal`, which is synchronized from a
  `yaml` manifest.

- Internal `transaction` model was renamed to `_txn`.

- Configuration interpretation now slighty changes. Previously in order to add
  new items into configuration, you had to do things like this::

    backends=default,mongo
    backends.mongo.type=mongo
    backends.mongo.dsn=mongo://...

  In order to make new item to be visible, you had to explicitly add it via
  `backends=default,mongo`. Now this is not needed. All parent configuration
  nodes are added automatically, this whould be enough::

    backends.mongo.type=mongo
    backends.mongo.dsn=mongo://...

  But possibility to explicitly specify list of keys is still supported.

- Configuraiton using Python dicts now suports dotted notation:

  .. code-block:: python

    CONFIG = {
        'backends.mongo': {
            'type': 'mongo',
            'dsn': 'mongo://...',
        },
    }

  This also works with environments:

  .. code-block:: python

    CONFIG = {
        'environments': {
            'test': {
                'backends.default.dsn': 'postgresql://...',
                'backends.mongo.dsn': 'mongo://...',
            }
        }
    }

  Configuration value provided as dict is no longer merged. For example:

  .. code-block:: python

    CONFIG = {
        'backends': {
            'default': {
                'type': 'postgresql',
            },
            'mongo': {
                'type': 'mongo',
            },
        },
        'environments': {
            'test': {
                'backends': {
                    'default': {
                        'type': 'mongo',
                    },
                },
            },
            'dev': {
                'backends.default.type': 'mongo',
            }
        }
    }

  Here, `test` configuration environment fully overrides `backends` and removes
  `mongo` backend defined in default configuration scope.o
  
  But `dev` environment overrides only `backends.default.type` and leaves
  everything else as is, `mongo` backend stays untouched.

  Previously all configuration parameters were always merged.

- Context variable `config.raw` was renamed to `rc`.

- Test fixture `config` was renamed to `rc`.

- `cli` test fixture, now overrides `CliRunner.invoke` and adds `RawConfig` as
  first argument. This gives possibility to execute commands under different
  configuration. Each command invocation creates new context using given
  configuration object, so now there is no issues related with using same
  context for multiple commands.

- Removed `get_referenced_model` command. Now `Ref` objects are linked with
  referenced model in `link` command.

- Renamed `object` to `model` on `ref` properties.

New features:

- New commands:

  `spinta bootstrap` - this command does same thing as previously did `spinta
  migrate` it simply creates all missing tables from scratch and upates all
  migration versions as applied. With `internal` manifest `bootstrap` does
  nothing if it finds that `_schema/version` table is created. But with `yaml`
  manifest `bootstrap` always tries to create all missing tables.

  `spinta sync` - this command updates default manifest from list of other
  manifests specified in `manifests.<manifest>.sync`. It is also possible to
  add other kinds of manifests, for example we can add Qvarn YAML files
  directly.

  `spinta migrate` - this command automatically runs `spinta bootstrap`, then
  `spinta sync` and then executes migration actions for all versions that are
  not yet migrated.

  All these three commands helps to control schema and data migrations.

- Introduced access log. Access log can be configured using `accesslog`
  configuration option. Corrently two `accesslog` backends are implemented,
  `file` and `python`. `python` backend is used only for tests and it logs into
  memory. `file` backend can log to `stdout`, `stderr`, `/dev/null` and to a
  file. When `/dev/null` is specified as `accesslog.file`, then nothing is
  logged, internally logs are not even written to real `/dev/null` file, log
  messages are simply ignored.

- `spinta config` command now does not tries to load manifest, it just reads
  configuration and prints it. Previously `spinta config` tried to load
  manifest and if something is misconfigured it failed without showing
  configuration which could help solve the issue.

- `spinta config` command now accepts queries liek `backends..type` it prints
  all `backends.*.type` backends. I did not use `*`, because `*` is reserved
  symbol in command line.

- `spinta config` now has `-f env` argument to show config option names as
  environment variables.

- Error response now includes `component` context var with pyton path of
  component class.

- Added new command `spinta decode-token`, this command decoded token from
  stdin and prints its content to stdout in JSON format.

- Added support for Json Web Key Sets.

- Added new `token_validation_key` configuration parameter.

Internal changes:

- Changed internal file structure, not code is organized into packages and each
  package has following structure::

    backends/
      backend/
        constants.py
        components.py
        helpers.py
        commands/
          load.py
          link.py
          check.py
          wait.py
          init.py
          freeze.py
          bootstrap.py
          migrate.py
          encode.py
          validate.py
          verify.py
          write.py
          read.py
          query.py
          changes.py
          wipe.py
        types/
          array/
            init.py
            write.py
            wipe.py
        manifest/
          load.py
          sync.py

    types/
      array/
        components.py
        commands/
          load.py
          link.py
          check.py
        backends/
          postgresql/
            init.py
            write.py
            read.py
            wipe.py

    manifests/
      yaml/
        components.py
        commands/
          load.py
          link.py
          sync.py

  Internal structure now is organized same way as Spinta extensions should be
  organized. There are two types of structures, one is backend focused and
  another is type focused. Essentially everything is composed of components and
  commands, both types and backends are components and there are number of
  commands responsible for various actions performed on components.

  Actions are organized into these categories:

  - Loading components from manifest:

    - `load` - do initial component loading.
    - `link` - when everythin is loaded link dependent components.
    - `check` - when all components are loaded and linked, check components.
    - `wait` - wait while backends are up and accepts connections.
    - `init` - initialized backends.

  - Schema and data migration commands:

    - `freeze` - save all changes to manifest files as new migration versions.
    - `bootstrap` - bootstrap empty databases, just creates all missing tables.
    - `sync` - synchronizes two manifests.
    - `migrate` - run migrations

  - Data convertion between external and internam forms:

    - `encode` - convert values from internal to external form.
    - `decode` - convert values from external to internal form.

  - Data validation:
    
    - `validate` - simple data validation.
    - `verify` - complex data validation involving access to stored data.

  - Writing data to dabases (high level):

    - `insert` - insert new data to database.
    - `upsert` - insert or modify existing data in database.
    - `update` - overwrite existing data in database.
    - `modify` - modify or patch existing data in database.
    - `delete` - delete exisint data in database.

  - Writing data to database (low level):

    - `insert` - insert new objects into database.
    - `update` - updated existing data.
    - `delete` - delete existin data from database.

  - Reading data from database:

    - `getone` - read one object from database.
    - `getall` - read multiple objects from database.

  - Query functions:

    - Functions used in query.

  - Changelog:

    - `commit` - save changes to changelog.
    - `changes` - read changes from changelog.

  - Wipe all data in fastest way possible:

    - `wipe` - wipes all data of a given model.

- `RawConfig` was moved from `spinta.config` to `spinta.core.config`.
  `spinta.config` now contains only configration dict `CONFIG`, nothing else.

  `RawConfig` was fulle refactored. Previously `RawConfig` supported only
  hardcoded list with hardcoded ordering of configuration sources. Now that was
  changed to a list of sources. And each configuration sources was refactored
  to separate components. So now there is a possibility to add other
  configuration sources if needed.

  Now `RawConfig` can be initialized like this:

  .. code-block:: python

    rc = RawConfig()
    rc.read(sources, after='name')

  This gives possibility to provide configuraiton sources in any order and even
  inject sources at specified position via `after` argument.

  In tests `RawConfig` fixture is initialized into session scope, but a new
  modified instance can be crated using `RawConfig.fork` method.

- `RawConfig` now uses configuration schema defined in `spinta/config.yml`
  file. Now, this schema is only used to identify if given environment variable
  should go to environments and used to recognize if a configuration option is
  leaft or not.

  But in future, configuration schema can be used to fully validate all
  configuration paramters.

- Switched to declarative app init style, that means there is no longer global
  app instances created, app configuration is fully declarative and app is
  always initialized dynamicaly insing `spinta.api.init`.

  `spinta.api.init` accepts `Context` argument, that means, we can confure app
  in any way we want, before initializing it.

  Same thing is done to comman line commans initialization. Commands can
  receive `context` via command scopes, this means, that command can be
  configured before running it.

  All these changes gives more control in tests and now it is possible to do
  things like these:

  .. code-block:: python

    from spinta.testing.utils import create_manifest_files, read_manifest_files
    from spinta.testing.client import create_test_client
    from spinta.testing.context import create_test_context

    def test(rc, cli, tmpdir, request):
        create_manifest_files(tmpdir, {
            'country.yml': {
                'type': 'model',
                'name': 'country',
                'properties': {
                    'name': {'type': 'string'},
                },
            },
        })

        rc = rc.fork().add('test', {'manifests.yaml.path': str(tmpdir)})

        cli.invoke(rc, freeze)

        cli.invoke(rc, migrate)

        context = create_test_context(rc)
        request.addfinalizer(context.wipe_all)

        client = create_test_client(context)
        client.authmodel('_version', ['getall', 'search'])

        data = client.get('/_schema/version').json()

- There is no longer separate `internal` manifest. Since now there is only one
  manifest, `internal` manifest does not exist as a separate manifest, but it
  is injected into the default manifest.

  When default manifest is loaded, in addition, internal manifest is always
  loaded from YAML files and injected into default manifest.

  Now `internal` manifest is always exists as part of default manifest.

- Manfest loading was abstracted using manifest components and all places
  reading YAML files directly was replaced with abstract manifest components.
  this way it does not matter were manifest is defined.

- `PostgreSQL` backends no longer uses `tables[manifest][table]`, this was
  replaced with `tables[table]`, since now there is only one manifest.

- In `PostgreSQL` backends, references to `_txn` model is no longer used, in
  order to remove interdependence between two separate manifests.
  
  Also, `_txn` might be saved on another backend.

- `RawConfig` now can take default values from `spinta/config.yml`.

- `prop.backend` was moved to `dtype.backend`.
