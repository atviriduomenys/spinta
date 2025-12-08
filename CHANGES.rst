Changes
#######

0.2dev12 (unreleased)
=====================



0.2dev11 (2025-12-03)
=====================

Improvements:

- `spinta inspect` with `Sql` manifest now inspects all schemas, while trying to ignore system generated ones (`#1483`_).
- Added ability to customize models and their properties inside config. You can now specify custom type implementation
  with: `models.<model_name>.properties.<property_name>.type`. It accepts python import path to the implementation (`#599`_).

.. _#1483: https://github.com/atviriduomenys/spinta/issues/1483
.. _#599: https://github.com/atviriduomenys/spinta/issues/599

0.2dev10 (2025-11-27)
=====================

Backwards incompatible:

- `spinta migrate` with the `postgresql` backend now requires all tables and columns to have up-to-date comments with
  their full uncompressed names. Migrations are likely to fail or be incorrect if comments are missing or
  outdated. Use the `spinta upgrade postgresql_comments` script to validate and update all required comments (`#1579`_).

Improvements:

- `spinta migrate` now uses PostgreSQL comments to map tables and models together (`#1579`_).
- The `internal` `postgresql` backend now adds full name comments to all its tables and columns. To migrate to the new
  changes, the `spinta upgrade postgresql_comments` script was added (`#1579`_).

Bug fixes:

- Fixed an issue where `spinta migrate` incorrectly created table drop scripts for `changelog` and `redirect` tables (`#1579`_).

  .. _#1579: https://github.com/atviriduomenys/spinta/issues/1579

0.2dev9 (2025-11-21)
====================

New Features:

- Spinta as auth server - introduce /.well-known/jwks.json API endpoint to retrieve public verification keys,
  also known as well-known, jwk.
- Spinta as Agent:
    - add support for multiple public keys picked dynamically for each access token by kid value. If not found, then by
      algorithm (`alg` & `kty`).
      This unlocks using auth servers with public key rotation, like Gravitee.
    - add new `spinta key download` command to download public keys (JWKs) to a local file to use it later for
      verification.
    - move existing `spinta genkeys` command to `spinta key generate`.


.. _#1569: https://github.com/atviriduomenys/spinta/issues/1569

New Features:
- Added new scope `client_backends_update_self` that only allows updating own client file backends attribute (`#1582`_)
- Add `param.header()` prepare function that constructs HTTP header. Can be used in `soap` backend (`#1576`_).


  .. _#1582: https://github.com/atviriduomenys/spinta/issues/1582
  .. _#1576: https://github.com/atviriduomenys/spinta/issues/1576

Improvements:

- Keymap and push db sync now attempts to retry data fetch after failing to get valid response from remote server.
  Retries can be modified with `sync_retry_count` and `sync_retry_delay_range` config values (`#1594`_).

  .. _#1594: https://github.com/atviriduomenys/spinta/issues/1594

Bug fixes:

- Fixed `inspect` command not recognizing Oracle LONG RAW types (`#1532`_).

  .. _#1532: https://github.com/atviriduomenys/spinta/issues/1532

0.2dev8 (2025-11-06)
====================

Bug fixes:

- Fixed a crash caused by `split()` prepare function with `None` values (`#1570`_).
- Changed `admin` and `upgrade` command `Argument` default value check (`#1575`_).
- Fixed an error where MySQL LONGBLOB wasn't recognized (`#1484`_).

  .. _#1570: https://github.com/atviriduomenys/spinta/issues/1570
  .. _#1575: https://github.com/atviriduomenys/spinta/issues/1575

0.2dev7 (2025-10-23)
====================

New Features:

- Added `eval()` prepare function for resources. This function allows using the value of a prepare expression
  as a data source instead of a source column. The `eval(param(..))` syntax can reference a property from
  another resource, even if it belongs to a different backend. `dask/json` and `dask/xml` backends can now
  use `eval()` to read data from properties of `dask/json`, `dask/xml`, or `soap` backends. (`#1487`_)
- Introduce new optional keymap backend - persistent Redis. (`#825`_)

.. _#1487: https://github.com/atviriduomenys/spinta/issues/1487
.. _#825: https://github.com/atviriduomenys/spinta/issues/825

Bug fixes:

- Removed `_base` column from HTML response when viewing SOAP data with URL parameters (`#1338`_)
- Added required parameters validation, when building SOAP query, and raising exception `MissingRequiredProperty` if parameter is missing (`#1338`_)
- Remove synchronization logic, will be re-introduced with upcoming iterations for the same ticket (`#1488`_).
- Fixed `spinta migrate -d` argument not collecting correct tables with long names (`#1557`_).

  .. _#1338: https://github.com/atviriduomenys/spinta/issues/1338
  .. _#1557: https://github.com/atviriduomenys/spinta/issues/1557

Security:

- Private keys and client credential files are now created with restrictive permissions (600 for files, 700 for directories) to prevent unauthorized access by other users on the same system. (`APL-1`_)

  .. _APL-1: https://github.com/atviriduomenys/spinta/pull/1573

Improvements:

- Introduce synchronization logic part one: Catalog to Agent (`#1488`_).

  .. _#1488: https://github.com/atviriduomenys/spinta/issues/1488


0.2dev6 (2025-10-09)
====================

New Features:

- Added config based OpenAPI generator that creates REST API documentation from manifest. (`#1463`_)


  .. _#1463: https://github.com/atviriduomenys/spinta/issues/1463

Improvements:

- `spinta copy` for XSD supports globally defined attributes, referenced in other places.(`#605`_)
- Updated `authlib` minimal version to 1.0.0 (`#675`_).
- `private` and `public` keys now include `kid` field (`#675`_).
- Added support for scopes following the UDTS format. Now users can use either the UDTS format scopes or the old scopes. If old scopes are used, a deprecation warning is shown. (`#1461`_)
- Introduced a new error handling framework with CLI integration, including error reporting and post-processing in `spinta copy` and `spinta check` (`#1462`_)
- Calling getall with soap backend will always return list of object, even if SOAP response has one element (`#1486`_)
- Introduce `base64()` prepare function that decodes base64 string (`#1486`_)
- `base64()` prepare function that decodes base64 binary now does this outside dask dataframes. Also, raises `InvalidBase64String` for invalid base64 (`#1486`_)
- Added `Cache-Control`, `ETag` and `Last-Modified` headers to most commonly used `GET` requests (`#1506`_).

  .. _#605: https://github.com/atviriduomenys/spinta/issues/605
  .. _#675: https://github.com/atviriduomenys/spinta/issues/675
  .. _#1461: https://github.com/atviriduomenys/spinta/issues/1461
  .. _#1462: https://github.com/atviriduomenys/spinta/issues/1462
  .. _#1486: https://github.com/atviriduomenys/spinta/issues/1486
  .. _#1506: https://github.com/atviriduomenys/spinta/issues/1506

Bug fixes:

- Fixed a bug where `spinta` was trying to connect to a wsdl source during `spinta check` (`#1424`_).

- Fixed `spinta copy` ignores resources without any models (`#1512`_)


  .. _#1512: https://github.com/atviriduomenys/spinta/issues/1512
  .. _#1424: https://github.com/atviriduomenys/spinta/issues/1424


Bug fixes:

- Recognize MySQL BLOB types (TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB) in
  inspect command. Previously, LONGBLOB columns caused TypeError during
  ŠDSA generation (`#1484`_).

  .. _#1484: https://github.com/atviriduomenys/spinta/issues/1484

Other:
- Removed dependency `mypy`


0.2dev5 (2025-09-03)
====================

- Added dependency `mypy`

0.2dev4 (2025-08-28)
====================

New Features:

- Added support for specifying SOAP request body in SOAP requests.(`#1274`_)
- Allow `Client` POST and PATCH endpoints to save variable `backends` that can store
  extra authentication data. Implement `.creds("key")` prepare function to read values
  from saved `backends`. (`#1275`_)
- Implement the skeleton of `spinta sync` command. (`#1378`_)
- Added OpenAPI(and Swagger 2.0) inline schema to DSA conversion `Model` and `ModelProperty` dimensions (`#1260`_) (`#1377`_) (`#1381`_) (`#1382`_) (`#1389`_)
- Refactored `spinta sync` into separate functions to improve readability and maintainability. (`#1415`_)
- During synchronization, create a Data Service and not a Dataset as was done initially. (`#1415`_)
- Adjust synchronization credentials retrieve, to include organization name & type. (`#1415`_)
- Add `spinta inspect` logic to `spinta sync` & loop through all the datasets from inspection instead of using the first one only. (`#1415`_)
- Refactor tests for synchronization to be more maintainable + assert what endpoints are called with and not only that they are called. (`#1415`_)
- Build full dataset name following UDTS conventions. (`#1415`_)
- Remove private source/resource values from DSA. (`#1415`_)
- Added the `spinta admin` command for running maintenance scripts. Unlike `spinta upgrade`, the `admin` command requires
  specific scripts to be passed and cannot run all scripts by default (`#1340`_).
- Added the `changelog` admin script (`spinta admin changelog`). This script checks for duplicate entries in the `changelog`
  (entries with the same local primary key but different global primary keys (`_id`)) and performs a `move` action on them
  to ensure a single active local-global key pair (`#1340`_).
- Change `spinta sync` hierarchy creation to: Data Service -> Dataset -> Distribution. (`#1415`_)
- Sprint Review fixes Part 1: Create data service following the Agent name; Remove distribution creation; Try to retrieve Data service before creating one. (`#1415`_)
- Sprint Review fixes Part 2: Generate dataset name from title or from the last part of dataset column value; Hide `visibility=private` rows; Add the full Dataset name in the DSA. (`#1415`_)
- Sprint Review fixes Part 2.1: Adjust docstrings. (`#1415`_)

  .. _#1274: https://github.com/atviriduomenys/spinta/issues/1274
  .. _#1275: https://github.com/atviriduomenys/spinta/issues/1275
  .. _#1378: https://github.com/atviriduomenys/spinta/issues/1378
  .. _#1260: https://github.com/atviriduomenys/spinta/issues/1260
  .. _#1377: https://github.com/atviriduomenys/spinta/issues/1377
  .. _#1381: https://github.com/atviriduomenys/spinta/issues/1381
  .. _#1382: https://github.com/atviriduomenys/spinta/issues/1382
  .. _#1389: https://github.com/atviriduomenys/spinta/issues/1389
  .. _#1415: https://github.com/atviriduomenys/spinta/issues/1415

Improvements:

- `spinta migrate` now is able to better map `ref` type migrations (`#1230`_).
- Added support for `ruff` linting and code formatting (`#434`_).
- Deobfuscated `SqlAlchemyKeymap` database values, they are no longer hashed (`#1307`_).
- `keymap sync` now supports `move` changelog action (`#1307`_).

Bug fixes:

- Fixed situation where nested properties in `ref` column were giving an error. (`#981`_)
- Fixed a bug where `spinta` didn't work with Python version 3.13 (`#986`_, `#1357`_)
- Updated `pyproj` from version 3.6.1 to 3.7.1 to ensure compatibility with Python version 3.13 (`#1358`_)
- Fixed an error caused by fetching changelog data containing columns no longer declared in the manifest (`#1251`_).
- Introduced `duplicate_warn_only` argument to `keymap` configuration (by default it's disabled). It can be used to supress
  duplicate error. Only use this if necessary and are aware of possible issues (`#1402`_).

  .. _#981: https://github.com/atviriduomenys/spinta/issues/981
  .. _#986: https://github.com/atviriduomenys/spinta/issues/986
  .. _#1357: https://github.com/atviriduomenys/spinta/issues/1357
  .. _#1358: https://github.com/atviriduomenys/spinta/issues/1358

0.2dev3
=======

New Features:

- Added OpenAPI Schema to DSA convertion `Resource` column part (`#1209`_)
- Added OpenAPI Schema to DSA convertion `Param` column part (`#1210`_)
- Added `soap` backend for basic SOAP data reading from WSDL (`#1273`_)
- Added separate `wsdl` backend to read WSDL file and `wsdl(...)` function to link any `soap` type resource with
  `wsdl` type resource for WSDL/SOAP data reading (`#279`_)

  .. _#1209: https://github.com/atviriduomenys/spinta/issues/1209
  .. _#1210: https://github.com/atviriduomenys/spinta/issues/1210
  .. _#1273: https://github.com/atviriduomenys/spinta/issues/1273
  .. _#279: https://github.com/atviriduomenys/spinta/issues/279

Bug fixes:

- Fixed a bug where an error was thrown when nested property was a `ref` followed by a `backref`. (`#1302`_)
- Fixed a bug where `spinta` didn't work with Python versions > 3.10. (`#1326`_)

  .. _#1302: https://github.com/atviriduomenys/spinta/issues/1302
  .. _#1326: https://github.com/atviriduomenys/spinta/issues/1326

0.2dev2
=======

Backwards incompatible:

- added `status`, `visibility`, `eli`, `origin`, `count` and `source.type` columns. (`#1032`_)
- Introduce Python package extras and optional dependencies. Now unicorn, gunicorn (http) and alembic (migrations) wont
  be installed by default. Commands `pip install spinta` and `poetry install` (locally) won't install all packages,
  optional ones (unicorn, gunicorn, alembic) will be skipped and if need should be installed by specifying one/multiple
  of extra group names - `http`, `migrations` or `all`. The last one (`all`) will install all dependencies (like before).
  For local development - `poetry install --all-extras` should be used to install all packages.

  .. _#1032: https://github.com/atviriduomenys/spinta/issues/1032
  .. _#1249: https://github.com/atviriduomenys/spinta/issues/1249

New Features:

- Added OpenAPI Schema manifest (`#1211`_)
- Added changes to support enum `noop()` classificator for copy & check commands (`#1146`_)
- Added OpenAPI Schema to DSA convertion `Dataset` column part (`#1208`_)
- Added new CLI command `getall` which returns JSON representation of YAML data. (`#1229`_)

  .. _#1211: https://github.com/atviriduomenys/spinta/issues/1211
  .. _#1146: https://github.com/atviriduomenys/spinta/issues/1146
  .. _#1208: https://github.com/atviriduomenys/spinta/issues/1208
  .. _#1229: https://github.com/atviriduomenys/spinta/issues/1229

Bug fixes:

- Fixed a bug where namespace (`ns`) dataset name would be placed in the ref column instead of the dataset column (`#1238`_)
- Add missing context to user facing error messages. (`#1196`_)
- Do not check if a declared namespace exists in the generated namespaces (`#1256`_)

  .. _#1238: https://github.com/atviriduomenys/spinta/issues/1238
  .. _#1256: https://github.com/atviriduomenys/spinta/issues/1256
  .. _#1196: https://github.com/atviriduomenys/spinta/issues/1196

0.2dev1
=======

Backwards incompatible:
 - conversion of XSD schemas to DSA manifests in an improved way. (`#842`_)
 - support for language tag for properties. (`#582`_)

  .. _#842: https://github.com/atviriduomenys/spinta/issues/842
  .. _#582: https://github.com/atviriduomenys/spinta/issues/582

0.1.86 (unreleased)
===================

Backwards incompatible:

- To support `redirect`, we introduced a new `API` endpoint `/:move` that creates redirect entries. Because all data
  manipulations must be logged in the `changelog`, we needed a way to indicate that one `_id` was moved to another `_id`.
  Since `_id` is unique and cannot be reused, we added a new property, `_same_as`, used exclusively to track which `_id`
  an entry was moved to. As a result, this property is now included in all tabular results (HTML, ASCII, CSV),
  even though it will typically be empty (`#1290`_).

- In order to add `move` support and to deobfuscate `SqlAlchemyKeymap` new migration system was added. From now on any
  schema changes to keymap should be done using `spinta upgrade`. Keymap now stores separate table called `_migrations`,
  it stores all already executed migrations. Each time `spinta` configures keymap, it will check if all of
  required migrations have been executed on it (`#1307`_).

- The `spinta upgrade` command no longer uses the `-r` argument to specify a script. Instead, you can now pass one or more
  scripts directly as arguments, e.g., `spinta upgrade redirect` or `spinta upgrade clients redirect` (`#1340`_).

Improvements:

- `migrate` command now warns users if there are potential type casting issues (invalid or unsafe).
  Can add `--raise` argument to raise `Exception` instead of warning (only applies to invalid casts, unsafe cast do not
  raise `Exception`, like `TEXT` to `INTEGER`, which potentially can be valid) (`#1254`_).

- The `upgrade` command now support `-c` or `--check` flag, which performs only the script check without executing
  any scripts. This is useful for previewing required upgrades without applying them (`#1290`_).

- Deobfuscated `SqlAlchemyKeymap` database values, they are no longer hashed (`#1307`_).

- `keymap sync` now supports `move` changelog action (`#1307`_).

- The `spinta upgrade` and `spinta admin` commands no longer require the `-r` or `--run` argument to specify scripts.
  Instead, script names can be passed directly as arguments, allowing multiple scripts to be run at once (`#1340`_).

- Reintroduced the legacy `SqlAlchemyKeymap` synchronization mode for models without a primary key.
  This is a temporary workaround until such models are reworked to restrict access to features that require a primary key (`#1340`_).

- Introduced `duplicate_warn_only` argument to `keymap` configuration (by default it's disabled). It can be used to supress
  duplicate error. Only use this if necessary and are aware of possible issues (`#1402`_).

- `keymap sync` now has `--check-all` flag, that allows model dependency checks on models that does not have source set (`#1402`_).

- Reserved models, no longer generate additional meta tables for `postgresql` backend (`#1419`_).

- `spinta migrate` now is able to better map `ref` type migrations (`#1230`_).

- Added support for `ruff` linting and code formatting (`#434`_).

  .. _#434: https://github.com/atviriduomenys/spinta/issues/434
  .. _#1419: https://github.com/atviriduomenys/spinta/issues/1419
  .. _#1254: https://github.com/atviriduomenys/spinta/issues/1254
  .. _#1402: https://github.com/atviriduomenys/spinta/issues/1402
  .. _#1307: https://github.com/atviriduomenys/spinta/issues/1307
  .. _#1230: https://github.com/atviriduomenys/spinta/issues/1230

New Features:

- Added the `spinta admin` command for running maintenance scripts. Unlike `spinta upgrade`, the `admin` command requires
  specific scripts to be passed and cannot run all scripts by default (`#1340`_).

- Added the `changelog` admin script (`spinta admin changelog`). This script checks for duplicate entries in the `changelog`
  (entries with the same local primary key but different global primary keys (`_id`)) and performs a `move` action on them
  to ensure a single active local-global key pair (`#1340`_).

- Added a `redirect` upgrade script (`spinta upgrade redirect`) that checks if the current `backend` supports redirects.
  If not, it will attempt to add the missing features (`#1290`_).

- Added a `deduplicate` admin script (`spinta admin deduplicate`). This checks models with assigned primary keys
  (`model.ref`) to ensure uniqueness is enforced. If not, it scans for duplicates, aggregates them using `model.ref` keys,
  and processes them via the `/:move` endpoint (keeping the oldest entry as the root). It then attempts to enforce
  uniqueness going forward (`#1290`_).

- Implemented `redirect` support. When trying to fetch an entry that no longer exists, the `API` will redirect the request
  if a mapping exists in the `redirect` table (`#1290`_).

- Added `DELETE` `/:move` endpoint, that removes an entry and marks it as moved to another existing entry via
  the `redirect` table (`#1290`_).

  .. _#1290: https://github.com/atviriduomenys/spinta/issues/1290
  .. _#1340: https://github.com/atviriduomenys/spinta/issues/1340

Bug fixes:

- Fixed `migrate` cast not including right column types while generating `USING` code part (`#1254`_).

- Fixed `keymap sync` ignoring `upsert` action (`#1269`_).

- Fixed `postgresql` `update` action updating `_created`, instead of `_updated` value (`#1307`_).

- Fixed an error caused by fetching changelog data containing columns no longer declared in the manifest (`#1251`_).

- Fixed `migration` script sometimes applying name compression twice (`#1409`_).

- Fixed several exponential backtracking regex issues (`#1435`_).

  .. _#1435: https://github.com/atviriduomenys/spinta/issues/1435
  .. _#1409: https://github.com/atviriduomenys/spinta/issues/1409
  .. _#1269: https://github.com/atviriduomenys/spinta/issues/1269
  .. _#1251: https://github.com/atviriduomenys/spinta/issues/1251

0.1.85 (2025-04-08)
===================

Backwards incompatible:

- The `Sql` backend no longer generates random UUIDs whenever `internal` models are being accessed in `external` mode.
  Instead, if a value mapping is not found, an error is raised. The only way to resolve this error is to update `keymap`
  by running `keymap sync` command (`#1214`_).

New Features:

- Added `split('...')` function support to `sql` backend (`#760`_).

- Added `flip('...')` function support in `select` query to `postgresql` and `sql` backends (`#1052`_).

  .. _#1052: https://github.com/atviriduomenys/spinta/issues/1052

Improvements:

- Added `Array` push support for `sql` backend (`#760`_).

  .. _#760: https://github.com/atviriduomenys/spinta/issues/760

- Replaced `from_wkt` and `to_wkt`, to `wkt.loads` and `wkt.dumps`. This will ensure, that older versions of `shapely`
  will still be supported (`#1186`_).

  .. _#1186: https://github.com/atviriduomenys/spinta/issues/1186

- `cast_backend_to_python` now allows extra properties to be passed (custom `select` functions that create new temporary
  properties can now be properly cast to python types) (`#1052`_).

- Better support for `Denorm` properties with `Sql` backend (`#1214`_).

  .. _#1214: https://github.com/atviriduomenys/spinta/issues/1214

- Added a specific `NoModelDefined` error when property is defined without a model (`#1000`_).

  .. _#1000: https://github.com/atviriduomenys/spinta/issues/1000

Bug fixes:

- Fixed `sql` backend not using overwritten `ref` mapping values when joining tables (`#1052`_).

- Fixed `cast_backend_to_python` not propagating casting to `Ref` children (`#1052`_).

- Fixed `cast_backend_to_python` not casting `Denorm` values with required type (`#1052`_).

- Added an additional check for properties that are not given a `type` and the `type` can not be inherited from the base model (`#1019`_).

  .. _#1019: https://github.com/atviriduomenys/spinta/issues/1019

- Adjusted error message for users, for when a DSA has a model with nested properties and the parent node is not defined (`#1005`_)

  .. _#1005: https://github.com/atviriduomenys/spinta/issues/1005

- Fixed tabular reader using `dtype` instead of `raw` type when handling datatype column (`#983`_).

  .. _#983: https://github.com/atviriduomenys/spinta/issues/983

0.1.84 (2025-02-19)
===================

Bug fixes:

- Fixed `SqliteQueryBuilder` importing wrong `Sqlite` class (`#1174`_).

  .. _#1174: https://github.com/atviriduomenys/spinta/issues/1174

0.1.83 (2025-02-18)
===================

Backwards incompatible:

- `sql` backend no longer tries to automatically change it's query functions based on dsn dialect. Now in order to access
  specific dialect's functionality, you need to specify it through type (`#1127`_).

  Currently supported `sql` backend types:
    - `sql` - generic default sql type (tries to use dialect indifferent functions).
    - `sql/postgresql` - PostgreSQL dialect.
    - `sql/mssql` - Microsoft SQL server dialect.
    - `sql/mysql` - MySQL dialect.
    - `sql/mariadb` - MariaDB dialect.
    - `sql/sqlite` - Sqlite dialect.
    - `sql/oracle` - Oracle database dialect.

  It is recommended to specify dialects in the manifest or config, this will ensure better performance and can unlock
  more functionality (in case some dialects support unique functions). Because system no longer tries to automatically
  detect the dialect there is a possibility of errors or invalid values if you do not set the correct dialect.

- `Backend` objects now store `result_builder_class` and `query_builder_class` properties, which can be used to initialize
  their respective builders. This changes how `QueryBuilders` and `ResultBuilders` are now created. Each `Backend` now has
  to specify their builder through `result_builder_type` and `query_builder_type`, which are strings, that map with
  corresponding classes in `config.components` (`#1127`_).

  All `QueryBuilder` classes are stored in `config.components.querybuilders` path.

  Currently there are these builders, that can be used:
    - '' - Empty default query builder.
    - `postgresql` - Internal postgresql query builder.
    - `mongo` - Internal mongo query builder.
    - `sql`- External default sql query builder.
    - `sql/sqlite` - External sqlite dialect query builder.
    - `sql/mssql` - External microsoft sql dialect query builder.
    - `sql/postgresql` - External postgresql dialect query builder.
    - `sql/oracle` - External oracle dialect query builder.
    - `sql/mysql` - External mysql dialect query builder.
    - `sql/mariadb` - External mariadb dialect query builder.
    - `dask` - External Dask dataframe query builder.

  All `ResultBuilder` classes are stored in `config.components.resultbuilders` path.

  Currently there are these builders, that can be used:
    - '' - Empty default result builder.
    - `postgresql` - Internal postgresql result builder.
    - `sql`- External sql result builder.

- In order to maintain cohesiveness in code and data structure, dask backends have gone through same treatment as `sql`.
  Before they worked similar to the new system (users had to manually specify their type), but now to make sure that
  naming convention is same with all components `csv`, `json` and `xml` types have been renamed to `dask/csv`, `dask/json`,
  `dask/xml`. If you used these backends before, you will now need to add `dask/` prefix to their types (`#1127`_).

  Because so many datasets use `csv`, `json` and `xml` types, they will not be fully removed, but they will be deprecated
  and eventually might be removed, so it's encouraged to change them to `dask` format.


New features:

- Added exposed intermediate table support for external `Sql` backend (`#663`_).

  .. _#663: https://github.com/atviriduomenys/spinta/issues/663

Improvements:

- Added better error messages for scalar to ref migrations (when system cannot determine previous primary keys) (`#1123`_).

  .. _#1123: https://github.com/atviriduomenys/spinta/issues/1123

- `export` command now supports `access` argument, that can filter models and properties
  if they are the same or higher level than given `access` (default is `private`, meaning everything is exported) (`#1130`_).

  .. _#1130: https://github.com/atviriduomenys/spinta/issues/1130

- Separated `sql` `backend` dialects to their own separate backends (`#1127`_).

- Added `dask/` prefix to `csv`, `xml` and `json` backends (`#1127`_).

  .. _#1127: https://github.com/atviriduomenys/spinta/issues/1127


Bug fix:

- Convertion from scalar to ref (and ref to scalar) now uses `alias` when there is self reference (`#1105`_).

  .. _#1105: https://github.com/atviriduomenys/spinta/issues/1105

- `spyna` when reading string values and escaping characters now properly restores converted `unicode` characters back
  to `utf-8` encoding, which will allow the use Lithuanian characters in query (`#1139`_).

  .. _#1139: https://github.com/atviriduomenys/spinta/issues/1139


0.1.82 (2025-01-21)
===================

Backwards incompatible:

- `postgresql` `backend` now no longer ignores `prepare` functions. Meaning if there are properties, which has functions
  set in `prepare` column, it can cause errors (if those functions are not supported in `postgresql` `backend`) (`#1048`_).

- `InternalSqlManifest` no longer is capable of knowing when to hide `Text` or `C` language (`#940`_). That means if you have
  `tabular` `manifest` with hidden `Text`, like so:

  .. code-block:: text

    d | r | b | m | property | type    | ref       | access | title
    example                  |         |           |        |
                             |         |           |        |
      |   |   | City         |         | id        |        |
      |   |   |   | id       | integer |           | open   |
      |   |   |   | name@lt  | string  |           | open   |
      |   |   |   | name@en  | string  |           | open   |

  if you were to convert it to `InternalSqlManifest` and back, you would get this result:

  .. code-block:: text

    d | r | b | m | property | type    | ref       | access | title
    example                  |         |           |        |
                             |         |           |        |
      |   |   | City         |         | id        |        |
      |   |   |   | id       | integer |           | open   |
      |   |   |   | name     | text    |           | open   |
      |   |   |   | name@lt  | string  |           | open   |
      |   |   |   | name@en  | string  |           | open   |

New features:

- Added support for `Object` type with `external` `Sql` `backend` (`#973`_).

  .. _#973: https://github.com/atviriduomenys/spinta/issues/973

- Added 'flip` function, which currently only supports `Geometry` type (flips coordinate axis). This features only works
  when reading data, meaning, when writing, you still need to provide coordinates in the right order (`#1048`_).

  .. _#1048: https://github.com/atviriduomenys/spinta/issues/1048

- Added `point` function support to `postgresql` `backend` (`#1053`_).

  .. _#1053: https://github.com/atviriduomenys/spinta/issues/1053

Improvements:

- Client data and `keymap` is now cached. This will reduce amount of file reads with each request (`#948`_).

  .. _#948: https://github.com/atviriduomenys/spinta/issues/948

- `Tabular` `manifest` now supports `Text` type nesting with other complex types (`Object`, `Ref`, etc.) (`#940`_).

  .. _#940: https://github.com/atviriduomenys/spinta/issues/940

0.1.81 (2024-12-17)
===================

Backwards incompatible:

- `SqlAlchemyKeyMap` synchronization no longer uses individual transactions for each synchronization action. Now it
  batches the actions under multiple transactions. By default it batches `10000` rows. In order to change that value,
  set `sync_transaction_size` in `config` under your `keymaps` configuration (`#1011`_).

  Like so:

  .. code-block:: yaml

      keymaps:
        default:
            type: sqlalchemy
            dsn: ...
            sync_transaction_size: 20000

- Changed `postgresql` naming convention. This will result in old tables having incorrect constraint and index names.
  `spinta migrate` should be able to find most of them (`P#153`).

- `AccessLog` no longer stores `scope` field on every request. Instead it will store `token` field (token `JTI` value).
  In order to track what scopes token uses, now we log `auth` requests (`/auth/token`), which will store list of scopes.
  This change should reduce the spam in logging and reduce log file size.

  In order track unique token identifiers, `JTI` field has been added to all new tokens (meaning old tokens, that still
  do not have the field, will not be properly logged) (`#1003`_).

Improvements:

- `SqlAlchemyKeyMap` now uses batch transactions to synchronize data, which greatly improves performance (`#1011`_).

  .. _#1011: https://github.com/atviriduomenys/spinta/issues/1011

- added enum level support, allowing to indicate a level for enum. (`#982`_)

  .. _#982: https://github.com/atviriduomenys/spinta/issues/982

- Standardized `postgresql` naming convention, now all new constraints and indexes should follow same naming
  scheme (`P#153`).

- `spinta migrate` now tries to rename constraints and indexes (if the name only changed) instead of dropping them and
  adding them with correct name (`P#153`).

- `JWT` tokens now also store `JTI` claim (`#1003`_).

- `AccessLog` now has `auth` logging (`#1003`_).

  .. _#1003: https://github.com/atviriduomenys/spinta/issues/1003

Bug fix:

- `Postgresql` `summary` now properly handles tables with long names (`P#160`).

- Fixed various cases where `migrate` command would not take into account truncated names (`P#153`).

0.1.80 (2024-12-03)
===================

Backwards incompatible:

- Keymap synchronization now uses `sync_page_size` config argument to limit amount of data being fetched with a single
  request. This will result in more actions being called to remote server. If `keymap` synchronization takes too long
  to start the process, reduce `sync_page_size` value. Keep in mind, that lower values reduce performance and increase
  server load (`#985`_).

- `push` command now has explicit timeouts set for requests.
  Previously, there were no timeouts set for requests, which meant that execution time was unlimited.
  After the changes the default values are `300` seconds  (5min) for `read` and `5` seconds for `connect` timeouts.
  The timeout values can be adjusted using `--read-timeout` and `--connect-timeout` push command options (`#662`_).

New features:

- Add `-d --datasets` option to migrate command (`#935`_).

  .. _#935: https://github.com/atviriduomenys/spinta/issues/935

- Add `export` cli command, that will export data to specified format (`#960`_).

  .. _#960: https://github.com/atviriduomenys/spinta/issues/960

- Add `keymap sync` command (`#666`_).

  .. _#666: https://github.com/atviriduomenys/spinta/issues/666

- Add `--read-timeout`, `--connect-timeout` options to `spinta push` command (`#662`_).

  .. _#662: https://github.com/atviriduomenys/spinta/issues/662

Improvements:

- Keymap synchronization now uses pagination to fetch data (`#985`_).

  .. _#985: https://github.com/atviriduomenys/spinta/issues/985

0.1.79 (2024-11-12)
===================

New features:

- Added support for `Denorm` type migrations (`#932`_).

  .. _#932: https://github.com/atviriduomenys/spinta/issues/932

Improvements:

- Added better support for migrations with nested data types (`#722`_).

- Added a check for reading client data files, to provide better error messages (`#933`_).

  .. _#933: https://github.com/atviriduomenys/spinta/issues/933

- Added scope information to access logs (`#903`_).

  .. _#903: https://github.com/atviriduomenys/spinta/issues/903

- Improved `summary` `query` memory usage (`#955`_).

  .. _#955: https://github.com/atviriduomenys/spinta/issues/955

Bug fix:

- Resolved ambiguity warning messages (`#895`_).

  .. _#895: https://github.com/atviriduomenys/spinta/issues/895

- Fixed `Denorm` properties being mapped to `Ref` foreign key migrations (`#722`_).

  .. _#722: https://github.com/atviriduomenys/spinta/issues/722

- Fixed memory leak caused by `resource_filename` function (`#954`_).

  .. _#954: https://github.com/atviriduomenys/spinta/issues/954

0.1.78 (2024-10-22)
===================

Bug fix:

- Removed `pymssql` library from requirements (was added in previous version by accident).

0.1.77 (2024-10-22)
===================

Backwards incompatible changes:

- `wait` command no longer raises exceptions, when it fails to connect to backend (`PostgresSql` and `Sql`).
  This means that you will only know if `backend` failed to connect, when you try to call `transaction` or `begin` methods,
  which should be called on every request (`#730`_).

- Changed minimum `starlette` version requirement to `0.40>=` (fixes vulnerability issue).
  More about it: https://github.com/encode/starlette/security/advisories/GHSA-f96h-pmfr-66vw

New features:

- Added support for literal values in `property` `prepare` expression (`#670`_).

  .. _#670: https://github.com/atviriduomenys/spinta/issues/670

- Added uuid data type (`#660`_).

  .. _#660: https://github.com/atviriduomenys/spinta/issues/660

Improvements:

- Added `backend``transaction` and `begin` method validations (`PostgresSql` and `Sql` backends). When launching
  `spinta` server, `wait` command no longer raises exceptions if it failed to connect to backend (`#730`_).

  .. _#730: https://github.com/atviriduomenys/spinta/issues/730

- Added the ability for 'Backref' to have nested properties; improved 'Backref' and 'ArrayBackref' handling (`#664`_).

  .. _#664: https://github.com/atviriduomenys/spinta/issues/664


0.1.76 (2024-10-08)
===================


Backwards incompatible changes:

- You can no longer directly set `Ref` foreign key values to `None`. Meaning you cannot set `"ref": {"_id": None}`.
  Now, if you want to unassign `Ref` value, you have to set it to `None` (`"ref": None`), it will also now set all
  nested values (`Denorm`) to `None` as well, this new feature now ensures, that there cannot be floating `Denorm` values
  when trying to remove references (`#846`_).


Improvements:

- Added removal of duplicate models when converting `XSD` to `DSA` even when `source` is different (`#787`_).

  .. _#787: https://github.com/atviriduomenys/spinta/issues/787

- Improved invalid scope error messaging for token auth (`#537`_).

  .. _#537: https://github.com/atviriduomenys/spinta/issues/537

- Added ability to remove all nested property values for `Ref` type, when assigning `None` to the value itself (`#846`_).


Bug fixes:

- Fixed a bug in XSD->DSA conversion, where properties need to become arrays in a `choice` which has `maxOccurs="unbounded"` (`#837`_).

  .. _#837: https://github.com/atviriduomenys/spinta/issues/837

- Fixed `checksum()` function bug, where it tried to calculate checksums before converting data from `backend` specific to
  python types (`#832`_).

- Fixed an oversight where `geoalchemy2` values were propagated to `prepare_dtype_for_response` instead of being converted to
  `backend` indifferent type (`shapely.geometry.base.BaseGeometry`) (`#832`_).

  .. _#832: https://github.com/atviriduomenys/spinta/issues/832

- Fixed errors when `Ref` changelog values were incorrect. Now, if changelog ref `_id`, or ref itself is `""`, it assumes
  that it is supposed to be `None` (`#556`_).

  .. _#556: https://github.com/atviriduomenys/spinta/issues/556

- Fixed `Ref` value unassignment not updating the values in database (`#846`_).

  .. _#846: https://github.com/atviriduomenys/spinta/issues/846


0.1.75 (2024-09-24)
===================

Improvements:

- Reverted github actions `postgresql` version to `11`, until production server is updated to `16`, so we don't get similar
  issues again (`#827`_).


Bug fixes:

- Fixed `summary` for `Geometry` not working with older than 16 `postgresql` version (`#827`_).

  .. _#827: https://github.com/atviriduomenys/spinta/issues/827


0.1.74 (2024-09-24)
===================

Bug fixes:

- Fixed `api` `inspect` `clean_up` function failing when there are exceptions while reading `manifest` files (`#813`_).

  .. _#813: https://github.com/atviriduomenys/spinta/issues/813

- Fixed `client add` not finding `config_path` when using `config.yml` instead of setting it with `-p` (`#818`_).

  .. _#818: https://github.com/atviriduomenys/spinta/issues/818


0.1.73 (2024-09-19)
===================

Backwards incompatible changes:

- Changed `pymongo` version requirement from `"*"` to `"<=4.8.0"`. Version `4.9.0` changed import paths, that broke `spinta` (`#806`_).

  .. _#806: https://github.com/atviriduomenys/spinta/issues/806

0.1.72 (2024-09-18)
===================

Improvements:

- Added support for negative float values in `starlette` float routing (use `spinta_float` instead of `float` type) (`#781`_).

  .. _#781: https://github.com/atviriduomenys/spinta/issues/781

- Changed `manifests.default.backend` config value from `''` to `'default'`. Now if nothing is set, default backend will be
  `MemoryBackend` instead of nothing (`#798`_).

  .. _#798: https://github.com/atviriduomenys/spinta/issues/798

- Added removal of duplicate models when converting `XSD` to `DSA` (`#752`_).

  .. _#752: https://github.com/atviriduomenys/spinta/issues/752

Bug fixes:

- Fixed `_srid` routing error, when using negative float values as coordinates (`#781`_).

- Fixed `Geometry` boundary check not respecting `SRID` latitude and longitude order (used to always assume, that x = longitude,
  y = latitude, now it will try to switch based on `SRID`) (`#737`_).

  .. _#737: https://github.com/atviriduomenys/spinta/issues/737

- Fixed some errors when trying to access api endpoints, while server is running with default config settings (`#798`_).

- Fixed a problem in `PropertyReader` and `EnumReader` where enums were always added to the top level `property` (`#540`_).

  .. _#540: https://github.com/atviriduomenys/spinta/issues/540

0.1.71 (2024-09-12)
===================

Backwards incompatible:

- Spinta no longer automatically migrates `clients` structure (`#122`_). Now you have to manually use
  `spinta upgrade` command to migrate files. Meaning if there are issues with `clients` file structure you will going to
  get errors, suggesting to fix the problem, or run `spinta upgrade` command (`#764`_).

Improvements:

- Changed `postgresql` github actions and docker compose version to `16-3.4` (`P#129`).

- Changed report bug link to `atviriduomenys@vssa.lt` email (`#758`_).

  .. _#758: https://github.com/atviriduomenys/spinta/issues/758

New features:

- Added `spinta upgrade` command, that will migrate backwards incompatible changes between versions (`#764`_).

  - Use `spinta upgrade` to run all scripts.
  - `spinta upgrade -m <script_name>` to run specific script.
  - `spinta upgrade -f` to skip all checks and forcefully run scripts.
  - `spinta upgrade -d` to run destructive mode, which, depending on script, will override existing changes.
    Only use destructive mode, if you know what will be changed, and you have made backups.

- Added `clients` migrate script to `spinta upgrade` command (`#764`_).
  Main goal is to migrate client files from old structure to newly introduced one in `#122`_ task.

  - You can specify it with `spinta upgrade -r clients` command.
  - Use `spinta upgrade -r clients -f` if you want to make sure that all files are migrated correctly. It will skip
    already migrated files and update `keymap.yml`.
  - `spinta upgrade -r clients -f -d` will override any new files that match old ones. This is destructive and there are
    no rollbacks for it, so only use it if you have backups and understand what will be changed.

  .. _#764: https://github.com/atviriduomenys/spinta/issues/764

Bug fixes:

- Added missing cluster limit to `:summary` for `Geometry` type properties. Now it's set to 25 clusters (`P#130`).


0.1.70 (2024-08-27)
===================

Improvements:

- Improved performance of `PostgreSQL` and `SQL` `backend` `getall` functions (`#746`_).

  .. _#746: https://github.com/atviriduomenys/spinta/issues/746

0.1.69 (2024-08-23)
===================

Improvements:

- Nested properties for XSD. (`#622`_).

  .. _#622: https://github.com/atviriduomenys/spinta/issues/622

Bug fixes:

- Removed `from mypy.dmypy.client import request` import from `spinta/components.py`.

0.1.68 (2024-08-23)
===================

Backwards incompatible:

- Renamed `push_page_size` config field to `default_page_size` (`#735`_).

Improvements:

- Changed default config `sync_page_size` and `default_page_size` parameters to be `100000` instead of `1000` (`#735`_).

New features:

- Added `enable_pagination` config field, which will enable or disable default pagination behaviour. Request and schema
  specifications take priority, meaning even if `enable_pagination` is set to `False`, you can still specify `page(disable:false)`
  to enable it for specific requests (`#735`_).

  .. _#735: https://github.com/atviriduomenys/spinta/issues/735

0.1.67 (2024-08-02)
===================

Backwards incompatible:

- Changed `spinta_sqlite` driver name to `spinta`. Old naming was unnecessary since you needed to use `sqlite+spinta_sqlite:///...`,
  now you can just use `sqlite+spinta:///...` (`#723`_).
- `spinta push` `state` database now will always going to append `sqlite+spinta:///` prefix, instead of `sqlite:///`. This
  ensures, that `sqlite` version is now dependant on `sqlean` library, instead of taking default python `sqlite` version
  (makes it easier to ensure, that users are using correct version of `sqlite`) (`#723`_).
- Changed `sqlalchemy` default `sqlite` driver to `SQLiteDialect_spinta` (instead of `SQLiteDialect_pysqlite`). Meaning
  every time you use `sqlite:///...` it will default to `spinta` driver, instead of `pysqlite` (default `sqlalchemy`) (`#723`_).

Improvements:

- Writing `InternalSQLManifest` now is done using `transaction`, meaning if there are errors, it will rollback any changes
  (This is useful when doing `copy` on already existing structure, since it clears all old data before writing new) (`#715`_).

- Changed `state` db, to always use `spinta` `sqlite` driver (`#723`_).

  .. _#723: https://github.com/atviriduomenys/spinta/issues/723

Bug fixes:

- Fixed `InternalSQLManifest` structure being fetched without index order (`#715`_).

  .. _#715: https://github.com/atviriduomenys/spinta/issues/715

0.1.66 (2024-07-23)
===================

New features:

- Added support for `eq`, `&` and `|` operators to `Dask` `backend` (`#702`_).

  .. _#702: https://github.com/atviriduomenys/spinta/issues/702


Bug fixes:

- Fixed `formula` being ignored when using `inspect` (`#685`_).

  .. _#685: https://github.com/atviriduomenys/spinta/issues/685

- Fixed errors with different formats when returning empty data (`#684`_).

  .. _#684: https://github.com/atviriduomenys/spinta/issues/684

- Fixed `keymap.yml` not updating mapping when changing `client_name` (`#688`_).

  .. _#688: https://github.com/atviriduomenys/spinta/issues/688

- Fixed error when opening `changes` in `html` format, when there is no `select` and you have
  only one language given to `Text` property (`#693`_).

  .. _#693: https://github.com/atviriduomenys/spinta/issues/693

- Fixed assertion error when only selecting not expanded `array` (`#696`_).

  .. _#696: https://github.com/atviriduomenys/spinta/issues/696

- Fixed issue, where sometimes `json` `blank nodes` gets discarded and return empty `dict` (`#699`_).

  .. _#699: https://github.com/atviriduomenys/spinta/issues/696

- Fixed error when trying to use `Dask` `backend` `&` and `|` operators (`#705`_).

  .. _#705: https://github.com/atviriduomenys/spinta/issues/705

0.1.65 (2024-07-03)
===================

Backwards incompatible changes:

- Changed `starlette` version requirement from `"*"` to `">=0.22"`. From version `0.22.0` `starlette` added better
  compatibility support for `AnyIO`.

Bug fixes:

- Fixed `getone` with `jsonl` format (`#679`_)

- Rolled back `Templates` warning fixes (caused errors with older `starlette` versions) (`#679`_)

.. _#679: https://github.com/atviriduomenys/spinta/issues/679

0.1.64 (2024-07-02)
===================

Bug fixes:

- Changed json Geometry type converter import to BaseGeometry (`#673`_)

    .. _#673: https://github.com/atviriduomenys/spinta/issues/673

0.1.63 (2024-06-27)
===================

Backwards incompatible changes:

- When migrating from version of `spinta`, where `push` pagination
  was not supported, to a version, where it is, the old `push state` database
  structure is outdated and it can result in getting `InfiniteLoopWithPagination`
  or `TooShortPageSize` errors (new `push state` database structure now stores pagination values, while old one does not).
  With the addition of (`P#98`) change, you now are able to run `push --sync` command to synchronize `push state` database.
  It is important to note that it will also update pagination values, which could fix some of the infinite loop errors.

- With (`P#98`) change, `internal` will no longer disable pagination when page key types are not supported.
  Before this change, when model's page went through `link` process, if there was any page keys, that were not supported,
  pagination was disabled, no matter what type of backend is used. Since all internal backends support `_id` property,
  which is always present and unique, if we find page keys that are not supported, we can always force pagination using `_id`.
  This results in that all of the requests will now by default going to be sorted by `_id` property.
  Important to note, if we use `sort` with unsupported keys, pagination is still going to be disabled.


New features:

- Mermaid format support for ability to create class diagrams (`#634`_).

  .. _#634: https://github.com/atviriduomenys/spinta/issues/634

- Parametrization support for XML and JSON external backends (`#217`_,
  `#256`_).

  .. _#217: https://github.com/atviriduomenys/spinta/issues/217
  .. _#256: https://github.com/atviriduomenys/spinta/issues/256

- Added new manifest backend for XSD schemas (`#160`_).

  .. _#160: https://github.com/atviriduomenys/spinta/issues/160

- Added `distinct()` function to `model.prepare` (`#579`_).

  .. _#579: https://github.com/atviriduomenys/spinta/issues/579

- Added push state database synchronization. (`P#98`)

- Added `checksum()` `select` function to PostgreSQL backend. (`P#98`)

Improvements:

- Added `ResultBuilder` support to PostgreSQL backend, also changed it's
  `QueryBuilder` to work like external SQL. (`P#98`)

- Changed `internal` backend page assignment logic to default to `_id`
  property, if any of the page keys are not supported. (`P#98`)

- Added proper support for functions in `select()` expressions (`P#100`).

Bug fixes:

- Migrate internal backend changed types (`#580`_).

  .. _#580: https://github.com/atviriduomenys/spinta/issues/580

- Added support for language tags in RDF strings (`#549`_).

  .. _#549: https://github.com/atviriduomenys/spinta/issues/549

- Show values of `text` type in tabular output (`#550`_, `#581`_).

  .. _#550: https://github.com/atviriduomenys/spinta/issues/550
  .. _#581: https://github.com/atviriduomenys/spinta/issues/581

- Added support for PostgreSQL OID type (`#568`_).

  .. _#568: https://github.com/atviriduomenys/spinta/issues/568

- Fixed sorting issue with MySQL and MSSQL external backends (`P#90`).

- Fixed issue with open transactions when writing data (`P#92`).

- Fixed issue with outdated page key in push state tables (`P#95`).

- Words in dataset names separated by underscores. (`#626`__).

  __ https://github.com/atviriduomenys/spinta/issues/626

- Added support for `getone` for `sql` backend (`#513`__).

  __ https://github.com/atviriduomenys/spinta/issues/513

- Fixed Ref id mapping with non-primary keys when primary keys were not initialized (`#653`__).

  __ https://github.com/atviriduomenys/spinta/issues/653

- Fixed issue with Geometry type conversion when pushing data (`#652`__).

  __ https://github.com/atviriduomenys/spinta/issues/652

- Fixed issue with Geometry bounding box check not applying CRS projection (`#654`__).

  __ https://github.com/atviriduomenys/spinta/issues/654


0.1.62 (2024-02-29)
===================

New features:

- Add possibility to update manifest via HTTP API, without restarting server
  (`#479`_).

  .. _#479: https://github.com/atviriduomenys/spinta/issues/479

Bug fixes:

- Fixed error with index names exceeding 63 character limit on PostgreSQL
  (`#566`_).

  .. _#566: https://github.com/atviriduomenys/spinta/issues/566

- Set WGS84 SRID for geometry tupe if SRID is not given as specified in
  documentation (`#562`_).

  .. _#562: https://github.com/atviriduomenys/spinta/issues/562


0.1.61 (2024-01-31)
===================

Backwards incompatible changes:

- Check geometry boundaries (`#454`_). Previously you could publish spatial
  data, with geometries out of CRS bounds, now if your geometry is out of CRS
  bound, you will get error. To fix that, you need to check if you specify
  correct SRID and if you pass geometries according to specified SRID
  specifikation.

  .. _#454: https://github.com/atviriduomenys/spinta/issues/454


New features:

- New type of manifest read from database, this enables live schema updates
  (`#113`_).

  .. _#113: https://github.com/atviriduomenys/spinta/issues/113

- Automatic migrations with `spinta migrate` command, this command compares
  manifest and database schema and migrates database schema, to match given
  manifest table (`#372`_).

  .. _#372: https://github.com/atviriduomenys/spinta/issues/372

- HTTP API for inspect (`#477`_). Now it is possible to inspect data source
  not only from CLI, but also via HTTP API.

  .. _#477: https://github.com/atviriduomenys/spinta/issues/477


Improvements:

- Generate next page only for last object (`#529`_).

  .. _#529: https://github.com/atviriduomenys/spinta/issues/529


Bug fixes:

- Fixing denormalized properties (`#379`_, `#380`_).

  .. _#379: https://github.com/atviriduomenys/spinta/issues/379
  .. _#380: https://github.com/atviriduomenys/spinta/issues/380

- Fix join with base model (`#437`_).

  .. _#437: https://github.com/atviriduomenys/spinta/issues/437

- Fix WIPE timeout with large amounts of related data (`#432`_). This is fixed
  by adding indexes on related columns.

  .. _#432: https://github.com/atviriduomenys/spinta/issues/432

- Fix changed dictionaly size error (`#554`_).

  .. _#554: https://github.com/atviriduomenys/spinta/issues/554

- Fix pagination infinite loop error (`#542`_).

  .. _#542: https://github.com/atviriduomenys/spinta/issues/542



0.1.60 (2023-11-21)
===================

New features:

- Add new `text` type (`#204`_).

  .. _#204: https://github.com/atviriduomenys/spinta/issues/204

Bug fixes:

- Fix client files migration issue (`#544`_).

  .. _#544: https://github.com/atviriduomenys/spinta/issues/544

- Fix pagination infinite loop error (`#542`_).

  .. _#542: https://github.com/atviriduomenys/spinta/issues/542

- Do not sync keymap on models not required for push operation (`#541`_).

  .. _#541: https://github.com/atviriduomenys/spinta/issues/541

- Fix `/:all` on RDF format (`#543`_).

  .. _#543: https://github.com/atviriduomenys/spinta/issues/543


0.1.59 (2023-11-14)
===================

Backwards incompatible changes:

- With addition of new API for client management, structure how client files
  are stored, was changed.

  Previously clients were stored in `SPINTA_CONFIG_PATH` like this::

    clients/
    └── myclient.yml

  Where `myclient` was usually a client name if given, if not given it was
  an UUID.

  Client file content looked like this:

  .. code-block:: yaml

      client_id: myclient
      client_secret: secret
      client_secret_hash: pbkdf2$sha256$346842$yLpG_ganZxGDuwzIsED4_Q$PBAqfikg6rvXzg2_s74zIPlGGilA5MZpyCyTjlEuzfI
      scopes:
        - spinta_getall
        - spinta_getone

  Now `clients/` folder structure looks like this::

    ├── helpers/
    │   └── keymap.yml
    └── id/
        └── 7e/
            └── 1c/
                └── 0625-fd42-4215-bd86-f0ddff04fda1.yml

  In the new structure, all clients are stored under `id/` folder and client
  files are named after client_id uuid form.

  In the example above `7e1c0625-fd42-4215-bd86-f0ddff04fda1` is a `client_id`.

  `client_id` now a clear meaning ant now it is just a client id in UUID form.
  Client name is stored in `client_name`. If client name is not given, then
  `client_name` is the same as `client_id`.

  There is another file called `helpers/keymap.yml`, that looks like this:

  .. code-block:: yaml

      myclient: 7e1c0625-fd42-4215-bd86-f0ddff04fda1

  This file, stores a mapping of client names as an index to help locating
  clients by name faster.

  Client names can change, but id can't.

  Structure of client file mostly stays the same, except `client_id` is not
  only id in UUID form and a new option `client_name` was added to store
  client name. For example content of
  `id/7e/1c/0625-fd42-4215-bd86-f0ddff04fda1.yml` now looks like this:

  .. code-block:: yaml

      client_id: 7e1c0625-fd42-4215-bd86-f0ddff04fda1
      client_name: myclient
      client_secret: secret
      client_secret_hash: pbkdf2$sha256$346842$yLpG_ganZxGDuwzIsED4_Q$PBAqfikg6rvXzg2_s74zIPlGGilA5MZpyCyTjlEuzfI
      scopes:
        - spinta_getall
        - spinta_getone


New features:

- Add possibility to manage clients via API (`#122`_).

  .. _#122: https://github.com/atviriduomenys/spinta/issues/122


Improvements:

- Add better support for denormalized properties (`#397`_).

  .. _#397: https://github.com/atviriduomenys/spinta/issues/397


Bug fixes:

- Fix error on object counting when running `spinta push` (`#535`_).

  .. _#535: https://github.com/atviriduomenys/spinta/issues/535

- Restore recognition of views in `spinta inspect` (`#476`_).

  .. _#476: https://github.com/atviriduomenys/spinta/issues/476

- Fix single object change list rendering in HTML format (`#459`_).

  .. _#459: https://github.com/atviriduomenys/spinta/issues/459


0.1.58 (2023-10-31)
===================

Bug fixes:

- Fix error in CSV containing NULL data (`#528`_).

  .. _#528: https://github.com/atviriduomenys/spinta/issues/528

- Fix `swap()` containing quotes (`#508`_).

  .. _#508: https://github.com/atviriduomenys/spinta/issues/508

- Fix `UnauthorizedKeymapSync` error on `spinta push` command (`#532`_).

  .. _#532: https://github.com/atviriduomenys/spinta/issues/532


0.1.57 (2023-10-24)
===================

New features:

- Add support for array type (`#161`_).

  .. _#161: https://github.com/atviriduomenys/spinta/issues/161

- Add support for backref type (`#96`_).

  .. _#96: https://github.com/atviriduomenys/spinta/issues/96

- Add support for XML resources (`#217`_).

  .. _#217: https://github.com/atviriduomenys/spinta/issues/217

- Add support for JSON resources (`#256`_).

  .. _#256: https://github.com/atviriduomenys/spinta/issues/256

- Add support for CSV resources (`#268`_).

  .. _#268: https://github.com/atviriduomenys/spinta/issues/268


Improvements:

- Add support for custom subject URI in RDF/XML format (`#512`_).

  .. _#512: https://github.com/atviriduomenys/spinta/issues/512


Bug fixes:

- Fixed pagination error with date types (`#516`_).

  .. _#516: https://github.com/atviriduomenys/spinta/issues/516

- Fix issue with old SQLite versions used for keymaps (`#518`_).

  .. _#518: https://github.com/atviriduomenys/spinta/issues/518

- Fix summary bbox function with negative values (`#523`_).

  .. _#523: https://github.com/atviriduomenys/spinta/issues/523


0.1.56 (2023-09-30)
===================

New features:

- Pagination, this should enable possibility to push large amounts of data
  (`#366`_).

  .. _#366: https://github.com/atviriduomenys/spinta/issues/366

- Push models using bases (`#346`_, `#391`_).

  .. _#346: https://github.com/atviriduomenys/spinta/issues/346
  .. _#391: https://github.com/atviriduomenys/spinta/issues/391

- Sync push state from push target (`#289`_).

  .. _#289: https://github.com/atviriduomenys/spinta/issues/289

- Add support for non-primary key refs in push (`#345`_).

  .. _#345: https://github.com/atviriduomenys/spinta/issues/345

- Push models with external dependencies (`#394`_).

  .. _#394: https://github.com/atviriduomenys/spinta/issues/394

- `swap()` function (`#508`_).

  .. _#508: https://github.com/atviriduomenys/spinta/issues/508


0.1.55 (2023-08-18)
===================

New features:

- Summary for numeric and date types (`#452`_).

  .. _#452: https://github.com/atviriduomenys/spinta/issues/452

- Summary for geometry types (`#451`_).

  .. _#451: https://github.com/atviriduomenys/spinta/issues/451

Bug fixes:

- Fixed error on `_id>"UUID"` (`#490`_).

  .. _#490: https://github.com/atviriduomenys/spinta/issues/490


- Fixed an error with unique constraints (`#500`_).

  .. _#500: https://github.com/atviriduomenys/spinta/issues/500


0.1.53 (2023-08-01)
===================

New features:

- Add support for RDF as manifest format (`#336`_).

  .. _#336: https://github.com/atviriduomenys/spinta/issues/336

- Add support for XML as manifest format (`#89`_).

  .. _#89: https://github.com/atviriduomenys/spinta/issues/89

Improvements:

- Delete push target objects in correct order (`#458`_).

  .. _#458: https://github.com/atviriduomenys/spinta/issues/458

Bug fixes:

- Add support for Oracle RAW type (`#493`_).

  .. _#493: https://github.com/atviriduomenys/spinta/issues/493


0.1.52 (2023-06-21)
===================

Improvements:

- Recognize Oracle ROWID data type.


0.1.51 (2023-06-20)
===================

New features:

- Add support for `param` dimension (`#210`_).

  .. _#210: https://github.com/atviriduomenys/spinta/issues/210

- Spinta inspect now supports JSON data as schema source (`#98`_).

  .. _#98: https://github.com/atviriduomenys/spinta/issues/98


Improvements:

- Recognize CHAR and BYTES data types (`#469`_).

  .. _#469: https://github.com/atviriduomenys/spinta/issues/469


- Allow writing data to models with base (`#205`_).

  .. _#205: https://github.com/atviriduomenys/spinta/issues/205


Bug fixes:

- Fix spint push with ref type set to level 3 or below (`#460`_).

  .. _#460: https://github.com/atviriduomenys/spinta/issues/460


- Automatically add unique constraints for all primary keys specified in
  model.ref (`#371`_).

  .. _#371: https://github.com/atviriduomenys/spinta/issues/371



0.1.50 (2023-05-22)
===================

New features:

- Add support for reading data from models with base (`#273`_).

  .. _#273: https://github.com/atviriduomenys/spinta/issues/273

- Add support for `unique` constraints in tabular manifests (`#148`_).

  .. _#148: https://github.com/atviriduomenys/spinta/issues/148

Improvements:

- Much better implementation for updating manifest files from SQL as data
  source (`#364`_).

  .. _#364: https://github.com/atviriduomenys/spinta/issues/364

- Show better error messages on foreign key constraint errors (`#363`_).

  .. _#363: https://github.com/atviriduomenys/spinta/issues/363

- Return a non-zero error code if `spinta push` command fails with an error
  (`#423`_).

  .. _#423: https://github.com/atviriduomenys/spinta/issues/423

- Add support for older SQLite versions (`#411`_).

  .. _#411: https://github.com/atviriduomenys/spinta/issues/411

Bug fixes:

- Correctly handle level 3 references, when referenced model does not have a
  primary key or property references a non-primary key (`#400`_).

  .. _#400: https://github.com/atviriduomenys/spinta/issues/400

- WIPE command now works on tables with long names (`#431`_).

  .. _#431: https://github.com/atviriduomenys/spinta/issues/431


0.1.49 (2023-04-19)
===================

Bug fixes:

- Fix issue with order of axes in geometry properties (`#410`_).

  .. _#410: https://github.com/atviriduomenys/spinta/issues/410


- Fix write operations models containing geometry properties (`#417`_,
  `#418`_).

  .. _#417: https://github.com/atviriduomenys/spinta/issues/417
  .. _#418: https://github.com/atviriduomenys/spinta/issues/418


0.1.48 (2023-04-14)
===================

Bug fixes:

- Fix issue with dask/pandas version incompatibility (`dask#10164`_).

  .. _dask#10164: https://github.com/dask/dask/issues/10164


0.1.47 (2023-03-27)
===================

Improvements:

- Add support for `point(x,y)` and `cast()` functions for sql backend
  (`#407`_).

  .. _#407: https://github.com/atviriduomenys/spinta/issues/407

Bug fixes:

- Error when loading manifest from XLSX file, where level is read as integer
  (`#405`_).

  .. _#405: https://github.com/atviriduomenys/spinta/issues/405



0.1.46 (2023-03-21)
===================

Bug fixes:

- Correctly handle cases, when a weak referece, references a model, that does
  not have primary key specified, in that case `_id` is used as primary key
  (`#399`_).

  .. _#399: https://github.com/atviriduomenys/spinta/issues/399


0.1.45 (2023-03-20)
===================

Improvements:

- Multiple improvements in `spinta push` command (`#311`_):

  - New `--no-progress-bar` option to disable progress bar, this also skips
    counting of rows, which can be slow in some cases, for example when reading
    data from views (`#332`_).

  - New `--retry-count` option, to repeat push operation only with objects that
    ended up in an error on previous push. By default 5 times are retried.

  - New `--max-error-count` option, to stop push operation after specified
    number of errors, by default 50 errors is set.

  - Now instead of sending `upsert`, push became more sofisticated and sends
    `insert`, `patch` or `delete`.

  - If objects were deleted from source, they are also deleted from target
    server.

  - Errors are automatically retried after each push.

  .. _#311: https://github.com/atviriduomenys/spinta/issues/311
  .. _#332: https://github.com/atviriduomenys/spinta/issues/332

- Now it is possible to reference external models, this is done by specifying 3
  or lower data maturity level. When `property.level` is set to 3 or lower for
  `ref` type properties, local values are accepted, testing notes
  `notes/types/ref/external`_ (`#208`_).

  .. _notes/types/ref/external: https://github.com/atviriduomenys/spinta/blob/a3d0157baaa4f82a7a760141a830ca2731b23387/notes/types/ref/external.sh
  .. _#208: https://github.com/atviriduomenys/spinta/issues/208

- Now it is possible to specify `required` properties in `property.type`_
  (`#259`_).

  .. _property.type: https://atviriduomenys.readthedocs.io/dsa/dimensijos.html#property.type
  .. _#259: https://github.com/atviriduomenys/spinta/issues/259

- Specifying SRID for `geometry` type data on writes is no longer required
  (`#330`_).

  .. _#330: https://github.com/atviriduomenys/spinta/issues/330

- Now it is pssible to specify `geometry(geometry)` and `geometry(geometryz)`
  types.

- `base` dimension is now supported in tabular manifest files (`#325`_), but reading and
  writing to models with base is still not fully implemented.

  .. _#325: https://github.com/atviriduomenys/spinta/issues/325

- Support for new `RDF` format was added (`#308`_).

  .. _#308: https://github.com/atviriduomenys/spinta/issues/308


Bug fixes:

- New ascii table formater, that should fix memory issues, when large amounts
  of data are downloaded (`#359`_).

  .. _#359: https://github.com/atviriduomenys/spinta/issues/359

- Fix order logitude and latidude when creatling links to OSM maps (`#334`_).

  .. _#334: https://github.com/atviriduomenys/spinta/issues/334

- Add possibility to explicitly select `_revision` (`#339`_).

  .. _#339: https://github.com/atviriduomenys/spinta/issues/339


0.1.44 (2022-11-23)
===================

Bug fixes:

- Convert a non-WGS coordinates into WGS, before giving link to OSM if SRID is
  not given, then link to OSM is not added too. Also long WKT expressions like
  polygons now are shortened in HTML output (`#298`_).

  .. _#298: https://github.com/atviriduomenys/spinta/issues/298


0.1.43 (2022-11-15)
===================

Improvements:

- Add `pid` (process id) to `request` messages in access log.

Bug fixes:

- Fix recursion error on getone (`#255`_).

  .. _#255: https://github.com/atviriduomenys/spinta/issues/255


0.1.42 (2022-11-08)
===================

Improvements:

- Add support for comments in resources..


0.1.41 (2022-11-08)
===================

Improvements:

- Add support for HTML format in manifest files, without actual backend
  implementing it. (`#318`_).

  .. _#318: https://github.com/atviriduomenys/spinta/issues/318


0.1.40 (2022-11-01)
===================

Improvements:

- Add memory usage logging in order to find memory leaks (`#171`_).

  .. _#171: https://github.com/atviriduomenys/spinta/issues/171

Bug fixes:

- Changes loads indefinitely (`#291`_). Cleaned empty patches, fixed
  `:/changes/<offset>` API call, now it actually works. Also empty patches now
  are not saved into the changelog.

  .. _#291: https://github.com/atviriduomenys/spinta/issues/291

- `wipe` action, now also resets changelog change id.


0.1.39 (2022-10-12)
===================

Bug fixes:

- Correctly handle invalid JSON responses on push command (`#307`_).

  .. _#307: https://github.com/atviriduomenys/spinta/issues/307

- Fix freezing, when XLSX file has large number of empty rows.



0.1.38 (2022-10-03)
===================

Bug fixes:

- Incorrect enum type checking (`#305`_).

  .. _#305: https://github.com/atviriduomenys/spinta/issues/305


0.1.37 (2022-10-02)
===================

New features:

- Check enum value to match property type and make sure, that level is not
  filled for enums.

Bug fixes:

- Correctly handle situation, when no is received from server (`#301`_).

Improvements:

- More informative error message by showing exact failing item (`#301`_).

  .. _#301: https://github.com/atviriduomenys/spinta/issues/301

- Upgrade versions of all packages. All tests pass, but this might introduce
  new bugs.

- Improve unit detection (`#292`_). There was an idea to disable unit checks,
  but decided to give it another try.

  .. _#292: https://github.com/atviriduomenys/spinta/issues/292


0.1.36 (2022-07-25)
===================

New features:

- Add support for HTTP HEAD method (`#240`_).

  .. _#240: https://github.com/atviriduomenys/spinta/issues/240

- Check number of row cells agains header (`#257`_).

  .. _#257: https://github.com/atviriduomenys/spinta/issues/257

Bug fixes:

- Error on getone request with ascii format (`#52`_).

  .. _#52: https://github.com/atviriduomenys/spinta/issues/52



0.1.35 (2022-05-16)
===================

New features:

- Allow to use existing backend with -r option (`#231`_).

  .. _#231: https://github.com/atviriduomenys/spinta/issues/231

- Add non-SI units accepted for use with SI (`#214`_).

  .. _#214: https://github.com/atviriduomenys/spinta/issues/214

- Add `uri` type (`#232`_).

  .. _#232: https://github.com/atviriduomenys/spinta/issues/232


Bug fixes:

- Allow NULL values for properties with enum constraints (`#230`_).

  .. _#230: https://github.com/atviriduomenys/spinta/issues/230


0.1.34 (2022-04-22)
===================

But fixes:

- Fix bug with duplicate `_id`'s (`#228`_).

  .. _#228: https://github.com/atviriduomenys/spinta/issues/228


0.1.33 (2022-04-22)
===================

But fixes:

- Fix `select(prop._id)` bug (`#226`_).

  .. _#226: https://github.com/atviriduomenys/spinta/issues/226


- Fix bug when selecting from two refs from the same model (`#227`_).

  .. _#227: https://github.com/atviriduomenys/spinta/issues/227


0.1.32 (2022-04-20)
===================

New features:

- Add `time` type support (`#223`_).

  .. _#223: https://github.com/atviriduomenys/spinta/issues/223


0.1.31 (2022-04-20)
===================

New features:

- Add support for `geometry` data type in SQL data sources (`#220`_).

  .. _#220: https://github.com/atviriduomenys/spinta/issues/220


0.1.30 (2022-04-19)
===================

Bug fixes:

- Fix `KeyError` issue when joining two tables (`#219`_).

  .. _#219: https://github.com/atviriduomenys/spinta/issues/219


0.1.29 (2022-04-12)
===================

Bug fixes:

- Fix errr on `select(left.right)` when left has multiple references to the same model (`#211`_).

  .. _#211: https://github.com/atviriduomenys/spinta/issues/211

- Fix `geojson` resource type (`#215`_).

  .. _#215: https://github.com/atviriduomenys/spinta/issues/215


0.1.28 (2022-03-17)
===================

Bug fixes:

- Fix error on `select(_id_)` (`#207`_).

  .. _#207: https://github.com/atviriduomenys/spinta/issues/207

- Fix error on `prop._id="..."` (`#206`_).

  .. _#206: https://github.com/atviriduomenys/spinta/issues/206


0.1.27 (2022-03-02)
===================

New features:

- Add support for comments in tabular manifest files.

Bug fixes:

- Fix sql backend join issue, when same table is joined multiple times.

- Fix ref html rendering with null values.

- Fix ref and file rendering on csv and ascii formats.



0.1.26 (2022-02-09)
===================

New features:

- Add `cast()` function for sql backend.

Improvements:

- Do not output resources with `spinta copy --no-source`.


0.1.25 (2022-02-08)
===================

New features:

- Add `spinta token get` command to receive access token using credentials
  from `~/.config/spinta/credentials.cfg` file.

- Add support for prefixes on dataset dimension.

Improvements:

- Show a human readable error message when a property is not found on a sql
  backend.


0.1.24 (2022-01-25)
===================

Backwards incompatible changes:

- Some reserved properties were changed in `:changes` endpoint:

  - `_id` -> `_cid`
  - `_rid` -> `_id`

- `_id` -> `name` was renamed in `:ns` endpoint.

- In `:ns` endpoint `title` is no longer populated with `name` and is empty if
  not explicitly specified.

New features:

- Add `geometry` type support with PostGIS. Now it is possible to store
  spatial data.

- Add `--dry-run` option to `spinta push`. This will run whole push process,
  but does not send data to the target location. Useful for testing push.

- Add `--stop-on-error` option to `spinta push`. This will stop push process
  when first error is encountered.

Refactoring:

- Exporting data to variuos formats and specifically HTML format was heavilly
  refactored. HTML format is mostly rewritten.


0.1.23 (2021-11-18)
===================

Bug fixes:

- Fix `spinta inspect` when updating existing manifest and a `property` with
  the `ref` type has changed.

Refactoring:

- Unify manifest loading and configuration. Now more placed uses unified
  `configure_rc` function for loading and configuring Spinta.

- Add possibility to load manifest from a file stream, without specifying
  file name. Currently this is not yet exposed via CLI interface.


0.1.22 (2021-11-11)
===================

Backwards incompatible changes:

- Refactored accesslog, now accesslog only logs information about a request,
  not a response content. Previously whole response content was logged, which
  created huge log files pretty quickly. Now logs should be a lot smaller.
  But information about each individual object accessed is no longer
  available. (`#97`_)

  .. _#97: https://gitlab.com/atviriduomenys/spinta/-/issues/97

New features:

- Add support for units in `property.ref`.

Improvements:

- `spinta run` no longer requires setting `AUTHLIB_INSECURE_TRANSPORT=1`
  environment variable, it is set internally.

Bug fixes:

- Fix incorrect parsing of `null`, `false` and `true`.


0.1.21 (2021-10-06)
===================

Backwards incompatible changes:

- When returning error messages now `eid` became string, previously it was an
  integer.

New features:

- Add support for enums on datasets.
- Add support for type arguments, for example `geometry(point)`. But
  currently type arguments are not interpreted in any way.
- Added `geojson` to list of supported backend, but actual `geojson` backend
  is not yet implemented.

Improvements:

- When reading manifest from XLSX show tab number.

Bug fixes:

- Fix `copy` command to render `-2` as is, instead of `negative(2)`.
- Fix `enum` with `0` as value, before this fix, `enum` items with `0` was
  ignored.


0.1.20 (2021-09-23)
===================

Backwards incompatible changes:

- Configuration reader now assumes, that a required configuration parameter
  is not given if it is None. Previously zeros, empty strings or lists were
  considered as if required value was not given. But zero or an empty list
  can be a valid given value. Since all tests pass I assume, this change should
  not create any issues.

New features:

- Allow unknown columns to be added at the end of manifest table.

Bug fixes:

- Fix a bug related with dynamic manifest construction from command line
  arguments. Now dynamic manifest uses `inline` manifest type, which now
  supports `sync` parameter. Also simplified code responsible for dynamic
  manifest building.

- Fix a bug on external sql backend in dynamic query construction from related
  models with filters. Bug appeared only of a model had more than one related
  models (`#120`_).

  .. _#120: https://gitlab.com/atviriduomenys/spinta/-/issues/120

- Fix a bug on external sql backend, when select was used with joins to
  related tables.


0.1.19 (2021-08-05)
===================

Backwards incompatible changes:

- Use different push state file for each server (`#110`_). Previously push
  state was stored in `{data_dir}/pushstate.db`, now it is moved to
  `{data_dir}/push/{remote}.db`, where remote is section name without client
  name part from credentials.cfg file. When upgrading, you need to move
  `pushstate.db` manually to desired location. If not moved, you will loose
  you state and all data will be pushed.

  .. _#110: https://gitlab.com/atviriduomenys/spinta/-/issues/110

- Use different location for keymap SQLite database file (`#117`_).
  Previously, by default `keymaps.db` file, was stored in a current working
  directory, but now file was moved to `{data_dir}/keymap.db`. Please move
  `keymaps.db` file to `{data_dir}/keymap.db` after upgrade. By default
  `{data_dir}` is set to `~/.local/share/spinta`.

  .. _#117: https://gitlab.com/atviriduomenys/spinta/-/issues/117

New features:

- Show server error and first item from data chunk sent to server, this will
  help to understand what was wrong in case of an error (`#111`_).

  .. _#111: https://gitlab.com/atviriduomenys/spinta/-/issues/111

- Add `--log-file` and `--log-level` arguments to `spinta` command.

- In HTML format view, show file name and link to a file if `_id` is included
  in the query (`#114`_).

  .. _#114: https://gitlab.com/atviriduomenys/spinta/-/issues/114

- Add support for ASCII manifest files. This makes it easy to test examples
  from tests or documentation. ASCII manifests files must have `.txt` file
  extension to be recognized as ASCII manifest files.

Bug fixes:

- Fix issue with self referenced models, external SQL backend ended up with
  an infinite recursion on self referenced models (`#110`_).

  .. _#110: https://gitlab.com/atviriduomenys/spinta/-/issues/110


0.1.18 (2021-07-30)
===================

Bug fixes:

- Because an incorrect template was used, html format was not outputing
  anything at all. Added a test to actually test what is inside rendered
  html, this should prevent errors like this in future.


0.1.17 (2021-07-29)
===================

New features:

- Add /robots.txt handler. Currently it allows everything for robots and is
  mainly added to avoid error messages in logs about missing robots.txt.

Bug fixes:

- Allow private properties to be used ad `file()` arguments for `file` types.

- When pushing data to remote server, read data as default client, by default

- Previously data was read with admin rights, which caused issues with
  non-open properties being sent to remote, which was refused by remote as
  unknown properties.

- When copying data with `spinta copy --no-source`, also clean `ref`, `source`
  and `prepare` values of `resource` rows.


0.1.16 (2021-07-23)
===================

New features:

- `spinta inspect` now can read an existing manifest file and update it with
  new schema changes made in data source, preserving all manual edits made in
  manifest file. This is not yet fully tested, but does work in simple cases.
  This feature is not yet ready for use in production, because not all manual
  edits in manifest file can be preserved. For example composite foreign keys
  are not yet implemented.

- Add API endpoint `/:check` for checking if manifest table is correct.

- Add `file()` function for reading file data from external datasets.
  Currently this is only implemented for SQL backend.

Bug fixes:

- Now root namespace is always added to manifest event if manifest is empty.
  This fixes 404 error when accessing root namespace on an empty manifest.

- Create default auth client automatically if it does not exists. Currently
  this was enabled only for `spinta inspect` command.


0.1.15 (unreleased)
===================

Actually this version was released, but because of human error, it was the
same as 0.1.14 version.


0.1.14 (2021-04-15)
===================

Backwards incompatible changes:

- `spinta push` command is now unified with other commands and works like
  this `spinta push manifest1 manifest2 target`. Target configuration is moved
  to XDG compatible credentials configuration, state is now saved in a XDG
  directory too, by default. `-r` was replaced with `--credentials`, but by
  default credentials are looked in `~/.config/spinta/credentials` so there is
  no need to specify it. `-c` flag is also no longer exists, you can add
  client to target like this `client@target`, if client is not specified it
  will be read from credentials file.

- Now configuration and data files are stored in a XDG Base Directory
  Specification compatible directories, by default, but can be overridden via
  main config file, environment variables or command line arguments.

Performance improvements:

- Migrated from Earley to LALR(1) parser algorithm and this made formula
  parser 10 times faster, doing write operations involving `_where`, things
  should be about 3-5 times faster. Whole test suite after this because 20%
  faster.

- Moved select list handling out of rows loops and this made lists of objects
  about 5 times faster.

- Enabled server-side cursors for getall actions, now memory consumption is
  always constant even when downloading large amounts of data.

- Fix few bugs in access logging, because of these bugs whole result set was
  consumed at once and stored in memory. This cause delays, when starting to
  download data and also used a lot of memory.


0.1.13 (2021-04-01)
===================

New features:

- Add support for XLSX format for manifest tables (`#79`_).

  .. _#79: https://gitlab.com/atviriduomenys/spinta/-/issues/79

- Add `lang` support in manifest files, now it is possible to describe data
  structures in multiple languages (`#85`_).

  .. _#85: https://gitlab.com/atviriduomenys/spinta/-/issues/85

- Add `spinta pii detect --limit` which is set to 1000 by default.

- Now it is possible to pass AST query form to `_where` for `upsert`,
  `update` and `patch` operations. This improves performance of data sync.

Bug fixes:

- Do a proper `content-type` header parsing to recognize if request is a
  streaming request.

- Fix bug with incorrect type conversion before calculating patch, which
  resulted in incorrect patch, for example with date types (`#94`_).

  .. _#94: https://gitlab.com/atviriduomenys/spinta/-/issues/94


0.1.12 (2021-03-04)
===================

Bug fixes:

- Fix a bug in `spinta push`. It failed when resource was defined on a dataset.


0.1.11 (2021-03-04)
===================

New features:

- Add implicit filters for external sql backend. With implicit filters, now
  you can specify filter on models once and they will be used automatically on
  related models (`#74`_).

  .. _#74: https://gitlab.com/atviriduomenys/spinta/-/issues/74

Bug fixes:

- Fix ref data type in HTML export.


0.1.10 (2021-03-01)
===================

Backwards incompatible changes:

- `choice` type was changed to `enum`.

New features:

- Add `root` config option, to set namespaces, which will be shown on `/`.
  Also this option restricts access only to specified namespace.

- Change ufunc `schema(name)` to `connect(self, schema: name)`.

- Possibility to provide title and description metadata for namespaces
  (`#56`_).

  .. _#56: https://gitlab.com/atviriduomenys/spinta/-/issues/56

- Fix duplicate items in `/:ns/:all` query results (`#23`_).

  .. _#23: https://gitlab.com/atviriduomenys/spinta/-/issues/23

- Add `spinta copy --format-name` option, to reformat names on copy (`#53`_).

  .. _#53: https://gitlab.com/atviriduomenys/spinta/-/issues/53

- Add `spinta copy --output --columns` flags. Now by default `spinta copy`
  writes to stdout instead of a file (`#76`_). `--columns` is only available
  when writing to stdout.

  .. _#76: https://gitlab.com/atviriduomenys/spinta/-/issues/76

- Add `spinta copy --order-by access` flag (`#53`_).

  .. _#53: https://gitlab.com/atviriduomenys/spinta/-/issues/53

- Add `enum` type dimension for properties. This allows to list possible values
  of a property (`#72`_).

  .. _#72: https://gitlab.com/atviriduomenys/spinta/-/issues/72

- Filter data automatically by `enum.access` (`#73`_).

  .. _#73: https://gitlab.com/atviriduomenys/spinta/-/issues/73


0.1.9 (2021-02-01)
==================

- Add `spinta --version`.

- Add `spinta init` command, to initialize empty manifest table.

- Add `spinta show` command, to print manifest table to stdout.

- Backend now became optional and by default manifest is configured without
  manifest.

- `spinta inspect` no longer overwrites existing manifest. By default, manifest
  is printed to stdout. Only if `-o` flag is given, then manifest is written
  into a csv file.


0.1.8 (2021-01-29)
==================

- Fix incorrectly built python packages (`poetry#3610`_).

.. _poetry#3610: https://github.com/python-poetry/poetry/issues/3610


0.1.7 (2021-01-28)
==================

- Fix URL link formatting in HTML output.

- `external.prepare` for Model and Property became `Expr` instead of `dict`.

- `Expr` now has it's own `unparse` and preserves exact expression
  representation.

- `Sql` backend now supports formulas in `select()`. This was only added to
   support composition keys, but also all kinds of formulas in `select()` are
   supported, but not yet implemented.

- `count()` now must be inside `select()`, but only for `Sql` backend.

- `Property.external` no longer can be a list, if you need more than one
  value, use `prepare`. That means, listing multiple items in `source` column
  is no longer supported.


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
  and `_schema/Version` models.

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
  nothing if it finds that `_schema/Version` table is created. But with `yaml`
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

    def test(rc, cli, tmp_path, request):
        create_manifest_files(tmp_path, {
            'country.yml': {
                'type': 'model',
                'name': 'country',
                'properties': {
                    'name': {'type': 'string'},
                },
            },
        })

        rc = rc.fork().add('test', {'manifests.yaml.path': str(tmp_path)})

        cli.invoke(rc, freeze)

        cli.invoke(rc, migrate)

        context = create_test_context(rc)
        request.addfinalizer(context.wipe_all)

        client = create_test_client(context)
        client.authmodel('_version', ['getall', 'search'])

        data = client.get('/_schema/Version').json()

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
