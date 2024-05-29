.. default-role:: literal

Changes
#######

0.1.63 (unreleased)
===================

New features:

- Added new manifest backend for XSD schemas. (`#160`__).

  __ https://github.com/atviriduomenys/spinta/issues/160

0.1.62 (2024-02-29)
===================

New features:

- Add possibility to update manifest via HTTP API, without restarting server
  (`#479`__).

  __ https://github.com/atviriduomenys/spinta/issues/479

Bug fixes:

- Fixed error with index names exceeding 63 character limit on PostgreSQL
  (`#566`__).

  __ https://github.com/atviriduomenys/spinta/issues/566

- Set WGS84 SRID for geometry tupe if SRID is not given as specified in
  documentation (`#562`__).

  __ https://github.com/atviriduomenys/spinta/issues/562


0.1.61 (2024-01-31)
===================

Backwards incompatible changes:

- Check geometry boundaries (`#454`__). Previously you could publish spatial
  data, with geometries out of CRS bounds, now if your geometry is out of CRS
  bound, you will get error. To fix that, you need to check if you specify
  correct SRID and if you pass geometries according to specified SRID
  specifikation.

  __ https://github.com/atviriduomenys/spinta/issues/454


New features:

- New type of manifest read from database, this enables live schema updates
  (`#113`__).

  __ https://github.com/atviriduomenys/spinta/issues/113

- Automatic migrations with `spinta migrate` command, this command compares
  manifest and database schema and migrates database schema, to match given
  manifest table (`#372`__).

  __ https://github.com/atviriduomenys/spinta/issues/372

- HTTP API for inspect (`#477`__). Now it is possible to inspect data source
  not only from CLI, but also via HTTP API.

  __ https://github.com/atviriduomenys/spinta/issues/477


Improvements:

- Generate next page only for last object (`#529`__).

  __ https://github.com/atviriduomenys/spinta/issues/529


Bug fixes:

- Fixing denormalized properties (`#379`__, `#380`__).

  __ https://github.com/atviriduomenys/spinta/issues/379
  __ https://github.com/atviriduomenys/spinta/issues/380

- Fix join with base model (`#437`__).

  __ https://github.com/atviriduomenys/spinta/issues/437

- Fix WIPE timeout with large amounts of related data (`#432`__). This is fixed
  by adding indexes on related columns.

  __ https://github.com/atviriduomenys/spinta/issues/432

- Fix changed dictionaly size error (`#554`__).

  __ https://github.com/atviriduomenys/spinta/issues/554

- Fix pagination infinite loop error (`#542`__).

  __ https://github.com/atviriduomenys/spinta/issues/542



0.1.60 (2023-11-21)
===================

New features:

- Add new `text` type (`#204`__).

  __ https://github.com/atviriduomenys/spinta/issues/204

Bug fixes:

- Fix client files migration issue (`#544`__).

  __ https://github.com/atviriduomenys/spinta/issues/544

- Fix pagination infinite loop error (`#542`__).

  __ https://github.com/atviriduomenys/spinta/issues/542

- Do not sync keymap on models not required for push operation (`#541`__).

  __ https://github.com/atviriduomenys/spinta/issues/541

- Fix `/:all` on RDF format (`#543`__).

  __ https://github.com/atviriduomenys/spinta/issues/543


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

- Add possibility to manage clients via API (`#122`__).

  __ https://github.com/atviriduomenys/spinta/issues/122


Improvements:

- Add better support for denormalized properties (`#397`__).

  __ https://github.com/atviriduomenys/spinta/issues/397


Bug fixes:

- Fix error on object counting when running `spinta push` (`#535`__).

  __ https://github.com/atviriduomenys/spinta/issues/535

- Restore recognition of views in `spinta inspect` (`#476`__).

  __ https://github.com/atviriduomenys/spinta/issues/476

- Fix single object change list rendering in HTML format (`#459`__).

  __ https://github.com/atviriduomenys/spinta/issues/459


0.1.58 (2023-10-31)
===================

Bug fixes:

- Fix error in CSV containing NULL data (`#528`__).

  __ https://github.com/atviriduomenys/spinta/issues/528

- Fix `swap()` containing quotes (`#508`__).

  __ https://github.com/atviriduomenys/spinta/issues/508

- Fix `UnauthorizedKeymapSync` error on `spinta push` command (`#532`__).

  __ https://github.com/atviriduomenys/spinta/issues/532


0.1.57 (2023-10-24)
===================

New features:

- Add support for array type (`#161`__).

  __ https://github.com/atviriduomenys/spinta/issues/161

- Add support for backref type (`#96`__).

  __ https://github.com/atviriduomenys/spinta/issues/96

- Add support for XML resources (`#217`__).

  __ https://github.com/atviriduomenys/spinta/issues/217

- Add support for JSON resources (`#256`__).

  __ https://github.com/atviriduomenys/spinta/issues/256

- Add support for CSV resources (`#268`__).

  __ https://github.com/atviriduomenys/spinta/issues/268


Improvements:

- Add support for custom subject URI in RDF/XML format (`#512`__).

  __ https://github.com/atviriduomenys/spinta/issues/512


Bug fixes:

- Fixed pagination error with date types (`#516`__).

  __ https://github.com/atviriduomenys/spinta/issues/516

- Fix issue with old SQLite versions used for keymaps (`#518`__).

  __ https://github.com/atviriduomenys/spinta/issues/518

- Fix summary bbox function with negative values (`#523`__).

  __ https://github.com/atviriduomenys/spinta/issues/523


0.1.56 (2023-09-30)
===================

New features:

- Pagination, this should enable possibility to push large amounts of data
  (`#366`__).

  __ https://github.com/atviriduomenys/spinta/issues/366

- Push models using bases (`#346`__, `#391`__).

  __ https://github.com/atviriduomenys/spinta/issues/346
  __ https://github.com/atviriduomenys/spinta/issues/391

- Sync push state from push target (`#289`__).

  __ https://github.com/atviriduomenys/spinta/issues/289

- Add support for non-primary key refs in push (`#345`__).

  __ https://github.com/atviriduomenys/spinta/issues/345

- Push models with external dependencies (`#394`__).

  __ https://github.com/atviriduomenys/spinta/issues/394

- `swap()` function (`#508`__).

  __ https://github.com/atviriduomenys/spinta/issues/508


0.1.55 (2023-08-18)
===================

New features:

- Summary for numeric and date types (`#452`__).

  __ https://github.com/atviriduomenys/spinta/issues/452

- Summary for geometry types (`#451`__).

  __ https://github.com/atviriduomenys/spinta/issues/451

Bug fixes:

- Fixed error on `_id>"UUID"` (`#490`__).

  __ https://github.com/atviriduomenys/spinta/issues/490


- Fixed an error with unique constraints (`#500`__).

  __ https://github.com/atviriduomenys/spinta/issues/500


0.1.53 (2023-08-01)
===================

New features:

- Add support for RDF as manifest format (`#336`__).

  __ https://github.com/atviriduomenys/spinta/issues/336

- Add support for XML as manifest format (`#89`__).

  __ https://github.com/atviriduomenys/spinta/issues/89

Improvements:

- Delete push target objects in correct order (`#458`__).

  __ https://github.com/atviriduomenys/spinta/issues/458

Bug fixes:

- Add support for Oracle RAW type (`#493`__).

  __ https://github.com/atviriduomenys/spinta/issues/493


0.1.52 (2023-06-21)
===================

Improvements:

- Recognize Oracle ROWID data type.


0.1.51 (2023-06-20)
===================

New features:

- Add support for `param` dimension (`#210`__).

  __ https://github.com/atviriduomenys/spinta/issues/210

- Spinta inspect now supports JSON data as schema source (`#98`__).

  __ https://github.com/atviriduomenys/spinta/issues/98


Improvements:

- Recognize CHAR and BYTES data types (`#469`__).

  __ https://github.com/atviriduomenys/spinta/issues/469


- Allow writing data to models with base (`#205`__).

  __ https://github.com/atviriduomenys/spinta/issues/205


Bug fixes:

- Fix spint push with ref type set to level 3 or below (`#460`__).

  __ https://github.com/atviriduomenys/spinta/issues/460


- Automatically add unique constraints for all primary keys specified in
  model.ref (`#371`__).

  __ https://github.com/atviriduomenys/spinta/issues/371



0.1.50 (2023-05-22)
===================

New features:

- Add support for reading data from models with base (`#273`__).

  __ https://github.com/atviriduomenys/spinta/issues/273

- Add support for `unique` constraints in tabular manifests (`#148`__).

  __ https://github.com/atviriduomenys/spinta/issues/148

Improvements:

- Much better implementation for updating manifest files from SQL as data
  source (`#346`__).

  __ https://github.com/atviriduomenys/spinta/issues/364

- Show better error messages on foreign key constraint errors (`#363`__).

  __ https://github.com/atviriduomenys/spinta/issues/363

- Return a non-zero error code if `spinta push` command fails with an error
  (`#423`__).

  __ https://github.com/atviriduomenys/spinta/issues/423

- Add support for older SQLite versions (`#411`__).

  __ https://github.com/atviriduomenys/spinta/issues/411

Bug fixes:

- Correctly handle level 3 references, when referenced model does not have a
  primary key or property references a non-primary key (`#400`__).

  __ https://github.com/atviriduomenys/spinta/issues/400

- WIPE command now works on tables with long names (`#431`__).

  __ https://github.com/atviriduomenys/spinta/issues/431


0.1.49 (2023-04-19)
===================

Bug fixes:

- Fix issue with order of axes in geometry properties (`#410`__).

  __ https://github.com/atviriduomenys/spinta/issues/410


- Fix write operations models containing geometry properties (`#417`__,
  `#418`__).

  __ https://github.com/atviriduomenys/spinta/issues/417
  __ https://github.com/atviriduomenys/spinta/issues/418


0.1.48 (2023-04-14)
===================

Bug fixes:

- Fix issue with dask/pandas version incompatibility (`dask/dask#10164`__).

  __ https://github.com/dask/dask/issues/10164


0.1.47 (2023-03-27)
===================

Improvements:

- Add support for `point(x,y)` and `cast()` functions for sql backend
  (`#407`__).

  __ https://github.com/atviriduomenys/spinta/issues/407

Bug fixes:

- Error when loading manifest from XLSX file, where level is read as integer
  (`#405`__).

  __ https://github.com/atviriduomenys/spinta/issues/405



0.1.46 (2023-03-21)
===================

Bug fixes:

- Correctly handle cases, when a weak referece, references a model, that does
  not have primary key specified, in that case `_id` is used as primary key
  (`#399`__).

  __ https://github.com/atviriduomenys/spinta/issues/399


0.1.45 (2023-03-20)
===================

Improvements:

- Multiple improvements in `spinta push` command (`#311`__):

  __ https://github.com/atviriduomenys/spinta/issues/311

  - New `--no-progress-bar` option to disable progress bar, this also skips
    counting of rows, which can be slow in some cases, for example when reading
    data from views (`#332`__).

    __ https://github.com/atviriduomenys/spinta/issues/332

  - New `--retry-count` option, to repeat push operation only with objects that
    ended up in an error on previous push. By default 5 times are retried.

  - New `--max-error-count` option, to stop push operation after specified
    number of errors, by default 50 errors is set.

  - Now instead of sending `upsert`, push became more sofisticated and sends
    `insert`, `patch` or `delete`.

  - If objects were deleted from source, they are also deleted from target
    server.

  - Errors are automatically retried after each push.

- Now it is possible to reference external models, this is done by specifying 3
  or lower data maturity level. When `property.level` is set to 3 or lower for
  `ref` type properties, local values are accepted, testing notes
  `notes/types/ref/external`_ (`#208`__).

  .. _notes/types/ref/external: https://github.com/atviriduomenys/spinta/blob/a3d0157baaa4f82a7a760141a830ca2731b23387/notes/types/ref/external.sh

  __ https://github.com/atviriduomenys/spinta/issues/208

- Now it is possible to specify `required` properties in `property.type`_
  (`#259`__).

  .. _property.type: https://atviriduomenys.readthedocs.io/dsa/dimensijos.html#property.type

  __ https://github.com/atviriduomenys/spinta/issues/259

- Specifying SRID for `geometry` type data on writes is no longer required
  (`#330`__).

  __ https://github.com/atviriduomenys/spinta/issues/330

- Now it is pssible to specify `geometry(geometry)` and `geometry(geometryz)`
  types.

- `base` dimension is now supported in tabular manifest files (`#325`__), but reading and
  writing to models with base is still not fully implemented.

  __ https://github.com/atviriduomenys/spinta/issues/325

- Support for new `RDF` format was added (`#308`__).

  __ https://github.com/atviriduomenys/spinta/issues/308


Bug fixes:

- New ascii table formater, that should fix memory issues, when large amounts
  of data are downloaded (`#359`__).

  __ https://github.com/atviriduomenys/spinta/issues/359

- Fix order logitude and latidude when creatling links to OSM maps (`#334`__).

  __ https://github.com/atviriduomenys/spinta/issues/334

- Add possibility to explicitly select `_revision` (`#339`__).

  __ https://github.com/atviriduomenys/spinta/issues/339


0.1.44 (2022-11-23)
===================

Bug fixes:

- Convert a non-WGS coordinates into WGS, before giving link to OSM if SRID is
  not given, then link to OSM is not added too. Also long WKT expressions like
  polygons now are shortened in HTML output (`#298`__).

  __ https://github.com/atviriduomenys/spinta/issues/298


0.1.43 (2022-11-15)
===================

Improvements:

- Add `pid` (process id) to `request` messages in access log.

Bug fixes:

- Fix recursion error on getone (`#255`__).

  __ https://github.com/atviriduomenys/spinta/issues/255


0.1.42 (2022-11-08)
===================

Improvements:

- Add support for comments in resources..


0.1.41 (2022-11-08)
===================

Improvements:

- Add support for HTML format in manifest files, without actual backend
  implementing it. (`#318`__).

  __ https://github.com/atviriduomenys/spinta/issues/318


0.1.40 (2022-11-01)
===================

Improvements:

- Add memory usage logging in order to find memory leaks (`#171`__).

  __ https://github.com/atviriduomenys/spinta/issues/171

Bug fixes:

- Changes loads indefinitely (`#307`__). Cleaned empty patches, fixed
  `:/changes/<offset>` API call, now it actually works. Also empty patches now
  are not saved into the changelog.

  __ https://github.com/atviriduomenys/spinta/issues/291

- `wipe` action, now also resets changelog change id.


0.1.39 (2022-10-12)
===================

Bug fixes:

- Correctly handle invalid JSON responses on push command (`#307`__).

  __ https://github.com/atviriduomenys/spinta/issues/307

- Fix freezing, when XLSX file has large number of empty rows.



0.1.38 (2022-10-03)
===================

Bug fixes:

- Incorrect enum type checking (`#305`__).

  __ https://github.com/atviriduomenys/spinta/issues/305


0.1.37 (2022-10-02)
===================

New features:

- Check enum value to match property type and make sure, that level is not
  filled for enums.

Bug fixes:

- Correctly handle situation, when no is received from server (`#301`__).

  __ https://github.com/atviriduomenys/spinta/issues/301

Improvements:

- More informative error message by showing exact failing item (`#301`__).

  __ https://github.com/atviriduomenys/spinta/issues/301

- Upgrade versions of all packages. All tests pass, but this might introduce
  new bugs.

- Improve unit detection (`#292`__). There was an idea to disable unit checks,
  but decided to give it another try.

  __ https://github.com/atviriduomenys/spinta/issues/292


0.1.36 (2022-07-25)
===================

New features:

- Add support for HTTP HEAD method (`#240`__).

  __ https://github.com/atviriduomenys/spinta/issues/240

- Check number of row cells agains header (`#257`__).

  __ https://github.com/atviriduomenys/spinta/issues/257

Bug fixes:

- Error on getone request with ascii format (`#52`__).

  __ https://github.com/atviriduomenys/spinta/issues/52



0.1.35 (2022-05-16)
===================

New features:

- Allow to use existing backend with -r option (`#231`__).

  __ https://github.com/atviriduomenys/spinta/issues/231

- Add non-SI units accepted for use with SI (`#214`__).

  __ https://github.com/atviriduomenys/spinta/issues/214

- Add `uri` type (`#232`__).

  __ https://github.com/atviriduomenys/spinta/issues/232


Bug fixes:

- Allow NULL values for properties with enum constraints (`#230`__).

  __ https://github.com/atviriduomenys/spinta/issues/230


0.1.34 (2022-04-22)
===================

But fixes:

- Fix bug with duplicate `_id`'s (`#228`__).

  __ https://github.com/atviriduomenys/spinta/issues/228


0.1.33 (2022-04-22)
===================

But fixes:

- Fix `select(prop._id)` bug (`#226`__).

  __ https://github.com/atviriduomenys/spinta/issues/226


- Fix bug when selecting from two refs from the same model (`#227`__).

  __ https://github.com/atviriduomenys/spinta/issues/227


0.1.32 (2022-04-20)
===================

New features:

- Add `time` type support (`#223`__).

  __ https://github.com/atviriduomenys/spinta/issues/223


0.1.31 (2022-04-20)
===================

New features:

- Add support for `geometry` data type in SQL data sources (`#220`__).

  __ https://github.com/atviriduomenys/spinta/issues/220


0.1.30 (2022-04-19)
===================

Bug fixes:

- Fix `KeyError` issue when joining two tables (`#219`__).

  __ https://github.com/atviriduomenys/spinta/issues/219


0.1.29 (2022-04-12)
===================

Bug fixes:

- Fix errr on `select(left.right)` when left has multiple references to the same model (`#211`__).

  __ https://github.com/atviriduomenys/spinta/issues/211

- Fix `geojson` resource type (`#215`__).

  __ https://github.com/atviriduomenys/spinta/issues/215


0.1.28 (2022-03-17)
===================

Bug fixes:

- Fix error on `select(_id_)` (`#207`__).

  __ https://github.com/atviriduomenys/spinta/issues/207

- Fix error on `prop._id="..."` (`#206`__).

  __ https://github.com/atviriduomenys/spinta/issues/206


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
  available. (`#97`__)

  __ https://gitlab.com/atviriduomenys/spinta/-/issues/97

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
  models (`#120`__).

  __ https://gitlab.com/atviriduomenys/spinta/-/issues/120

- Fix a bug on external sql backend, when select was used with joins to
  related tables.


0.1.19 (2021-08-05)
===================

Backwards incompatible changes:

- Use different push state file for each server (`#110`__). Previously push
  state was stored in `{data_dir}/pushstate.db`, now it is moved to
  `{data_dir}/push/{remote}.db`, where remote is section name without client
  name part from credentials.cfg file. When upgrading, you need to move
  `pushstate.db` manually to desired location. If not moved, you will loose
  you state and all data will be pushed.

  __ https://gitlab.com/atviriduomenys/spinta/-/issues/110

- Use different location for keymap SQLite database file (`#117`__).
  Previously, by default `keymaps.db` file, was stored in a current working
  directory, but now file was moved to `{data_dir}/keymap.db`. Please move
  `keymaps.db` file to `{data_dir}/keymap.db` after upgrade. By default
  `{data_dir}` is set to `~/.local/share/spinta`.

  __ https://gitlab.com/atviriduomenys/spinta/-/issues/117

New features:

- Show server error and first item from data chunk sent to server, this will
  help to understand what was wrong in case of an error (`#111`__).

  __ https://gitlab.com/atviriduomenys/spinta/-/issues/111

- Add `--log-file` and `--log-level` arguments to `spinta` command.

- In HTML format view, show file name and link to a file if `_id` is included
  in the query (`#114`__).

  __ https://gitlab.com/atviriduomenys/spinta/-/issues/114

- Add support for ASCII manifest files. This makes it easy to test examples
  from tests or documentation. ASCII manifests files must have `.txt` file
  extension to be recognized as ASCII manifest files.

Bug fixes:

- Fix issue with self referenced models, external SQL backend ended up with
  an infinite recursion on self referenced models (`#113`__).

  __ https://gitlab.com/atviriduomenys/spinta/-/issues/110


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

- Add support for XLSX format for manifest tables (`#79`__).

  __ https://gitlab.com/atviriduomenys/spinta/-/issues/79

- Add `lang` support in manifest files, now it is possible to describe data
  structures in multiple languages (`#85`__).

  __ https://gitlab.com/atviriduomenys/spinta/-/issues/85

- Add `spinta pii detect --limit` which is set to 1000 by default.

- Now it is possible to pass AST query form to `_where` for `upsert`,
  `update` and `patch` operations. This improves performance of data sync.

Bug fixes:

- Do a proper `content-type` header parsing to recognize if request is a
  streaming request.

- Fix bug with incorrect type conversion before calculating patch, which
  resulted in incorrect patch, for example with date types (`#85`__).

  __ https://gitlab.com/atviriduomenys/spinta/-/issues/94


0.1.12 (2021-03-04)
===================

Bug fixes:

- Fix a bug in `spinta push`. It failed when resource was defined on a dataset.


0.1.11 (2021-03-04)
===================

New features:

- Add implicit filters for external sql backend. With implicit filters, now
  you can specify filter on models once and they will be used automatically on
  related models (`#74`__).

  __ https://gitlab.com/atviriduomenys/spinta/-/issues/74

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
  (`#56`__).

  __ https://gitlab.com/atviriduomenys/spinta/-/issues/56

- Fix duplicate items in `/:ns/:all` query results (`#23`__).

  __ https://gitlab.com/atviriduomenys/spinta/-/issues/23

- Add `spinta copy --format-name` option, to reformat names on copy (`#53`__).

  __ https://gitlab.com/atviriduomenys/spinta/-/issues/53

- Add `spinta copy --output --columns` flags. Now by default `spinta copy`
  writes to stdout instead of a file (`#76`__). `--columns` is only available
  when writing to stdout.

  __ https://gitlab.com/atviriduomenys/spinta/-/issues/76

- Add `spinta copy --order-by access` flag (`#53`__).

  __ https://gitlab.com/atviriduomenys/spinta/-/issues/53

- Add `enum` type dimension for properties. This allows to list possible values
  of a property (`#72`__).

  __ https://gitlab.com/atviriduomenys/spinta/-/issues/72

- Filter data automatically by `enum.access` (`#73`__).

  __ https://gitlab.com/atviriduomenys/spinta/-/issues/73


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

- Fix incorrectly built python packages (`python-poetry/poetry/issues/3610`__).

__ https://github.com/python-poetry/poetry/issues/3610


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
