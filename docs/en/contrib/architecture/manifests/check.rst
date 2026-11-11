.. default-role:: literal

#####
Check
#####

`spinta check` is a CLI command that loads configuration and manifests and
validates them. Internally it runs the `check` multiple-dispatch command
(see :doc:`/contrib/architecture/multiple-dispatch/commands`) on the
components that were loaded: the configuration, the manifest, every node
defined in the manifest (namespaces, datasets, models, scopes, properties,
data types, enum items) and the backend used by each model.

This page describes how `check` works, how the traversal starts from
`Store` and reaches individual `DataType` objects, what is checked
besides the manifest, and how new checks should be added.


Running the command
*******************

.. code-block:: sh

    spinta check [MANIFESTS]... [--mode internal] [--check-names]

The command is defined in `spinta/cli/config.py:check()` and registered in
`spinta/cli/main.py`. Its steps are:

#. `configure_context()` creates a context with the manifest paths, `mode`
   and `check_names` options (`--check-names` sets the `check.names`
   config option, see below).
#. `prepare_manifest(context, ensure_config_dir=True, full_load=True)`
   loads the configuration, the store, the manifest (`load`, `link`) and
   then runs `check` on the configuration and on the manifest. These calls
   are described in :ref:`check-traversal` below.
#. For each model that has an external resource with a backend, the
   command runs `commands.check(context, model, backend)`.
#. Errors collected by the error manager are printed with a summary
   (`CLIErrorHandler.post_process()`). If there are no errors, `OK` is
   printed.

Other code paths run the same checks. `spinta/asgi.py` calls
`commands.check` on the config and on the manifest on API startup,
`spinta/cli/helpers/store.py:load_config()` and `load_manifest()` run them
before most CLI commands, and `spinta/testing/manifest.py` runs them in
tests. This matters for backwards compatibility (see
:ref:`check-opt-in`).


How errors are reported
***********************

Check functions report problems in one of two ways:

- **Raised exceptions** (for example `raise InvalidName(...)`,
  `raise MissingRequiredProperty(...)`) stop the check at the first error.
- **Handled errors** are passed to the error manager with
  `context.get("error_manager").handle_error(error)`
  (`spinta/handlers.py`). They are counted per file and the check keeps
  going. Used for example for `InlineEnumWithName` in
  `spinta/types/model.py` and `DaskBackendCompareNotSupported` in
  `spinta/datasets/backends/dataframe/commands/check.py`.

The `ErrorManager` is created by `create_context()` in
`spinta/core/context.py`. With `re_raise=True` every handled error is raised
immediately instead (see `tests/test_handlers.py`).


.. _check-traversal:

Traversal
*********

The `check` command has many overloads, and each overload checks its own
node and then calls `commands.check` on the child nodes. The traversal
therefore is a tree walk, which starts at the configuration and the store
and ends at the data types. The overloads are listed in the order they are
reached.

Store, configuration and manifest
=================================

`Store` (`spinta/components.py:Store`) is the container that owns
`config`, `keymaps`, `backends`, `manifest`, `internal` and `accesslog`.
Its `load` command (`spinta/types/store.py`) loads all of them from
configuration. Note that there is **no** `check(Context, Store)` overload,
even though the `check` command docstring (`spinta/commands/__init__.py`)
lists it. The store is traversed by the callers instead:

#. `check(Context, Config)` — `spinta/types/config.py`. Verifies that
   `default_auth_client` exists in the clients directory.
#. `check(Context, Manifest)` — `spinta/manifests/commands/check.py`.
   Iterates over the node groups returned by `get_manifest_object_names()`
   (`ns`, `dataset`, `model`, see `MetaDataContainer` in
   `spinta/manifests/components.py`), gets every node with
   `commands.get_nodes()` and calls `check()` on it. After that it verifies
   that every model named in the `models` configuration section exists in
   the manifest, otherwise `ModelNotFound` is raised.
#. `check(Context, Model, Backend)` — for each model with
   `model.external.resource.backend`, called from `spinta/cli/config.py`
   (see :ref:`check-backends`).

Namespaces and datasets
=======================

- `check(Context, Namespace)` (`spinta/types/namespace.py`): when
  `check.names` is enabled, validates the namespace name.
- `check(Context, Dataset)` (`spinta/datasets/commands/check.py`): checks
  every resource with `commands.check(context, resource)`, then validates
  the dataset name when `check.names` is enabled.
- `check(Context, Resource)` (`spinta/datasets/commands/check.py`): no-op.

Models
======

`check(Context, Model)` (`spinta/types/model.py`) is the main node check:

#. validates the model name (`check_model_name()`, only with `check.names`);
#. requires an `_id` property (`MissingRequiredProperty` otherwise);
#. checks every `Scope` of the model;
#. checks every `Property` of the model;
#. checks that properties listed in the `models.<name>.properties`
   configuration exist in the model (`PropertyNotFound`);
#. rejects a `ref` type with properties used as a primary key of an
   external model (`NotImplementedFeature`).

Scopes
======

`check(Context, Scope)` (`spinta/types/model.py`) validates the scope name
with `check_scope_name()` when `check.names` is enabled.

Properties
==========

`check(Context, Property)` (`spinta/types/model.py`):

#. validates the property name with `check_property_name()` (only with
   `check.names`);
#. reports `InlineEnumWithName` for inline enums that have a name;
#. for every item of `prop.enum` calls
   `commands.check(context, item, prop.dtype, item.prepare)`, see
   `check(Context, EnumItem, DataType, value)` in
   `spinta/dimensions/enum/commands.py`. There are overloads per data type
   (`Integer`, `Boolean`, `String`, and fallbacks for `NoOp`, `Expr`,
   `None` and `NotAvailable`) that verify the enum value matches the type;
#. calls `commands.check(context, prop.dtype)` — the data type check.

Data types
==========

The last step is `commands.check(context, prop.dtype)`. The dispatcher
selects the overload that matches the concrete `DataType` class
(`spinta/types/datatype.py`). Overloads for the base `DataType` class exist
so that every type is covered, and a type without a more specific overload
falls back to them (it is a no-op):

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - data type
     - what is checked
   * - `DataType` (base)
     - nothing, it is the default for all types.
   * - `Base32`
     - allowed only on `_id` and `_revision` properties.
   * - `Array` (`spinta/types/array/check.py`)
     - checks the item property (`dtype.items`); for intermediate-table
       arrays, the left and right mapping properties must be `ref` types
       pointing to the right models.
   * - `Object` (`spinta/types/object/check.py`)
     - checks each sub-property.
   * - `Ref` (`spinta/types/ref/check.py`)
     - checks each sub-property of a `ref` with properties.
   * - `ExternalRef`
     - checks sub-properties and that they have the same type as the
       matching `refprops` of the referenced model.
   * - `Text` (`spinta/types/text/check.py`)
     - checks each language property.
   * - `Denorm` (`spinta/types/denorm/check.py`)
     - rejects denormalized properties that refer to a ref property listed
       in the parent `ExternalRef`'s `refprops`.
   * - `Inherit` (`spinta/types/inherit/check.py`)
     - the property must be defined in the base model, unless it is a
       reserved property.

The traversal can be summarized as:

.. code-block:: text

    spinta check
    ├── check(Config)                          spinta/types/config.py
    ├── check(Manifest)                        spinta/manifests/commands/check.py
    │   ├── check(Namespace)                   spinta/types/namespace.py
    │   ├── check(Dataset)                     spinta/datasets/commands/check.py
    │   │   └── check(Resource)
    │   └── check(Model)                       spinta/types/model.py
    │       ├── check(Scope)
    │       └── check(Property)
    │           ├── check(EnumItem, DataType, value)
    │           └── check(DataType)            spinta/types/*/check.py
    │               └── check(Property)       (nested: Object, Ref, Array items, Text langs)
    └── check(Model, Backend)                  spinta/cli/config.py
        └── check(Model, DaskBackend)          spinta/datasets/backends/dataframe/commands/check.py


.. _check-backends:

Backends
********

Backends are not traversed from the store. They are checked per model, by
the CLI loop in `spinta/cli/config.py`, which calls
`commands.check(context, model, model.external.resource.backend)`.

The overloads that exist today are:

- `check(Context, Model, Backend)` (`spinta/types/model.py`): no-op. It is
  the fallback for every backend.
- `check(Context, Model, DaskBackend)`
  (`spinta/datasets/backends/dataframe/commands/check.py`): reports
  `DaskBackendCompareNotSupported` when the model's `prepare` expression
  uses comparison operators (`COMPARE`), which the Dask backend does not
  support.

Other backends (PostgreSQL, memory, and the other dataset backends) have
no `check` overloads of their own, so they fall back to the no-op
`(Context, Model, Backend)` overload. Backend configuration is read when the
backend is loaded (`load_backend()` in `spinta/backends/helpers.py`), and a
missing required parameter raises `RequiredConfigParam`. A backend-specific
check is added by registering a `check` overload for `(Context, Model,
<Backend>)` or `(Context, Backend)`.


Adding a new check
******************

To add a new check:

#. Register an overload of `check` for the node or data type it validates.
   Use the most specific class that is correct, and call `commands.check`
   on child nodes so that the walk reaches them.
#. Report problems with the exceptions from `spinta/exceptions.py` or by
   calling `error_manager.handle_error()` when the check should continue.
#. Add the check to the traversal described above, if it is for a new node
   type.

.. _check-opt-in:

Checks are opt-in
=================

`check` runs on API startup, and on every CLI command that loads a
manifest. A new check that rejects manifests which are valid today would
break existing deployments and CI pipelines. For backwards compatibility,
**new checks must be opt-in and disabled by default**, and users must be
able to enable them when needed.

The existing `check.names` option is the model to follow:

- it is defined in `spinta/config.yml` under `check`, with `names: boolean`;
- its default is `False` in `spinta/types/config.py`
  (`config.check_names = rc.get("check", "names", default=False)`);
- the CLI option `spinta check --check-names` sets it (`spinta manifest copy`
  also sets it implicitly, unless `--format-names` or `--rename-duplicates`
  is given); it can also be set in configuration or with the
  `SPINTA_CHECK__NAMES=true` environment variable;
- the check function returns early when the option is disabled, see
  `check_model_name()` in `spinta/types/helpers.py`.

A new check should follow the same pattern: add a boolean option under
`check` in `spinta/config.yml`, read it into `Config` in
`spinta/types/config.py` with default `False`, and return early from the
check when it is disabled. Enabling the check then needs no code change,
only configuration.


See also
********

- :doc:`/contrib/architecture/manifests/index` — the manifest component
  overview, where `check` is run after load and link.
- :doc:`/contrib/architecture/configuration/config` — the `Config` component
  and how configuration options are read.
- :doc:`/contrib/architecture/configuration/store` — the `Store` component.
- :doc:`/contrib/architecture/multiple-dispatch/commands` — how `commands`
  overloads are registered and dispatched.
- :doc:`/contrib/architecture/wired-together` — the startup pipeline that
  runs `check` on the API.
