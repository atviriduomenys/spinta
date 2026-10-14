.. default-role:: literal

*****
Store
*****

`spinta/components.py:Store` is the in-memory container of everything
that was loaded at server startup: the manifest tree, backends, keymaps
and the access log.

Attributes:

- `manifest` — the main (user provided) `Manifest` instance, with all its
  loaded nodes (see :doc:`/contrib/architecture/concepts/metadata`).
- `internal` — the `InternalManifest` instance — the manifest with
  internal data models (e.g. `_txn`, `_audit`), is always created, even
  when the main manifest is `none`.
- `backends` — dict of `Backend` instances by backend name, e.g.
  `"default"` → `spinta.backends.postgresql.components:PostgreSQL`.
- `keymaps` — dict of `KeyMap` instances by keymap name.
- `accesslog` — the `AccessLog` instance selected by the `accesslog.type`
  config option.
- `config` — a back reference to the runtime `Config` (set during
  loading, see :doc:`/contrib/architecture/configuration/index`).

Creation and loading
********************

A `Store` class is selected from the `components.core.store`
configuration option (default `spinta.components:Store`) and instantiated
in `spinta/core/context.py:create_context()`, together with `Context` and
`Config`. The instance is bound to the context as `store`
(`context.set('store', store)`).

Because `Store` is just a container, its content is loaded by the `load`
command overload `load(Context, Store)` implemented in
`spinta/types/store.py`:

#. keymaps are read from the `keymaps` section of the raw config
   (`rc.keys('keymaps')`), one instance per keymap type;
#. backends are read from the `backends` section of the raw config (see
   :doc:`/contrib/architecture/data-access/backends/index` and :doc:`/contrib/architecture/concepts/component`);
   backends can also be declared in manifests, then they are added by
   `spinta/manifests/helpers.py`;
#. the main manifest is created (and loaded) from the `manifest` config
   option (`spinta/manifests/helpers.py:create_manifest`);
#. the internal manifest (`store.internal`) is created;
#. the access log is loaded (`store.accesslog = commands.load(context,
   config.AccessLog(), config)`).

The `wait(Context, Store)` overload in the same module waits until all
backends are ready (e.g. that the tables exist in the database), and
`check(Context, Config)` is a counterpart that validates that everything
required is configured — it runs where validation is required (e.g.
at API server startup, `spinta/asgi.py`).

Pipeline context
****************

The store loads in the middle of the startup pipeline — after the config
is loaded (`load(Context, Config)`), but before the manifest itself is
fully linked and checked:

- `create_context()` → creates `Context`, `Config` and the empty `Store`;
- `commands.load(context, config)` → config values, registries, ufuncs;
- `commands.load(context, store)` → keymaps, backends, manifest, accesslog;
- `commands.load(context, store.manifest)` + `commands.link(...)`
  + `commands.check(...)` → manifest nodes;
- `commands.wait(context, store)` → poll backends until ready.

See :doc:`/contrib/architecture/wired-together` for the
full pipeline.

At request time, per-request context forks get access to the store via
`context.get('store')` (the same shared object — no copying), e.g. for
`store.manifest.backend` or the keymaps.

External specifications
***********************

`Store` has no direct counterpart in UAPI — it is Spinta's server side
implementation detail. Related external concepts:

- DCAT (https://www.w3.org/TR/vocab-dcat-3/, local extract
  `.tmp/sources/w3c/vocab-dcat-3.md`): the tuple (store.manifest,
  store.backends) corresponds to the dataset/service split of a DCAT
  catalog — `dcat:Catalog` holds `dcat:Dataset` entries, which expose
  `dcat:Distribution` entries backed by concrete data services.
- RFC 6749 (https://www.rfc-editor.org/info/rfc6749, local extract
  `.tmp/sources/ietf/RFC6749.md`): the store holds the reference state of
  the resource server (RFC 6749 section 1.1): the backends and models
  from which protected resources are served, while the access log records
  access to them.

See also
********

- :doc:`/contrib/architecture/configuration/index` — the runtime `Config` object created alongside the
  store.
- :doc:`/contrib/architecture/data-access/backends/backend` — the `Backend` class of the backend instances stored
  in `store.backends`.
- :doc:`/contrib/architecture/concepts/metadata` — what the manifest tree contains.
- :doc:`/contrib/architecture/index` — the architecture map and the
  starting pipeline.
