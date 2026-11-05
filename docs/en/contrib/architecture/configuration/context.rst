.. default-role:: literal

.. py:function:: create_context(name="spinta", rc=None, context=None, args=None, envfile=None)

    The single entry point to create a ``Context`` instance.

    This function:
    
    - Imports all command and ufunc modules listed in ``CONFIG['commands']['modules']`` and ``CONFIG['ufuncs']``
    - Initializes the context with required configuration, loaders, and helpers
    - Returns a fully configured context ready for use in requests or CLI commands

The ``Context`` object is the root of the Spinta request lifecycle, providing access to the store, models, manifests, and other components needed to execute commands and operations.

.. role:: code
    :class: highlight

``Context``
----------

``spinta/components.py:Context`` is Spinta's service locator / state
container. It is passed as the first argument to almost every command and
command overload, and it is the way common services (`Store`, `Config`,
transactions, keymaps, the auth token, ...) are shared by all code acting
on the request.

Creation
********

- The `Context` class used is configurable — selected from the
  `components.core.context` option (default `spinta.components:Context`)
  and instantiated in `spinta/core/context.py:create_context()`, which
  immediately binds the core objects: `context.set('rc', rc)` (see
  :doc:`/contrib/architecture/configuration/rawconfig`), `context.set('config', config)` (see
  :doc:`/contrib/architecture/configuration/index`), `context.set('error_manager', ...)`, and
  `context.set('store', ...)` (see :doc:`/contrib/architecture/configuration/store`).
- In the ASGI server (`spinta/asgi.py`) the same context is loaded with
  the command pipeline (config → store → manifest) at module import time,
  and later served to all request forks via
  `spinta/middlewares.py:ContextMiddleware`.
- In tests the context comes from the `context` fixture
  (`spinta/testing/pytest.py`), which builds
  `spinta/testing/context.py:ContextForTests`; a loaded context is shared
  across the test session (engines allocated by short-lived forks must be
  disposed with `close_test_context_engines` to avoid connection pool
  exhaustion, see :doc:`/contrib/tests`).

Context Lifecycle
=================

Context is a context manager that allows for stateful operations in different request contexts:

* **Forking**: During a request, a forked context is created via ``ContextMiddleware`` (see :doc:`../asgi`). Forking creates an independent state that can be modified without affecting the global context.
* **Python context managers**: ``with context:`` blocks create new state levels, automatically cleaning up resources on exit.
* **Binding vs. Setting**: ``set()`` sets values directly, while ``bind()`` defers computation until first access and caches the result (useful for expensive operations).

``ContextMiddleware``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``spinta/middlewares.py:ContextMiddleware``:

* Creates a fork of the global preloaded context for each incoming request
* Assigns the forked context to ``request.state.context``
* Ensures each request has its own clean context state
* All modifications to the forked context don't affect the global context

``configure_context()``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``spinta/core/context.py:configure_context``:

* Modifies the configuration part of the context (``rc``) by creating a forked ``RawConfig``
* Useful for applying different configurations without affecting the global context

Context Variables Index
========================

Various context variables are set or bound during context initialization and request handling. These variables provide access to core Spinta components throughout the request lifecycle.

Core Variables
--------------
``rc``
    Raw configuration object (``RawConfig``) containing all configuration sources. Initialized in ``create_context()`` and further modified via ``configure_context()``.

``config``
    Resolved configuration component (instance of configuration class loaded from ``CONFIG['components']['core']['config']``). Initialized in ``create_context()``. Provides access to configured values like auth clients, backends, etc.

``store``
    **Store** object containing all essential objects like manifest, backends, access log, etc. Initialized in ``create_context()``. Shared across all requests.

``error_manager``
    **ErrorManager** instance for handling CLI errors. Initialized in ``create_context()``.

``auth.server`` (bound)
    **AuthorizationServer** instance responsible for OAuth 2.0 operations (token creation, introspection, etc.). Bound in ``spinta/api:build()``.

``auth.resource_protector`` (bound)
    **ResourceProtector** instance for validating access tokens and checking scopes. Bound in ``spinta/api:build()``.

``auth.token``
    **Token** object representing the current authenticated user's access token. Set by ``auth_token()`` request handlers. Used for authorization checks throughout the request.

``auth.request``
    **OAuth2Request** object containing the current request's OAuth 2.0 parameters (method, URI, headers, form, query). Set by ``_auth_client_context()`` helper.

Request-Specific Variables
---------------------------
``request.manifest``
    **Manifest** object for the current request. Created in ``spinta/api:build()`` from the store's manifest + request-specific data. Loaded on first request via ``load_for_request()``.

``client`` (set in tests)
    Client test helper (used in test fixtures). Set in test setup.

``request.system`` (set in tests/helpers.py)
    Boolean flag indicating whether a request is a system operation. Used for special handling in dataset backends.

Transaction Variables
---------------------
Transaction context managers are attached to context for request-scoped database operations:

``transaction``
    Generic transaction manager bound for operations on ``manifest.backend``. Used for data migrations and pull/push operations.

``transaction.<backend.name>``
    Backend-specific transaction manager. Attached per backend in CLI helpers and dataset backends.

``transaction.manifest``
    Manifest-specific transaction object (for migration operations). Attached in ``manifests/internal_sql/commands/load.py``.

Access Log Variables
-------------------
``accesslog.stream`` (test only)
    In-memory stream for testing access log capture. Set in test fixtures.

``accesslog`` (attached)
    **AccessLog** instance for logging HTTP requests/responses. Automatically attached via ``context.attach("accesslog", create_accesslog, ...)``.

Utility Variables
-----------------
``rc.fork(config)`` (set in CLI)
    Forked configuration object for CLI environments. Set in ``spinta/cli/pii.py``.

Context Management Features
===========================

Bind
^^^^

Deferred value initialization with caching:

.. code-block:: python

    context.bind('expensive_resource', factory_func, *args, **kwargs)

* Value is cached after first access
* Callable runs only once
* Subsequent uses retrieve from cache

Attach
^^^^^^

Attach context managers for automatically managed resources:

.. code-block:: python

    context.attach('resource_name', factory_func, *args, **kwargs)

* Creates a resource when first accessed
* Resource is automatically cleaned up when context exits (via ``ExitStack``)
* Useful for database connections, temporary files, etc.

For more information about context managers, see:

- `Python Context Management Guide <https://docs.python.org/3/reference/datamodel.html#context-managers>`_
- `contextlib documentation <https://docs.python.org/3/library/contextlib.html>`_

Set
^^^

Direct value assignment:

.. code-block:: python

    context.set('key', value)

* Overwrites existing value
* Immediately available to all contexts looking up ``key``
* Cannot be used for names already bound/attached (raises exception)

Get
^^^

Retrieve a value, resolving bindings and attachments:

.. code-block:: python

    value = context.get('key')

* Returns set value or invokes bound/attached factory
* Caches results in all reachable context levels
* Raises exception if key doesn't exist

Fork
^^^^

Create a child context with independent state:

.. code-block:: python

    with context.fork('child') as child:
        # Independent state from parent
        pass
    # Original context unchanged

* Useful for concurrent operations that need isolated state
* Child context shares parent set/bound values in cache
* Child can modify values without affecting parent

Context State Stack
===================

Each ``Context`` maintains multiple state stacks that track context modifications:

* ``_context`` — Values set in each state
* ``_factory`` — Bound lazy factories in each state
* ``_cmgrs`` — Attached context managers in each state
* ``_names`` — Names defined in each state
* ``_local_names`` — Only names defined in current state (not inherited)
* ``_exitstack`` — ExitStack for managing attached context managers on exit

When creating a fork or entering a ``with`` block:
1. A new state is appended to all stacks (``context.append(...)``)
2. All stacks share a shallow copy from the previous state
3. Local modifications only affect the new state
4. Exiting the block pops the state, reverting changes

This design ensures thread-safety through isolation while preserving cache efficiency across context levels.

States, fork, bind and attach
****************************

A context is a stack of states. Each state inherits values from the
previous one and can override them without affecting earlier states.
There are two ways to create a new state: `with context:` (a new state on
the same instance) and `context.fork(name)` (a new `Context` instance
that's used for concurrent work — this is what `ContextMiddleware` does
for each HTTP request).

State manipulation methods:

- `context.set(name, value)` — set a value directly.
- `context.bind(name, factory, *args, **kwargs)` — bind a callable that
  is invoked (once, per state) when `name` is first accessed — e.g.
  `context.bind('auth.server', AuthorizationServer, context)`.
- `context.attach(name, factory, ...)` — attach a context manager
  factory, activated on first access and closed when the state scope
  ends — e.g. transactions:
  `context.attach('transaction', validate_and_return_transaction,
  context, backend)`.
- `context.get(name)` — get a value (evaluating bounds, activating
  context managers), raising an exception for unknown names.
- `context.has(name, local=False, value=False)` — check whether `name`
  is defined (only in the local state when `local=True`; only when it
  has been evaluated when `value=True`).

Typical request-time context names (set by `spinta/api/__init__.py`):
`context.get('store')`, `context.get('auth.token')`,
`context.get('request.manifest')`, the attached `accesslog` and
`transaction`.

External specifications
***********************

- RFC 6749 (https://www.rfc-editor.org/info/rfc6749, local extract
  `.tmp/sources/ietf/RFC6749.md`): OAuth 2.0 — each request carries an
  access token that the resource server validates (section 7), building a
  per-request authorization context (the client, granted scopes). The
  Spinta context fork plays the same role in code: per-request
  `auth.token`, `accesslog` and authentication state live on it.
- OpenAPI (https://spec.openapis.org/oas/latest.html, local extract
  `.tmp/sources/openapi/OpenAPI.md`): the OpenAPI "Security Scheme"
  declares how a request context is assembled; in Spinta this behavior
  is implemented by the middleware + context fork.

See also
********

- :doc:`/hacking` — the authoritative "Context" section (fork, bind,
  attach semantics).
- :doc:`/contrib/architecture/configuration/store` — the `Store` bound to the context.
- :doc:`/contrib/architecture/index` — how the context is created and used in the
  startup pipeline.
