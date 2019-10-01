.. default-role:: literal

Development environment
=======================

You can use docker-compose to run dependant services for spinta development::

   docker-compose up -d

If migrations are not applied, spinta will crash. To launch migrations::

   env/bin/spinta migrate

You can run the app using::

   make run


System dependencies
-------------------

Archlinux::

   sudo pacman -S --needed docker docker-compose $(pacman -Sgq base-devel)


Diretory tree
=============

::

   spinta/
      config/
         components.py
         commands.py
      backends/
         postgresql/
            commands.py
            services.py
         mongo/
            commands.py
            services.py
         commands.py
         services.py
      manifest/
         components.py
         commands.py
      types/
         components.py
         commands.py
      validation/
         commands.py
      commands.py

Components
==========

Inheritance::

   Store

   Config
   BackendConfig

   Manifest

   Node
      Model
      Project
         ProjectModel
         ProjectProperty
      Dataset
         Resource
         DatasetModel
         DatasetProperty
      Owner

   Backend
      Python
      PostgreSQL
      MongoDB

   Type
      Integer
      String
      Number
      ForeignKey
      PrimaryKey

   EnvVars

   File
      EnvFile
      CfgFile
      YmlFile


Composition::

   Store

     config                            (Config)

       commands[]                      (str)

       backends
         [default]                     (BackendConfig)
           type                        (str)
           dsn                         (str)

       manifests:
         [default]
           path                        (pathlib.Path)

       ignore[]                        (str)

       debug                           (bool)

     backends
       [backend]                       (Backend)

     manifests
       [ns]                            (Manifest)
         path                          (pathlib.Path)
         objects

            ['model']
              [name]                   (Model)
                properties
                  [name]               (Property)
                    type               (Type)

            ['project']
              [name]                   (Project)
                objects
                  [name]               (ProjectModel)
                    properties
                      [name]           (ProjectProperty)

            ['dataset']
              [name]                   (Dataset)
                resources
                  [name]               (Resource)
                    objects
                      [name]           (Object)
                         properties
                           [name]      (Property)
                             type      (Type)

            ['owner']
              [name]                   (Owner)

   Node
     parent                            (Node)
     manifest                          (Manifest)

   Type
     name                              (str)

   EnvVars
     environ

   File
     path


Testing
=======

Authorization
-------------

Here is example how to test endpoints with authorization:


.. code-block:: python

   def test(app):
      app.authorize(['spinta_model_action'])
      resp = app.get('/some/endpoint')
      assert resp.status_code == 200

When `app.authorize` is called, client
`tests/config/clients/baa448a8-205c-4faa-a048-a10e4b32a136.yml` credentials are
are used to create access token and this access token is added as
`Authorization: Bearer {token}` header to all requests.

If `app.authorize` is called without any arguments, scopes are taken from
client YAML file. If scopes are given, then the given scopes are used, even if
client's YAML file does not have those scopes.

Access token is created using `tests/config/keys/private.json` key and
validated using `tests/config/keys/public.json` key.

Additional clients can be created using this command::

   spinta client add -p tests/config/clients

But currently `app.authorize` does not support using another client, currently
only `baa448a8-205c-4faa-a048-a10e4b32a136` is always used, but that can be
easily changed if needed.

By default `app.authorize` will not call `/auth/token` endpoint to get access
token, because access token is generated internally giving access to all
requested scopes, even if client does not have those scopes. If you want to get
token by calling `/auth/token`, then you need to pass `creds` argument, like
this:

.. code-block:: python

   app.authorize([scopes, ...], creds=(client_id, client_secret))

Then token will net be generated and real `/auth/token` endpoint will be
called.


Run a test on a real server
--------------------------

It is possible to reuse any tests using `app` fixture and run that test on a
real server. All you need to do is this:

.. code-block:: python

   def test(app):
      app.start_session('http://127.0.0.1:8000')  # <-- add this line
      app.authorize(['spinta_model_action'])
      resp = app.get('/some/endpoint')
      assert resp.status_code == 200

By default, it is assumed, then you are using local server (`make run`) and
token will be generated internally. But you can pass `creds` argument to
`app.authorize` to get token via `/auth/token`. This will allow to run tests on
any external server and this should work with any existing test.


Context
-------

In Spinta `context` is used to pass variables that are commonly used in
multiple places.

Also `context` is passed as first argument to most commands and it is also used
to override commands, because each project using Spinta should have its own
`context` class. Which context class should be used is defined in
`components.core.context` configuration parameter. By default `context` class
is set to `spinta.components:Context`.

In tests, `spinta.testing.context:ContextForTests` class is used. But in tests,
it is made sure, that `ContextForTests` always extends whatever is set in
`components.core.context` configuration parameter.

In tests, you get `context` from fixture called `context`. This fixture is
defined in `spinta.testing.pytest`.

Context is loaded on Starlette startup, startup handler is defined in
`spinta.api:create_app_context`. In tests, context is loaded at the start of
test session and this loaded context is reused in all tests.

Once context is loaded, it has a global state which is kept through whole
process life time.

For each http request, context is forked via
`spinta.middlewares:ContextMiddleware`.  Forked context inherits all values
defined in global state, but all values are copied to make it thread-safe.

Context has states. Each state inherits all values from a previous state. Each
state can be modified, without effecting previous states. There are two ways,
how to create new sates: forking and activating new state with `with`
statement.

Here is an example how forking works:

.. code-block:: python

    base = Context('base')
    with base.fork('fork') as fork:
        ...

Here `fork` context has a new state and also inherits all values from `base`.

Here is an example how to create new state using `with` statement:

.. code-block:: python

    context = Context('base')
    with context:
        with context:
            ...

Using `with` statement same context instance can be reused, to create new
states. This way of creating new states is not thread safe.

All this is needed to have isolated context states. When new context state is
created, you can add new values or override inherited ones without affecting
previous states. Currently this is mostly used to initialize context at startup
which is quite expensive operation, because we need to read configuration, load
manifest YAML files, etc. And once we have this base state, we can run each
http request under new state inheriting everything from base.

Context can be manipulated using these methods:

- `context.set(name, value)` - set a value in context directly.

  All directly set values are always copied between forks. Copies are shallow,
  that means, in order to ensure thread safety, you should only read values,
  bet do not change them. If you need to change values, then use `attach`
  instead and construct values on request. This way, each new fork fill call
  bound factory to get fresh values.

- `context.bind(name, factory, *args, **kwargs)` - bind a callable `factory` to
  get value. This factory will be called on first `name` access and then
  retrieved value is cached in current and on a previous state were it was
  bound. In case of a fork, factories are always called and cached in each fork
  separately, to ensure thread safety.

- `context.attach(name, factory, *args, **kwargs)` - attach a context manager
  factory to current state. This context manager factory is activated on first
  `name` access and is deactivated when current context scope ends. Attach
  works pretty much the same way as bind.

- `context.get(name)` - access value of given `name`, if `name` points to a
  factory, then factory will be called to get value, if `name` points to
  context manager, then context manager will be activated.

- `context.has(name, local=False, value=False)` - check if `name` is defined.
  If `local` is true, check if `name` was defined in current state, if `value`
  is true, check if `name` was has value (means factory is called or context
  manager is activated).


Here are some examples:

.. code-block:: python

    context = Context('base')
    context.set('a', 1)
    with context:
        context.get('a')      # returns: 1
        context.set('a', 2)
        context.get('a')      # returns: 2
        context.set('a', 3)   # error, 'a' was already set in this scope

    context.get('a')          # returns: 1, base value was restored


.. code-block:: python

    def f():
        return 42

    context = Context('base')
    context.bind('a', f)
    with context:
        context.get('a')      # returns 42, `f` is called and value is cache in
                              # current scope
        context.get('a')      # returns 42, `f` is not called

    context.get('a')          # returns 42, `f` is not called
