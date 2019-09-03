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
easly changed if needed.

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
token will be generated internaly. But you can pass `creds` argument to
`app.authorize` to get token via `/auth/token`. This will allow to run tests on
any external server and this should work with any existing test.
