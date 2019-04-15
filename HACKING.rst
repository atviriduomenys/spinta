Development environment
=======================

You can develop either on local machine or using docker

Docker
------

Run docker-compose::

   docker-compose up

If migrations are not applied, app will crash. To launch migrations::

   docker-compose run --rm app spinta migrate
   # and restart the app
   docker-compose start app

In case PostgreSQL server will startup sooner than the app and app will crash - just restart the app when PostgreSQL is ready::

   docker-compose start app


Local machine
-------------

Create database::

   createdb spinta

Create configuration files::

   SPINTA_BACKENDS_DEFAULT_DSN=postgresql:///spinta
   SPINTA_MANIFESTS_DEFAULT_PATH=path/to/manifest

Prepare environment::

   make

Run database migrations::

   env/bin/spinta migrate


Import some datasets::

   env/bin/spinta pull gov/vrk/rinkejopuslapis.lt/kandidatai
   env/bin/spinta pull gov/lrs/ad


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

      config                              (Config)

         commands[]                       (str)

         backends
            [default]                     (BackendConfig)
               type                       (str)
               dsn                        (str)

         manifests:
            [default]
               path                       (pathlib.Path)

         ignore[]                         (str)

         debug                            (bool)

      backends
         [backend]                        (Backend)

      manifests
         [ns]                             (Manifest)
            path                          (pathlib.Path)
            objects

               ['model']
                  [object]                (Model)
                     properties
                        [property]        (Property)
                           type           (Type)

               ['project']
                  [object]                (Project)
                     objects
                        [object]          (ProjectModel)
                           properties
                              [property]  (ProjectProperty)

               ['dataset']
                  [object]                (Dataset)
                     objects
                        [object]          (Object)
                           properties
                              [property]  (Property)
                                 type     (Type)

               ['owner']
                  [object]                (Owner)

   Node
      parent                              (Node)
      manifest                            (Manifest)

   Type
      name                                (str)

   EnvVars
      environ

   File
      path


Commands
========

::

   'load' - convert primitive serialized form to a python-native form
      prepare(Date)           -> (old) spinta/types/property.py:PrepareDate
                                 (new) spinta/types/commands.py:@command('load', datetime.date, str)
                              -> (new) spinta/types/commands.py:@command('load', datetime.datetime, str)
      load(Config)            -> (old) spinta/types/config.py:LoadConfig
                              -> (new) spinta/config/commands.py:@command('load', Config, EnvVars)
                              -> (new) spinta/config/commands.py:@command('load', Config, EnvFile)
                              -> (new) spinta/config/commands.py:@command('load', Config, CfgFile)
      load(Config)            -> (old) spinta/types/config.py:LoadBackends
                              -> (new) spinta/config/commands.py:@command('load', Backend, Config)
      load(Config)            -> (old) spinta/types/config.py:LoadManifests
                              -> (new) spinta/config/commands.py:@command('load', Manifest, Config)
      load(Manifest)          -> (old) spinta/types/manifest.py:ManifestLoadManifest
                                 (new) spinta/manifest/commands.py:@command('load', Manifest, YmlFile)
      load(Node)              -> (old) spinta/types/type.py:ManifestLoad
                                 (new) spinta/manifest/commands.py:@command('load', Node, dict)
      load(Project)           -> (old) spinta/types/project.py:LoadProject
                                 (new) spinta/projects/commands.py:@command('load', Project, dict)
      load(Dataset)           -> (old) spinta/types/dataset.py:LoadDataset
                                 (new) spinta/datasets/commands.py:@command('load', Dataset, dict)
      load(Object)            -> (old) spinta/types/object.py:LoadObject
      load(PostgreSQL)        -> (old) spinta/backends/postgresql/__init__.py:LoadBackend
                                 (new) spinta/backends/postgresql/commands.py:@command('load', PostgreSQL, Config)

   'dump' - convert python-native objects to primitive serialized form
                              -> (new) spinta/types/commands.py:@command('dump', datetime.date)
                              -> (new) spinta/types/commands.py:@command('dump', datetime.datetime)

         self.run(self.config, {'manifest.load.backends': None}, ns='internal')

   'prepare': {},
      prepare(Node)           -> (old) spinta/types/type.py:Prepare
      prepare(Command)        -> (old) spinta/types/command.py:Prepare
      prepare(CommandList)    -> (old) spinta/types/command.py:PrepareCommandList

         self.run(self.load(obj, {'prepare': {'obj': self.obj, 'prop': name, 'value': value}})

   'prepare.type': {},
      prepare(Config)         -> (old) spinta/types/config.py:PrepareConfig
      prepare(Manifest)       -> (old) spinta/types/manifest.py:PrepareManifest
      prepare(Node)           -> (old) spinta/types/type.py:PrepareType
      prepare(Dataset)        -> (old) spinta/types/dataset.py:PrepareDataset
      prepare(Project)        -> (old) spinta/types/project.py:PrepareProject
      prepare(Object)         -> (old) spinta/types/object.py:PrepareObject

         self.run(self.config, {'prepare.type': None}, ns='internal')

   'backend.prepare': {},
      prepare(Manifest)                   -> (old) spinta/types/manifest.py:BackendPrepare
      prepare(Manifest, PostgreSQL)       -> (old) spinta/backends/postgresql/__init__.py:Prepare
      prepare(Model, PrepareModel)        -> (old) spinta/backends/postgresql/__init__.py:PrepareModel
      prepare(DatasetModel, PostgreSQL)   -> (old) spinta/types/manifest.py:Prepare

         self.run(manifest, {'backend.prepare': None}, ns=name)

   'backend.migrate': {},
      migrate(Manifest)                   -> (old) spinta/types/manifest.py:BackendMigrate
      migrate(Manifest, PostgreSQL)       -> (old) spinta/backends/postgresql/__init__.py:Migrate

         self.run(manifest, {'backend.migrate': None}, ns=name)

   'manifest.check': {},
      check(Manifest)         -> (old) spinta/types/manifest.py:CheckManifest
      check(Model)            -> (old) spinta/types/model.py:CheckModel
      check(Project)          -> (old) spinta/types/project.py:CheckProject
      check(Dataset)          -> (old) spinta/types/object.py:CheckDataset
      check(Owner)            -> (old) spinta/types/owner.py:CheckOwner
      check(Object)           -> (old) spinta/types/object.py:CheckObject
      check(ForeignKey)       -> (old) spinta/types/property.py:RefManifestCheck

         self.run(manifest, {'manifest.check': None}, ns=name)

   'check': {},
      check(Version, Model, PostgreSQL)            -> (old) spinta/backends/postgresql/__init__.py:CheckModel
      check(Version, DatasetModel, PostgreSQL)     -> (old) spinta/backends/postgresql/dataset.py:Check

         self.run(model, {'check': {'transaction': transaction, 'data': data}}, backend=backend, ns=ns)

   'push': {},
      push(Version, Model, PostgreSQL)             -> (old) spinta/backends/postgresql/__init__.py:Push
      push(Version, DatasetModel, PostgreSQL)      -> (old) spinta/backends/postgresql/dataset.py:Push

         self.run(model, {'push': {'transaction': transaction, 'data': data}}, backend=backend, ns=ns)

   'pull': {},
      pull(Dataset)           -> (old) spinta/types/dataset.py:Pull

         self.run(dataset, {'pull': params}, backend=None, ns=ns)

   'get': {},
   'getall': {},
   'changes': {},
   'wipe': {},

   'csv': {
   'html': {
   'xml': {
   'pdf': {
   'hint': {
   'xlsx': {
   'json': {

   'export.asciitable': {
   'export.csv': {
   'export.json': {
   'export.jsonl': {

   'replace': {},
   'range': {
   'self': {},
   'chain': {},
   'all': {},
   'list': {
   'denormalize': {},
   'unstack': {},
   'url': {
   'getitem': {

   'serialize': {},
