.. default-role:: literal

.. _configuration:

Configuration
#############

Spinta can be configured using multiple configuration soruces. By default these
sources are used in precedence order:

- Command line arguments.

- Environment variables.

- `.env` file.

- Configuration sources provided by extensions via `config` option.

- Default Spinta configuration `spinta.config:CONFIG`.


There is an additional configuration source `config_path`. `config_path` is a
directory where additional configuration files are looked for.


Inspecting configuration
========================

You can inspect current configuration by using following command::

  spinta config

This command will list current configuration values and will also tell source of
origin of each configuration value.

You can filter listed configuration options by providing list of prefixes, for
example::

  spinta config backends manifests

Since Spinta is usually configured using envoronment varialbes, you can show
configuration options names as environemtn variables by adding `-f env`
argument::

  spinta config -f env backends manifests


Configuring spinta
==================

Spinta configuration values are organized in a hierarchy of options. Usually
hierarchy levels are separated by a `.` or by a `__`. `__` is used for
environment variables, since `.` is not allowed in environment variables names.

Configuration options containing suboptions are interpreted as list of
suboption names. For example if we have following configuration::

  backends.pg.type=postgresql
  backends.mongo.type=mongo
  backends.fs.type=fs

`backends` value is a list containing `pg` and `fs`, called keys. If you want to remove
existing keys, you can set `backends`, like this::

  backends=pg,fs

This will remove all configuration options except `backends.pg` and
`backends.fs`. In this case, `backends.mongo` will be removed.


Command line
------------

All spinta commands have `-o` command line argument. With `-o` you can set
configuration values, for example::

  > spinta -o backends.foo.type=mongo config backends
  Origin   Name               Value
  -------  -----------------  -----
  cliargs  backends.foo.type  mongo

`-o` must be use immediately after `spinta` command and before any subcommands.

You can use `-o` multipe times, to set multiple configuraiton options.


Environemtn variables
---------------------

All environemnt variables must use `SPINTA_` prefix and hierarchy levels must
be separated with `__`. For example::

  > SPINTA_BACKENDS__FOO__TYPE=mongo spinta config backends
  Origin   Name               Value
  -------  -----------------  -----
  cliargs  backends.foo.type  mongo


.env file
---------

Spinta tries to riead `.env` file from current directory if such file exists.
`.env` file simply contains list environemnt variables.

Empty lines and lines starting with `#` are ignored.


Additional configuration sources
--------------------------------

After reading configuraiton values from command line arguments, environemnt
variables and `.env` file, Spinta reads additional configuration sources set
via `config` option.

`config` option can contain list of comma separated values. Each value can be a
path fo `.yml` file or it can be a python dotted path like
`myapp.config:CONFIG`, pointing to a dict.

For example we can create and additional configuration file:

.. code-block:: bash

  > cat > /tmp/custom.yml <<EOF
  backends:
    foo:
      type: mongo
  EOF

And use it to configure Spinta::

  > spinta -o config=/tmp/custom.yml config backends
  Origin           Name               Value
  ---------------  -----------------  -----
  /tmp/custom.yml  backends.foo.type  mongo


This way of configuring Spinta is ment for extensions to provide custom default
values, overriding defaults coming from `spinta.config:CONFIG`.


More on configuration
=====================

.. toctree::
   :maxdepth: 1

   auth
   backend
   manifest
