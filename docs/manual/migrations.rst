.. default-role:: literal

Migrations
##########

There are two types of migrations, schema and data migrations. Schema
migrations are responsible for applying changes to you schema, data migrations
are responsible for updating data when you found some kind of error in data.

For better visibility all migrations are stored as part of manifest YAML files.
All migrations of a model can be found in the same model YAML file.

Model YAML file is a multi-document_ file and documents are organized this way:

.. _multi-document: https://yaml.org/spec/1.2/spec.html#id2800132

.. code-block:: yaml

  ---
  # Current model schema
  ---
  # Initial model version
  ---
  # Second model version
  ---
  ...

First document in YAML file is always latest model schema with all the changes
applied ant possibly new changes that are net yet freezed into model's version
history.


Creating new model
------------------

When you want to add a new model, you need to start by writing schema like
this:

.. code-block:: yaml

  ---
  type: model
  name: country
  properties:
    name:
      type: string

Currently this model is in draft mode, it will not be used and will not be
visible. To make this model available for use you need to freeze it:

.. code-block:: sh

  spinta freeze

`freeze` command will freeze all drafts and creates new schema version. You can
freeze only one model:

.. code-block:: sh

  spinta freeze country

After freezing model YAML file is updated an now looks like this:

.. code-block:: yaml

  ---
  type: model
  name: country
  version: aee17c1f-f16c-4d7c-ac45-d7ed9c7ac771
  properties:
    name:
      type: string
  ---
  version:
    id: aee17c1f-f16c-4d7c-ac45-d7ed9c7ac771
    date: 2020-02-12 07:14
  changes:
    - {op: add, path: /type, value: model}
    - {op: add, path: /name, value: country}
    - {op: add, path: /version, value: aee17c1f-f16c-4d7c-ac45-d7ed9c7ac771}
    - {op: add, path: /properties, value: {name: string}}
  migrate:
    - type: schema
      upgrade: >-
        create_table(
          country,
          column(_id, uuid4(), primary_key: true),
          column(_revision, string(), unique: true),
          column(name, string(), nullable: true),
        )
      downgrade: >-
        drop_table(country)

Current model schema updated with new version and now model is ready to use.

In new version document we see all the information aboult changes. In `changes`
parameter we see what exactly was changed in schema document and in `migrate`
we see all the steps that will be performed on database.

Different backends might generate different migration steps. For example Mongo
backend will not have any steps, because Mongo is schemaless database.


Running migrations
------------------

But before using this model we need to apply migrations:

.. code-block:: sh

  spinta migrate

This command will run migration steps and finally will register new schema
version in the internal `_schema` table.


Changing existing model
-----------------------
