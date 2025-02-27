.. default-role:: literal

Issues
######

Process
*******

Before starting to code, first describe, what you want to do and how do you
want to do it and then submit task with description for review.

One when task tescription is approved, then start coding.


Definition of done
******************

An issue is considered done when all of the following tasks are done:

- Code review by a colegue is done.

- Automated tests covers new functionality or reproduces a bug, that was fixed.
  See :ref:`writing-tests`.

- Change is added to `CHANGES.rst`_ file.

  .. _CHANGES.rst: https://github.com/atviriduomenys/spinta/blob/master/CHANGES.rst

- Documentation is up to date, we have following documentation pages:

  - `Spinta documentaiton`_ - technical documentation abould Spinta.
  - `Manifest specification`_
    (`old version <Manifest specification (old)_>`_) - DSA specification.
  - `UAPI specification`_ (UDTS) - universal API specificaiton.
  - `Spinta (open data manual)`_ - instructions for users who use Spinta for
    open data.
  - `Open data store`_ (Saugykla) - API service for open data distribution.

- Deployed to the test environment.

- Tested manually on the test environment.

  - Make sure automated manifest validation still works in these repositories:

    | https://github.com/atviriduomenys/manifest
    | https://github.com/ivpk/metadata

- Deployed on a production environment.


Task description
****************

Types
=====

There are following task types:

- `Epic` - large part of a project usually project can be divided into several
  half year or so large task grups or stages, thos should be labed as `epic`.
  An `epic` is diveded into smaller `feature` tasks.

- `Feature` - high level feature description, usually comes from a technical
  specification, shouch tasks are oriented for wider audience ans should not be
  too technical and detailed. A `feature` is diveded into smaller `story`
  tasks.

- `Story` - user story, a task from user perspectives, does not go into
  technical implementation detais, only describes how things should work from
  user perspective. `story` is divided into smaller `task` tasks.

- `Task` - smallest type of a task, describing implementation, who a specific
  `story` will be implemented.

- `Analysis` - a separate task, used for analysis, if needed.



Links
=====

Always provide link to the upper level task, by adding sub-issue to the upper task.

Always provide link to specification task if change is related to specification changes.

Also provide related issues and resources if any.

Example for a `story`:

.. code-block:: plain

    ### Related

    - #52
    - #58

    ### Resources

    - https://example.com/docs.html


Minimal reproducible example
============================

https://en.wikipedia.org/wiki/Minimal_reproducible_example

Always provide inputs and outputs.

Probably most tasks will have to privide a DSA table as an example. Use ASCII
format DSA table, which can be used in tests without modification. For example,
this is a good DSA example it task description::

    ```
    d | r | b | m | property     | type    | ref
    example                      |         |
      |   |   | Country          |         |
      |   |   |   | name@lt      | string  |
      |   |   |   | population   | integer |
      |   |   | City             |         |
      |   |   |   | name@lt      | string  |
      |   |   |   | population   | integer |
      |   |   |   | country      | ref     | Country
    ```

Do not use read, especially private examples, instead always use a fictional
Country/City data model, that reproduces the issue.

Include only necesary columns and metadata to reproduce the issue, in your examples.


When converting DSA to other formats, provide example output in another format from a given DSA exapmle.

When issue is related with API, provide HTTP examples or requests and response, with a given DSA example. For exaple::

    Request

    ```http
    GET /example/City HTTP/1.1
    ```

    Response

    ```http
    HTTP/1.1 200 OK
    Content-Type: application/json

    {
        "_type": "example/City",
        "_id": "631c7a1c-a572-441a-a5f4-ec4d1555eb30",
        "name@lt": "Vilnius",
        "population": 500000,
        "country": {
            "_id": "33d7d11e-2ec2-4e57-b1bb-6155a9c4aa9a"
        }
    }
    ```

If issue is related with command line, provide command line examples.

See examples in notes_ directory, for inspiration on how to privide inputs, outputs and other things, like configuration in order to reproduce the issue.

.. _notes: https://github.com/atviriduomenys/spinta/tree/master/notes


Tracebacks
==========

Provide clean tracebackes by removing:

1. path prefixes up to repository root, in order to hide personal data, like your home directory or to hide real location on server.
2. remove extra lines from traceback, leaving only necesary information, that shows where the error is

For example instead of followint (bad example, don't do this)::

    ```
    Traceback (most recent call last):
      File "/home/me/secret-projects/spinta/.venv/lib/python3.9/site-packages/typer/main.py", line 340, in __call__
        raise e
      File "/home/me/secret-projects/spinta/.venv/lib/python3.9/site-packages/typer/main.py", line 323, in __call__
        return get_command(self)(*args, **kwargs)
      File "/home/me/secret-projects/spinta/.venv/lib/python3.9/site-packages/click/core.py", line 1161, in __call__
        return self.main(*args, **kwargs)
      File "/home/me/secret-projects/spinta/.venv/lib/python3.9/site-packages/typer/core.py", line 743, in main
        return _main(
      File "/home/me/secret-projects/spinta/.venv/lib/python3.9/site-packages/typer/core.py", line 198, in _main
        rv = self.invoke(ctx)
      File "/home/me/secret-projects/spinta/.venv/lib/python3.9/site-packages/click/core.py", line 1697, in invoke
        return _process_result(sub_ctx.command.invoke(sub_ctx))
      File "/home/me/secret-projects/spinta/.venv/lib/python3.9/site-packages/click/core.py", line 1443, in invoke
        return ctx.invoke(self.callback, **ctx.params)
      File "/home/me/secret-projects/spinta/.venv/lib/python3.9/site-packages/click/core.py", line 788, in invoke
        return __callback(*args, **kwargs)
      File "/home/me/secret-projects/spinta/.venv/lib/python3.9/site-packages/typer/main.py", line 698, in wrapper
        return callback(**use_params)
      File "/home/me/secret-projects/spinta/spinta/cli/config.py", line 38, in check
        prepare_manifest(context, ensure_config_dir=True, full_load=True)
      File "/home/me/secret-projects/spinta/spinta/cli/helpers/store.py", line 147, in prepare_manifest
        store = load_manifest(
      File "/home/me/secret-projects/spinta/spinta/cli/helpers/store.py", line 129, in load_manifest
        commands.load(
      File "/home/me/secret-projects/spinta/.venv/lib/python3.9/site-packages/multipledispatch/dispatcher.py", line 279, in __call__
        return func(*args, **kwargs)
      File "/home/me/secret-projects/spinta/spinta/manifests/yaml/commands/load.py", line 110, in load
        commands.load(
      File "/home/me/secret-projects/spinta/.venv/lib/python3.9/site-packages/multipledispatch/dispatcher.py", line 279, in __call__
        return func(*args, **kwargs)
      File "/home/me/secret-projects/spinta/spinta/manifests/tabular/commands/load.py", line 59, in load
        load_manifest_nodes(context, into, schemas, source=manifest)
      File "/home/me/secret-projects/spinta/spinta/manifests/helpers.py", line 137, in load_manifest_nodes
        node = _load_manifest_node(context, config, manifest, source, eid, schema)
      File "/home/me/secret-projects/spinta/spinta/manifests/helpers.py", line 203, in _load_manifest_node
        commands.load(context, node, data, manifest, source=source)
      File "/home/me/secret-projects/spinta/.venv/lib/python3.9/site-packages/multipledispatch/dispatcher.py", line 279, in __call__
        return func(*args, **kwargs)
      File "/home/me/secret-projects/spinta/spinta/types/model.py", line 144, in load
        commands.load(context, model.external, external, manifest)
      File "/home/me/secret-projects/spinta/.venv/lib/python3.9/site-packages/multipledispatch/dispatcher.py", line 279, in __call__
        return func(*args, **kwargs)
      File "/home/me/secret-projects/spinta/spinta/datasets/commands/load.py", line 128, in load
        _check_unknown_keys(entity.model, pkeys, entity.model.properties)
      File "/home/me/secret-projects/spinta/spinta/datasets/commands/load.py", line 116, in _check_unknown_keys
        raise MultipleErrors(
    spinta.exceptions.MultipleErrors: Multiple errors:
     - Property 'address.id' not found.
         Context:
           component: spinta.components.Model
           manifest: default
           schema: Sheet1:13
           model: Address
           entity: None
           property: address.id
    ```

Clean the paths and leave just important parts (correct example)::

    ```python
    Traceback (most recent call last):
      File "spinta/cli/config.py", line 38, in check
        prepare_manifest(context, ensure_config_dir=True, full_load=True)
      File "spinta/cli/helpers/store.py", line 147, in prepare_manifest
        store = load_manifest(
      File "spinta/cli/helpers/store.py", line 129, in load_manifest
        commands.load(
      File "spinta/manifests/yaml/commands/load.py", line 110, in load
        commands.load(
      File "spinta/manifests/tabular/commands/load.py", line 59, in load
        load_manifest_nodes(context, into, schemas, source=manifest)
      File "spinta/manifests/helpers.py", line 137, in load_manifest_nodes
        node = _load_manifest_node(context, config, manifest, source, eid, schema)
      File "spinta/manifests/helpers.py", line 203, in _load_manifest_node
        commands.load(context, node, data, manifest, source=source)
      File "spinta/types/model.py", line 144, in load
        commands.load(context, model.external, external, manifest)
      File "multipledispatch/dispatcher.py", line 279, in __call__
        return func(*args, **kwargs)
      File "spinta/datasets/commands/load.py", line 128, in load
        _check_unknown_keys(entity.model, pkeys, entity.model.properties)
      File "spinta/datasets/commands/load.py", line 116, in _check_unknown_keys
        raise MultipleErrors(
    spinta.exceptions.MultipleErrors: Multiple errors:
     - Property 'address.id' not found.
         Context:
           component: spinta.components.Model
           manifest: default
           schema: Sheet1:13
           model: Address
           entity: None
           property: address.id
    ```

This clean up example is much more readable and hides sensitive inforamtion in you paths.

And don't forget to enable syntax highlightling, which also helps for readability.


Screenshots
===========

It is a good idea to provide some screenshots if we are dealing with graphical
user interfaces.

But do not make screenshots of code, tracebacks or DSA tables, these should be
provided as text, in order to be able to select, copy and search the text.



.. _Spinta documentaiton: https://spinta.readthedocs.io/en/latest/
.. _Manifest specification: https://ivpk.github.io/dsa/
.. _Manifest specification (old): https://atviriduomenys.readthedocs.io/dsa/index.html
.. _Spinta (open data manual): https://atviriduomenys.readthedocs.io/spinta.html
.. _Open data store: https://atviriduomenys.readthedocs.io/api/index.html
.. _UAPI specification: https://ivpk.github.io/uapi/

