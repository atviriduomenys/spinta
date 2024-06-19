.. default-role:: literal

Logging
#######

Here you can find information about access logging. Access log, will track
which API endpoints are accessed, it will not log details about accessed data
objects (rows).


Configuring logging
===================

Access log is controlled by `accesslog` component. You can find all
available `accesslog` components using this command::

    $ spinta config components.accesslog
    Origin  Name                         Value
    ------  ---------------------------  ---------------------------------------
    spinta  components.accesslog.file    spinta.accesslog.file:FileAccessLog
    spinta  components.accesslog.python  spinta.accesslog.python:PythonAccessLog

Here we can see two logging components available, `file` and `python`.

`python` component is mainly used for testing, it saves log messages in
memory, do not use it in production environment, you will run out of memory.

For production you should use `file` component.

You can set logging backend like this:

.. code-block:: yaml

    accesslog.type: file
    accesslog.file: /path/to/file.log

See :ref:`configuration` for more information.


Log message format
==================

Log messages are stored in JSON Lines format, a new JSON object is appended
to the specified file as a new line.

Here is an example of a log message:

.. code-block:: json

    {
      "agent": "HTTPie/2.5.0",
      "client": "default",
      "format": "json",
      "action": "getall",
      "method": "GET",
      "url": "http://localhost:8000/example/City",
      "time": "2021-11-09T14:49:12.173881+00:00",
      "model": "example/City"
    }

Real log message would be stored in a single line, here log message is split
into multiple lines for better readability.

List of possible fields in a log message:

Always present fields:

:agent:
    User agent used to access data.
:client:
    Client name used to access data (see :ref:`client-credentials`).
:format:
    Requested data format (see `spinta config exporters` for supported formats).
:action:
    Data request action (:ref:`available actions <available-actions>`).
:method:
    HTTP request method.
:url:
    Full request URL.
:time:
    Date and time when log message was recorded.

Extra fields fields, that might not be present in log message:

:txn:
    Transaction id of a write operation.
:rctype:
    Request body content type, for example `application/json`.
:ns:
    Accessed namespace name (if a namespace was accessed).
:model:
    Accessed absolute model name (if a model was accessed).
:prop:
    Accessed property name of a model (if property was accessed).
:id:
    Accessed object id (if object was accessed directly).
:ref:
    Accessed object revision (if object was accessed directly for writing).

Log messages contains only relevant fields. For example `rctype` is only
available for write operations, `ns`, `model`, `prop` are only available if a
namespace, a model or a property is accessed.


Reading log messages
====================

You can use jq_ tool for reading log messages from a json lines file.

.. _jq: https://stedolan.github.io/jq/

For example if you want to get only messages where a `model` was accessed,
you can run this command::

    $ cat accesslog.json | jq 'select(has("model"))'

Or if you want to get all `model` messages where `action` was `getall`::

    $ cat accesslog.json | jq 'select(has("model") and .action == "getall")'
