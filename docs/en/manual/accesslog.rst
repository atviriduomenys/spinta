.. default-role:: literal

Logging
#######

Here you can find information about access logging. Access log, will track
which API endpoints are accessed. For each request two long entries are created
on for request and optionaly another for response. Response entry might not be
created, if there was no response due to an error.


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

    accesslog:
      type: file
      file: /path/to/file.log

See :ref:`configuration` for more information.


Log message format
==================

Log messages are stored in JSON Lines format, a new JSON object is appended
to the specified file as a new line.

For each request two messages and two lines are added to log file. One message
for request and another for response. In case of an error, response message
might not be added.

Both request and response messages share the same `txn` value. You can find
matching requests and response entries by `txn`. It is not guaranteed, that
request and response are added one after the other, because multiple parallel
requests might be added and responses will follow as soon as request is
completed.

Request message
---------------

Example of request log message:

.. code-block:: json

    {
      "time": "2024-07-31T07:37:03.552142+00:00",
      "pid": "1000",
      "type": "request",
      "method": "GET",
      "action": "getall",
      "ns": "example/:ns",
      "model": "example/City",
      "prop": "example/City/prop",
      "id": "3317125b-bf21-4458-9977-46a9dec05897",
      "rev": "3012e5e5-a24d-4535-b140-560601b3e7d5",
      "txn": "db5e8248-c1e6-4ca4-a5ea-c63e5d2d005b"
      "rctype": "application/json",
      "format": "json",
      "url": "http://localhost:8000/example/City"
      "client": "a2f712a5-33e2-4620-a609-018eb9c9c212",
      "reason": "",
      "agent": "HTTPie/2.5.0",
    }

Example is pretty printed for readability, but in log files, each message is
stored in a single line.

Always present fields:

:time:
    Date and time when log message was recorded.

:txn:
    Transaction id (UUID) of this request. Same id is used in response log
    message.

:type:
    For requests this will always be `request`.

    .. versionadded:: 0.1.40 (2022-11-01)

:pid:
    Process id of a web worker who issued log message.

    .. versionadded:: 0.1.43 (2022-11-15)

:method:
    HTTP request method.

:action:
    Data request action (:ref:`available actions <available-actions>`).

:format:
    Requested data format (see `spinta config exporters` for supported
    formats).

:url:
    Full request URL.

:client:
    Client id in UUID format, which was used to access data (see
    :ref:`client-credentials`).

    .. versionchanged:: 0.1.59 (2023-11-14)

    Before 0.1.59, client name was used, starting with 0.1.59 client id is used
    in logs.

:agent:
    User agent used to access data.

Extra fields fields, that might not be present in log message:

:rctype:
    Request body content type, for example `application/json`. This is used
    only for write operations and describes content type of request payload.

:ns:
    Accessed namespace name (if a namespace was accessed).

:model:
    Accessed absolute model name (if a model was accessed).

:prop:
    Accessed property name of a model (if a subresource was accessed).

:id:
    Accessed object id (if object was accessed directly).

:rev:
    Accessed object revision (if object was accessed directly for writing).


Response message
----------------

.. versionadded:: 0.1.40 (2022-11-01)

Example of response log message:

.. code-block:: json

    {
      "time": "2024-07-31T07:37:03.557142+00:00",
      "type": "response",
      "delta": 7.822166530415416,
      "memory": 2433024,
      "objects": 200,
      "txn": "db5e8248-c1e6-4ca4-a5ea-c63e5d2d005b"
    }

Example is pretty printed for readability, but in log files, each message is
stored in a single line.

Always present fields:

:time:
    Date and time when log message was recorded.

:txn:
    Transaction id (UUID) of this request. Same id is used in response log
    message.

:type:
    For responses this will always be set to `response`.

:memory:
    RAM memory difference between request and response in bytes.

    Difference might include memory used by other requests, if multiple
    requests happen at the same time and on the same web worker.

:delta:
    Time difference between request and response in seconds.

:objects:
    Number of objects processed during requests. This counts only top levels
    objects and does not include composite objects attached to the top level
    object.

Response message does not include data which is already provided in request
message. You can find request of a response by `txn` value.

Log messages contain only relevant fields. For example `rctype` is only
available for write operations, `ns`, `model`, `prop` are only available if a
namespace, a model or a property is accessed.


Reading log messages
====================

You can use jq_ tool for reading log messages from a json lines file.

.. _jq: https://stedolan.github.io/jq/

For example if you want to get only messages where a `model` was accessed,
you can run this command::

    jq 'select(.type=="request" and has("model"))' accesslog.json

Or if you want to get all `model` messages where `action` was `getall`::

    jq 'select(.type=="request" and has("model") and .action == "getall")' accesslog.json

Count number of requests by month::

    jq 'select(.type=="request") | .time[:7]' accesslog.json | sort | uniq -c 

Count number of response by month::

    jq 'select(.type=="response") | .time[:7]' accesslog.json | sort | uniq -c 

Count number of objects processed by month::

    jq -r 'select(.type=="response") | [.time[:7], .objects | tostring] | join(" ")' accesslog.json | \
        awk '{sum[$1] += $2} END {for (date in sum) print sum[date], date}'

In most cases you need to add `select(.type=="request")` or
`select(.type=="response")` in order to avoid dublication.
