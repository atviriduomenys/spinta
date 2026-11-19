.. default-role:: literal

################
Changes endpoint
################

The ``/:changes`` endpoint provides access to the *historical log of modifications*
for a model. It returns a paginated list of change records, each representing a
single data operation (insert, update/patch, delete, or move) performed on the
model. The changelog is automatically maintained by Spinta for all writes
through the API and ``spinta push/pull`` operations.

Implementation
***************

The endpoint is implemented as a backend command overload:

``spinta/backends/postgresql/commands/changes.py``::

    @commands.changes.register(Context, Model, PostgreSQL)
    def changes(
        context: Context,
        model: Model,
        backend: PostgreSQL,
        *,
        id_: str = None,
        limit: int = 100,
        offset: Optional[int] = -100,
    ):
        ...

It reads from the ``CHANGELOG`` table (``TableType.CHANGELOG``) associated with
the model, which is structured as follows:

| Column      | Type   | Description                                                   |
| ----------- | ------ | ------------------------------------------------------------- |
| ``_id``     | int    | Internal change sequence number (ascending, used for pagination) |
| ``_rid``    | string | The affected record ID (``_id``)                             |
| ``_txn``    | string | Transaction ID (from ``_txn`` table)                           |
| ``_created``| timestamp| When the change was made                                    |
| ``_op``     | string | Operation type: ``insert``, ``patch``, ``delete``, ``move`` |
| ``_data``   | json   | JSON representation of affected field values (may be empty)  |

The command also supports::

- ``id_`` — filter changelog entries to those modifying a specific record
  (by ``_rid``). The ``/:changes`` path in the URL is parsed as the ``id_``
  parameter.
- ``limit`` — maximum number of records to return (default: 100).
- ``offset`` — pagination offset; can be:
  - ``None`` — start from the beginning
  - ``>= 0`` — start from a specific ``_cid`` value
  - ``< 0`` — start from a calculated offset relative to the latest changes
    (also internally maps to pagination in sync)

The ``_data`` payload varies by operation:

- **insert/patch/delete** — the affected field values as a ``dict``,
  with all properties of the record (prefixes like ``_`` reserved for metadata).
- **move** — encodes the move operation as ``{"_same_as": <target_id>}`` instead
  of the moved record's data.

Action assignment
*****************

The ``:changes`` URL parameter triggers the ``Action.CHANGES`` action:

``spinta/urlparams.py`` and ``spinta/core/enums.py``::

    METHOD_TO_ACTION = {
        "POST": Action.INSERT,
        "PUT": Action.UPDATE,
        "PATCH": Action.PATCH,
        "DELETE": Action.DELETE,
    }

    def get_action(params: UrlParams, request: Request) -> Action:
        if params.action is not None:
            return params.action

        if request.method in ("GET", "HEAD"):
            if params.changes:
                return Action.CHANGES
            elif params.pk:
                return Action.GETONE
            # ...

All backends (PostgreSQL, fs, memory, nobackend) register an overload for
``commands.changes``; the PostgreSQL implementation is shown above as it is
the primary production backend.

Usage examples
**************

**List all recent changes for a model**

``GET /{model}/:changes``

.. code-block:: bash

    curl -X GET "http://localhost:8000/Report/:changes" \
      -H "Authorization: Bearer <token>" \
      -H "Content-Type: application/json"

**Paginated changelog with cursor-based offset**

``GET /{model}/:changes/{cid}?limit({n})``

.. code-block:: bash

    # Start from change with _id = 1000, limit 50 entries
    curl -X GET "http://localhost:8000/Report/:changes/1000?limit(50)" \
      -H "Authorization: Bearer <token>" \
      -H "Content-Type: application/json"

**Changelog for a specific record**

``GET /{model}/{_id}/:changes``

.. code-block:: bash

    # Changes affecting the record with _id = "abc-123"
    curl -X GET "http://localhost:8000/Report/abc-123/:changes" \
      -H "Authorization: Bearer <token>" \
      -H "Content-Type: application/json"

Typical response (excerpt)::

    {
      "_data": [
        {
          "_cid": 1,
          "_created": "2024-01-15T10:23:45Z",
          "_op": "insert",
          "_rid": "new-record-1",
          "_txn": "tx-abc-123",
          "_revision": "1",
          "status": "draft",
          "title": "New Report"
        },
        {
          "_cid": 2,
          "_created": "2024-01-15T10:24:10Z",
          "_op": "patch",
          "_rid": "new-record-1",
          "_txn": "tx-abc-123",
          "_revision": "1",
          "data": {"status": "published"}
        }
      ],
      "_meta": {
        "query": {"select": ["_cid", "_op", "status"]}
      }
    }

During synchronization (e.g., ``spinta keymap sync``), Spinta uses the changes
endpoint to fetch only the delta of modified records. The remote changelog URL
is built as::

    {server}/{model}/:changes/{offset_cid}?limit({sync_page_size})

See: :doc:`/contrib/architecture/data-access/backends/index` for backend features and :doc:`/contrib/architecture/multiple-dispatch/commands` for how commands are dispatched per backend.

Authorization
**************

The endpoint requires the ``changes`` action on the model:

``tests/test_changes.py`` and ``tests/backends/test_postgresql.py``::

    app.authmodel(model, ["insert", "patch", "changes"])
    # or
    app.authmodel("Country", ["insert", "update", "changes"])

Clients must be explicitly authorized for the ``changes`` permission; without
it, the request returns a 403 Forbidden.

Related concepts
****************

- :doc:`/contrib/architecture/data-access/backends/index` — backends, backend features, and commands
- :doc:`/contrib/architecture/data-access/backends/internal-data-store` — PostgreSQL changelog table in the internal backend
- :doc:`/contrib/architecture/multiple-dispatch/commands` — command registration and dispatcher
- :doc:`/contrib/architecture/keymap/index` — ``spinta keymap sync`` uses ``/:changes`` to fetch the delta of modified records
- :doc:`/contrib/architecture/concepts/component` — ``Component`` and ``Model`` concepts
