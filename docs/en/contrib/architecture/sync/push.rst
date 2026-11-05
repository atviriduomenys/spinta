.. default-role:: literal

####
Push
####

Target: ``spinta sync push``. Status: **Planned**. Parent overview:
:doc:`/contrib/architecture/sync`.

Would send the state of ``spinta push`` to the Metadata Catalogue, that is, the
time of the last successful push and any errors, so that the catalogue shows the
current state of published data. Today push state is only kept locally in
``{data_path}/push/{remote}.db`` (see :doc:`/contrib/architecture/user-interfaces/cli`).
Related: spinta#1416 (``spinta push`` in the synchronization context, blocked)
and the catalogue metadata update described in the Katalogas documentation.

Element
*******

**Planned.** ``spinta sync push <element>`` takes the remote host name, the same
name that identifies the remote in ``credentials.cfg``, for example
``data.gov.lt``.

Location
********

**Planned.**

.. code-block:: text

    $XDG_DATA_HOME/spinta/push/<remote>.yml

``<remote>`` is the remote name. The push state of ``spinta push`` is stored in
the same directory, as ``<remote>.db``, so the two files differ only in the
extension.

Current implementation
**********************

**Planned.** Push state is only local:

* ``spinta push`` writes its state to ``$XDG_DATA_HOME/spinta/push/<remote>.db``
  (SQLite), or to the file given with ``--state`` (``spinta/cli/push.py``).
* The remote name is not the raw host name. ``_parse_client_handle()`` in
  ``spinta/client.py`` replaces dots and dashes with underscores, so
  ``data.gov.lt`` becomes ``data_gov_lt``, and it appends the port for
  non-standard ports. The ``.db`` file is named with this normalized name.
* No ``spinta sync push`` target exists.

Open questions
**************

* **Element form.** Should the element be the section name in
  ``credentials.cfg`` (the host name), or the normalized remote name used in
  the file name (``data_gov_lt``)?
* **Relation to the push state file.** The ``.db`` push state and the ``.yml``
  sync output are in the same directory. Should the ``.yml`` file be generated
  from the ``.db`` file, or from the same data read by ``spinta push``?

Related issues
**************

* spinta#1416 — ``spinta push`` in the synchronization context.

.. seealso::

    * :doc:`/contrib/architecture/sync` — target architecture and status
    * :doc:`/contrib/architecture/user-interfaces/cli` — the ``spinta push`` command
