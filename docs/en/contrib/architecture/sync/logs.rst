.. default-role:: literal

####
Logs
####

Target: ``spinta sync logs``. Status: **Planned**. Parent overview:
:doc:`/contrib/architecture/sync`.

Would aggregate statistics from the agent's access logs (for example views and
downloads per dataset) and send them to the Metadata Catalogue. Today this is
done by ``downloadstats.py`` in the Katalogas repository (``scripts/``), run
outside of Spinta. Related Katalogas issues: #1057 (closed, gaps in download
statistics) and #298 (closed, reading the access log through the catalogue API).
No open spinta issue exists yet for moving this into ``spinta sync``. See also
:doc:`/contrib/architecture/access-log/index`.

Element
*******

**Planned.** ``spinta sync logs <element>`` takes the path to the access log
file.

Location
********

**Planned.**

.. code-block:: text

    $XDG_DATA_HOME/spinta/sync/<catalog>/logs/accesslogs/<date>.jsonl

``<date>`` is the log rotation date. One file per rotation period, in JSON
Lines format (one JSON object per line).

Current implementation
**********************

**Planned.** The access log is written by Spinta, but not into a sync-owned
location:

* The access log is configured with ``accesslog.type`` and ``accesslog.file``
  (``spinta/config.py``). The file backend is ``FileAccessLog``
  (``spinta/accesslog/file.py``), which writes one JSON object per line.
* The default ``accesslog.file`` is ``/dev/null``, so logging is off unless a
  file is set.
* There is no date-based rotation. The file is opened in append mode, and
  rotation is not done by Spinta.
* No ``spinta sync logs`` target exists.

Open questions
**************

* **Element meaning.** Is the element the source access log file that is read,
  or the sync-owned file that is updated?
* **Default log location.** Should ``accesslog.file`` default to a file under
  ``$XDG_DATA_HOME/spinta/sync/<catalog>/logs/accesslogs/``, or stay ``/dev/null``?
* **Rotation.** Who rotates the files by date: Spinta, or an external tool such
  as ``logrotate``?

.. seealso::

    * :doc:`/contrib/architecture/sync` — target architecture and status
    * :doc:`/contrib/architecture/access-log/index` — how the access log works
