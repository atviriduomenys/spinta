.. default-role:: literal

#######
Clients
#######

Target: ``spinta sync clients``. Status: **Partial**. Parent overview:
:doc:`/contrib/architecture/sync`.

Would synchronize OAuth clients from the Metadata Catalogue, if the catalogue
manages client administration. The catalogue side of the client and contract
synchronization endpoint is done (spinta#1656, closed). The agent side, which
calls the endpoint and stores clients and their scopes in local client files, is
still open in spinta#1684. Local clients are managed today with ``spinta client``.

Element
*******

**Planned.** ``spinta sync clients <element>`` takes the client id, in UUID
form.

Location
********

**Planned.**

.. code-block:: text

    $XDG_DATA_HOME/spinta/sync/<catalog>/clients/<client>.yml

``<client>`` is the client id in UUID form.

Current implementation
**********************

**Partial.** Clients are created and managed with ``spinta client``, which
stores them in the configuration directory, not in ``XDG_DATA_HOME``:

* Client files are in ``config_path/clients`` (by default
  ``$XDG_CONFIG_HOME/spinta/clients``), see ``get_clients_path()`` in
  ``spinta/utils/config.py``.
* A client with a UUID id is stored in a directory built from the id:
  ``clients/id/<ab>/<cd>/<rest>.yml``. Other names go to ``clients/unknown/``
  (``get_client_file_path()`` in ``spinta/auth.py``).
* The file holds the client id, the client name, the secret hash, the scopes,
  the backends and the contract scopes. A separate keymap file
  (``helpers/keymap.yml``) maps client names to client ids.

The sync-owned location above is a different directory from where the existing
client files are stored today.

Open questions
**************

* **Two places for client files.** Should sync-owned client files in
  ``$XDG_DATA_HOME/spinta/sync/<catalog>/clients/`` be a separate store, or should they be
  read as a fallback for the client files in the configuration directory? If a
  client exists in both, which one wins?
* **Layout.** The sync-owned location uses ``<client>.yml``. The existing store
  uses a directory tree built from the UUID. Which layout should sync-owned
  files use?

Related issues
**************

* spinta#1656 — catalogue side of the client and contract synchronization
  endpoint (closed).
* spinta#1684 — agent side: call the endpoint and store clients and scopes.

.. seealso::

    * :doc:`/contrib/architecture/sync` — target architecture and status
    * :doc:`contracts` — contract scopes are also stored in client files
    * :doc:`/contrib/architecture/auth/clients` — how clients are stored and managed
