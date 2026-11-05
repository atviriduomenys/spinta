.. default-role:: literal

#########
Contracts
#########

Target: ``spinta sync contracts``. Status: **Planned**. Parent overview:
:doc:`/contrib/architecture/sync`.

Would synchronize smart contracts, which are ODOC documents (``adoc`` files),
from the Metadata Catalogue to the agent. The agent uses contracts to validate
data recipients' tokens: the scopes in a token must match the contract. The
catalogue side returns the contracts that are linked to the agent's datasets
and are in the ``SIGNED`` or ``ACTIVE`` state. The agent-side work is tracked
in spinta#1684, and the agent-side token validation in katalogas#2071.

Element
*******

**Planned.** ``spinta sync contracts <element>`` takes the contract id, in UUID
form.

Location
********

**Planned.**

.. code-block:: text

    $XDG_DATA_HOME/spinta/contracts/<contract>.yml

``<contract>`` is the contract id in UUID form. The file is ``.yml``, but the
contract content is an ODOC ``adoc`` document, so the file format is open, see
`Open questions`_.

Current implementation
**********************

**Planned.** No contract files and no contract target exist. What exists today:

* Contract scopes are stored inside client files, in the ``contract_scopes``
  field (``create_client_file()`` in ``spinta/auth.py``). ``spinta client``
  manages those client files, see :doc:`clients`.
* The ``check_contract_scopes`` setting (default ``false``) enables checking
  token scopes against the contract scopes of the client (``spinta/auth.py``).
  No contract documents are read from disk.

Open questions
**************

* **File format.** Contracts are ``adoc`` documents, but the location rule
  uses ``.yml``. Should the file keep the ``adoc`` content, or should it be
  converted to YAML?
* **Relation to client files.** Contract scopes are also stored in client
  files. Should the contract target write them into client files, into its
  own contract files, or both?

Related issues
**************

* spinta#1684 — agent-side synchronization of clients and contracts.
* katalogas#2071 — agent-side token validation against contracts.

.. seealso::

    * :doc:`/contrib/architecture/sync` — target architecture and status
    * :doc:`clients` — client files, which carry the contract scopes
