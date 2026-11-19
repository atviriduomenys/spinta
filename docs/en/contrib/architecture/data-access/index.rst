.. default-role:: literal

###########
Data access
###########

Data access is the layer between the user interfaces and the stored or
external data: it decides *where* a model's data lives (see
:doc:`backends/index`) and *how* responses are sliced into pages
(:doc:`pagination`).

A model's backend is resolved at link time (`commands.link`) — from the
model definition, the manifest backend or, for external models, from
`model.external.resource.backend` (see :doc:`/contrib/architecture/manifests/physical/resources`).
Data is then read and written through multiple-dispatched commands (`commands.getone`, `commands.getall`, `commands.changes`,
`commands.push`, ...) — see
:doc:`/contrib/architecture/multiple-dispatch/commands` and :doc:`changes`.

.. toctree::
   :maxdepth: 1

   Backends <backends/index>
   Pagination <pagination>
   Changes <changes>
