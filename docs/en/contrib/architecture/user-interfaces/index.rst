.. default-role:: literal

###############
User interfaces
###############

Spinta is used through two user-facing interfaces which share the same
core layers (commands, manifests, backends, configuration):

- **API** — the HTTP interface served by the `Starlette` web app (see
  :doc:`api/index`). Requests are authenticated with OAuth 2.0 (see
  :doc:`/contrib/architecture/auth/index`), parsed into `UrlParams` and
  dispatched to data commands.
- **CLI** — the `spinta` command line tool built with `Typer`
  (see :doc:`cli`). Handlers fork the context, load the store and
  manifest, and call the same commands the API uses.

Where to start
==============

For the HTTP API: :doc:`api/index` (and :doc:`api/spyna` for the query
expression language). For the CLI: :doc:`cli`. For the startup pipeline
both interfaces share, see
:doc:`/contrib/architecture/wired-together`.

.. toctree::
   :maxdepth: 1

   API <api/index>
   CLI <cli>
