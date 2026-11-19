.. default-role:: literal

C4 architecture diagrams
########################

The UML class diagram above shows the code structure (C4 level 4). Here is
the same code base seen through the `C4 model`_ abstraction levels:
`software system` (Spinta itself) -> `containers` (applications and data
stores) -> `components` (modules within a container) -> `code` (classes,
see :doc:`uml-diagram` and :doc:`/contrib/architecture/concepts/index`). More on the C4
abstractions: https://c4model.com/abstractions.

.. _C4 model: https://c4model.com/

System context (level 1)
========================

Spinta as a single software system, with the people who use it and the
external systems it talks to.

.. mermaid::

   C4Context
   title Spinta — system context (C4 level 1)

   Person(consumer, "Data consumers", "Read open data over the HTTP API (JSON, CSV, RDF, XLSX) or browse the web UI")
   Person(publisher, "Data publishers", "Describe datasets and models in manifest files, load and publish data with the spinta CLI")
   Person(operator, "Operators", "Deploy Spinta, configure backends, auth clients and access log")

   System(spinta, "Spinta", "Open data platform: describes, extracts, transforms, loads and serves open data")

   System_Ext(sources, "External data sources", "SQL servers, CSV/JSON/XML files read with dask, WSDL/SOAP services, Google Sheets")
   System_Ext(remote, "Remote Spinta instances", "Other Spinta servers: pull/push data and synchronize keymaps via {server}/{model}/:changes")
   System_Ext(idp, "Identity provider", "Publishes JWKS public keys used to validate access tokens (e.g. Keycloak)")

   Rel(consumer, spinta, "Reads and pushes open data", "HTTP")
   Rel(publisher, spinta, "Defines models, imports, pulls and pushes data", "spinta CLI")
   Rel(operator, spinta, "Deploys and maintains", "config files, env vars")
   Rel(spinta, sources, "Reads external data", "pull command")
   Rel(spinta, remote, "Pulls and pushes data, syncs keymaps", "HTTP")
   Rel(spinta, idp, "Downloads public keys and validates tokens", "JWKS")
   UpdateRelStyle(consumer, spinta, $y="-25")
   UpdateRelStyle(publisher, spinta, $y="-25")
   UpdateRelStyle(operator, spinta, $y="25")
   UpdateRelStyle(spinta, sources, $y="20")

Data publishers and operators work through the `spinta` CLI and the
configuration layer described in :doc:`/contrib/architecture/configuration/index`. The identity
provider is used by :doc:`/contrib/architecture/auth/index` — public keys are downloaded from
`token_validation_keys_download_url` and used to validate access tokens
(see `spinta/auth.py:download_public_keys()`).

Containers (level 2)
====================

The applications and data stores inside the Spinta system.

.. mermaid::

   C4Container
   title Spinta — containers (C4 level 2)

   Person(consumer, "Data consumers", "Read open data over the HTTP API")
   Person(publisher, "Data publishers", "Work with manifests and data using the spinta CLI")

   System_Boundary(c0, "Spinta") {
       Container(agent, "Agent (spinta run --mode=external)", "Starlette (Python)", "Serves the REST API reading data from external data sources in real time")
       Container(repo, "Saugykla (spinta run --mode=internal)", "Starlette (Python)", "Serves the REST API from the internal data store")
       Container(push, "Push service (spinta push)", "Typer (Python)", "Pushes data from an external data source into an internal data store")
       Container(sync, "Sync service (spinta sync)", "Typer (Python)", "Syncs manifests, clients and other metadata with the Metadata Catalogue")
       Container(inspect, "Inspect tool (spinta inspect, spinta copy)", "Typer (Python)", "Converts and manages manifest files")
       Container(keymapsvc, "Keymap service (spinta keymap sync)", "Typer (Python)", "Manages external identifiers by encoding external keys")
       ContainerDb(internal, "Internal data store", "PostgreSQL (incl. Citus and PostGIS)", "Stores model schema, data and changelog; alternative backends: fs, memory, nobackend")
       ContainerDb(keymap, "Keymap store", "SQLite or Redis", "Maps heterogeneous external primary keys to generated internal ids")
       ContainerDb(accesslog, "Access log", "JSONL files", "Logs failures, responses and warnings per request")
       ContainerDb(manifests, "Manifest files", "CSV, ASCII, XLSX, Google Sheets, YAML, JSON, XML", "Describe datasets, resources, models and properties; read from disk or URLs")
   }

   System_Ext(sources, "External data sources", "SQL servers, files via dask, WSDL services")
   System_Ext(catalogue, "Metadata Catalogue", "Manifests, clients and other metadata")
   System_Ext(remote, "Remote Spinta instances", "Other Spinta servers: pull/push data and keymap sync")
   System_Ext(idp, "Identity provider", "Publishes JWKS public keys (e.g. Keycloak)")

   Rel(consumer, agent, "Reads data", "HTTPS")
   Rel(consumer, repo, "Reads and pushes data", "HTTPS")
   Rel(publisher, push, "Runs spinta push")
   Rel(publisher, inspect, "Runs spinta inspect and spinta copy")
   Rel(push, sources, "Reads data")
   Rel(push, internal, "Writes data")
   Rel(push, keymap, "Encodes external keys")
   Rel(sync, catalogue, "Syncs manifests, clients and other metadata", "HTTPS")
   Rel(inspect, manifests, "Reads and writes manifests")
   Rel(keymapsvc, keymap, "Writes encoded keys")
   Rel(agent, sources, "Reads data in real time")
   Rel(repo, manifests, "Loads manifests at startup")
   Rel(repo, internal, "Reads and writes schema, data and changelog")
   Rel(repo, keymap, "Encodes and decodes keys")
   Rel(repo, accesslog, "Writes access log")
   Rel(repo, idp, "Downloads public keys", "JWKS")
   UpdateLayoutConfig($c4ShapeInRow="2", $c4BoundaryInRow="1")

The :doc:`CLI <user-interfaces/cli>` and the HTTP API (``spinta run``)
share the same startup pipeline
(:doc:`/contrib/architecture/wired-together`) and operate on the same
manifest and keymap containers. Where each container is documented in
detail is listed in the Containers section of
:doc:`/contrib/architecture/index`.