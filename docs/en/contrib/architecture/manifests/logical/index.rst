.. default-role:: literal

#############
Logical layer
#############

The logical layer of a manifest describes *what* Spinta serves over the
API — the naming and structure of the published data model, independent
of where the data is stored:

- :doc:`namespaces` — the tree grouping models under `/datasets/gov/rc`
  style paths.
- :doc:`datasets` — DCAT-compatible dataset metadata, doubling as a
  namespace.
- :doc:`base-models` — model inheritance
  (`rdfs:subClassOf`-like links between models).
- :doc:`models` — one resource served by the API with its properties.
- :doc:`properties` — model properties, including the reserved ones
  (`_id`, `_revision`, ...).
- :doc:`data-type` — the per-type behavior carried by every property.

All logical layer classes are `spinta/components.py` subclasses of
`MetaData` (or `ExtraMetaData` for attached entries like `Property` and
`Base`) and are stored in the manifest's node lists — see
:doc:`/contrib/architecture/concepts/metadata` and
:doc:`/contrib/architecture/concepts/node`.

How the manifest rows load into these classes is described in
:doc:`../index` ("Loading a manifest") and, row by row, in
:doc:`/contrib/architecture/manifests/tabular-reader`.

.. toctree::
   :maxdepth: 1

   Namespaces <namespaces>
   Datasets <datasets>
   Base models <base-models>
   Models <models>
   Properties <properties>
   Data type <data-type>
