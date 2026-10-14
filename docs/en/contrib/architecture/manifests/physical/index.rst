.. default-role:: literal

##############
Physical layer
##############

The physical layer of a manifest describes *where* the data comes from
and *how* it maps to the logical model — the source side of the mapping
(in RML terms: the logical source, the triples map's subject map and the
predicate-object term maps):

- :doc:`resources` — one accessible form of a dataset's data: which
  backend to use, where data lives (DSN, URL, file path), which
  parameters are used.
- :doc:`entities` — the binding of one model to one external source:
  which dataset and resource rows come from, which properties form the
  source primary key, the source table or object name.
- :doc:`attributes` — the binding of one property to one external
  column, with an optional `prepare` formula and a custom type.

All three extend the `External` marker class
(`spinta/datasets/components.py`) and live on the *internal* side of the
mapping — resources attached to datasets (`dataset.resources`), entities
on models (`model.external`) and attributes on properties
(`prop.external`). The external source *backends*
that read through these bindings are described in
:doc:`/contrib/architecture/data-access/backends/external-data-sources`.

.. toctree::
   :maxdepth: 1

   Resources <resources>
   Entities <entities>
   Attributes <attributes>
