:orphan:

.. default-role:: literal

########
Concepts
########

This is a collection of concept pages — one page per major Spinta class or
concept. Each page links the Spinta class to concepts in external
specifications (UAPI, DSA, DCAT, RFC 6749, and the `Conceptual model`_ — the
ubiquitous language of the Lithuanian open data catalogue) and to the place
where the class is implemented.

The `Conceptual model`_ is an external Draw.io UML diagram of the Lithuanian
open data catalogue. It is the ubiquitous language the following Spinta
concepts map to:

.. list-table::
   :header-rows: 1

   * - Spinta
     - Conceptual model
   * - `Dataset` (:doc:`/contrib/architecture/manifests/logical/datasets`)
     - `Dataset`
   * - `Resource` (:doc:`/contrib/architecture/manifests/physical/resources`)
     - `Distribution`
   * - `Model` (:doc:`/contrib/architecture/manifests/logical/models`)
     - `Model`
   * - `Entity` (:doc:`/contrib/architecture/manifests/physical/entities`)
     - `SourceModel`
   * - `Property` (:doc:`/contrib/architecture/manifests/logical/properties`)
     - `Property`
   * - `Attribute` (:doc:`/contrib/architecture/manifests/physical/attributes`)
     - `SourceProperty`
   * - `DataType` (:doc:`/contrib/architecture/manifests/logical/data-type`)
     - `DataType`

.. _Conceptual model: https://raw.githubusercontent.com/atviriduomenys/
   katalogas/f8ad4b7d9f332ed9f9ada6c4e3e735512745e328/uml.drawio.svg

In C4 terms (see :doc:`/contrib/architecture/index`), these pages are the code
level (C4 level 4) — one page per class, complementing the UML class
diagram in :doc:`/contrib/architecture/uml-diagram`.

Pages here are linked from the architecture pages
(see :doc:`/contrib/architecture/index` for the map):

- `Component`, `Node` and `Metadata` (this directory) — the base
  classes of the manifest node tree;
- the logical layer pages (:doc:`/contrib/architecture/manifests/logical/index`) and
  physical layer pages (:doc:`/contrib/architecture/manifests/physical/index`);
- configuration concepts (:doc:`/contrib/architecture/configuration/rawconfig`,
  :doc:`/contrib/architecture/configuration/config`,
  :doc:`/contrib/architecture/configuration/context`,
  :doc:`/contrib/architecture/configuration/store`);
- data access concepts (:doc:`/contrib/architecture/data-access/backends/backend`,
  :doc:`/contrib/architecture/data-access/backends/external-backend`,
  :doc:`/contrib/architecture/data-access/pagination`);
- keymap (:doc:`/contrib/architecture/keymap/index`);
- manifest and formula machinery (:doc:`/contrib/architecture/manifests/manifest`
  and related pages under :doc:`/contrib/architecture/manifests/index`).

.. toctree::
   :hidden:

   component
   node
   metadata
