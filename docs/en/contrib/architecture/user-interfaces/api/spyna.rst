.. default-role:: literal

#####
Spyna
#####

Spyna is the expression language used by Spinta's user interfaces — the
API query string, `prepare` formulas in manifests and `param` sources. The
name stands for "Spinta YAML-like AST": it is a small, Python-like
expression grammar defined with `lark`_ (`spinta/spyna.py:GRAMMAR`) that
`spinta.spyna.parse()` compiles into a JSON-serializable abstract syntax
tree of ``{name, args}`` dicts (a `Spinta/spyna.py:SpynaAST` typed dict).

The query string of every data request is parsed as a spyna expression in
`spinta/urlparams.py:prepare()` (via
`spinta/utils/url.py:parse_url_query()`): ``?eq(population, 100)`` becomes
a filter, ``?select(code, name)`` a selection, ``?sort(-code)`` a sort,
and so on — see `Path operators`_ on the :doc:`/contrib/architecture/user-interfaces/api/index` page (the ``:name``
path operators are separate, fixed grammar, everything else falls into
the query expression).

.. _lark: https://lark-parser.readthedocs.io/
.. _Path operators:
   /contrib/architecture/user-interfaces/api/index.html#path-operators

Grammar and parsing
*******************

The `spinta/spyna.py:GRAMMAR` (a `lark`_ LALR grammar) supports the usual
expression shapes: comparisons, boolean combinations (`|`, `&`, `!`),
arithmetic operators, method call syntax (`country.name`), named and
keyword arguments, and constants. `spinta.spyna.parse(rql)` returns
a dict like:

.. code-block:: python

   {
       "name": "eq",
       "args": [
           {"name": "bind", "args": ["population"]},
           100,
       ],
   }

Function calls become ``{name: 'select', args: [...]}``, attribute
accesses become ``getattr``, method calls get ``type: 'method'`` and
short comparison forms get ``type: 'expression'``. There is also
`spinta.spyna.unparse()` which renders a spyna AST back to text.

From spyna to Expr
******************

The parsed AST is handed to `spinta.core.ufuncs.asttoexpr()` which
converts it into resolvable `Expr` objects (`ShortExpr` for the short
form, `MethodExpr` for method calls). From there the expression is
resolved and executed on a `Env` subclass built by the backend — see
:doc:`/contrib/architecture/multiple-dispatch/ufuncs/index` and
:doc:`/contrib/architecture/multiple-dispatch/ufuncs/expr`.

Where spyna is used
*******************

- API query strings (filters, selects, sorts, function operators) — see
  :doc:`/contrib/architecture/user-interfaces/api/index` and :doc:`/contrib/architecture/data-access/pagination`.
- `prepare` formulas of datasets, resources, entities, attributes,
  models, properties and data types (see
  :doc:`/contrib/architecture/manifests/physical/resources` and
  :doc:`/contrib/architecture/manifests/logical/properties`).
- `param` sources and formulas (see
  :doc:`/contrib/architecture/manifests/physical/resources`).
- `change` request expressions (``:changes`` actions).

External specification
**********************

- Spyna implements DSA's formula language (see DSA's Formulas section
  and the DSA grammar with value/context/selection functions): the DSA
  spec, with the grammar compiled into a uniform ``{name, args}`` AST —
  this AST is what Spinta calls spyna, and `Expr` is its object form.
- In UAPI, spyna appears in query string parameters (filter, select,
  sort, limit, etc.): UAPI draft 2.0 replaces spyna query expressions
  with reserved query parameters starting with ``_`` (``_select``,
  ``_sort``, ``_gt``, ...) — current Spinta takes path operators
  (``:select(...)``) and spyna expressions in the query string instead.

See also
********

- :doc:`/contrib/architecture/multiple-dispatch/ufuncs/expr` — the
  `Expr` tree produced from spyna.
- `spinta/spyna.py` — the grammar, `parse()` and `unparse()`.
