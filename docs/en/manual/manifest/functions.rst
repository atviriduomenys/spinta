.. default-role:: literal

Functions
#########

Functions in manifest are used for:

- Transforming and validating data using `prepare` parameter.

- Describing database migration steps.

- Describing outgoing requests to data sources of `resource` nodes, such as
  HTTP headers and query parameters.


Using functions in Manifest
===========================

Manifest functions are written in the `prepare` column. Most functions are
resolved as soon as the manifest is loaded. Functions that depend on a request
(for example `eq()` in a URL) are resolved when the request is handled.

All examples on this page use the same two `resource` nodes, `cities` and
`countries`, both loaded from JSON APIs with the `dask/json` backend:

- `https://example.com/api/cities` returns cities, for example
  ``[{"id": 1, "name": "Vilnius", "country": "LT"}, ...]``.

- `https://example.com/api/countries` returns countries, for example
  ``[{"code": "LT", "name": "Lithuania"}, ...]``.


Resource parameters
===================

Resource parameters (`param` rows under a `resource`) describe values that
are sent to the data source. Each parameter has a `source` (the name used by
the data source) and a `prepare` formula that provides its value. The
parameter is linked to a property of a model with `param()`, so the value can
be given in the UAPI request URL.

The examples below use this manifest:

.. code-block:: text

   d | r | b | m | property | type      | ref     | source                            | prepare          | access
   example                  | dataset   |         |                                   |                  |
     | cities               | dask/json |         | https://example.com/api/cities    |                  |
     |   |   |              | param     | country | COUNTRY                           | query()          |
     |   |   |              | param     | api_key | X-Api-Key                         | header('secret') |
     |   |   | City         |           | id      | .                                 |                  |
     |   |   |   | id       | integer   |         | id                                |                  | open
     |   |   |   | name     | string    |         | name                              |                  | open
     |   |   |   | country  | string    |         |                                   | param()          | open
     | countries            | dask/json |         | https://example.com/api/countries |                  |
     |   |   |              | param     | lang    | LANG                              | query('lt')      |
     |   |   | Country      |           | code    | .                                 |                  |
     |   |   |   | code     | string    |         | code                              |                  | open
     |   |   |   | name     | string    |         | name                              |                  | open
     |   |   |   | lang     | string    |         |                                   | param()          | open


query()
-------

`query()` defines an outgoing query parameter. The `source` of the `param` row
is the name of the query parameter, and the `prepare` formula gives its
default value.

``query('default')``
    Sends the parameter with the default value. A value given in the request URL
    for a property linked with `param()` replaces the default.

``query()``
    Sends the parameter without a default. The value must come from the request
    URL through a property linked with `param()`. If it is missing, the request
    fails with ``400``, and the data source is not called. A `query()` without a
    default that is not linked to any property is an invalid manifest.

Example: `query()` on `City.country`, so the request URL value is sent to the
data source:

.. code-block:: text

   GET /example/City?country='LT'

Data source request::

   GET https://example.com/api/cities?COUNTRY=LT

Example: `query('lt')` on `Country.lang`, the default is sent when the request
URL has no value:

.. code-block:: text

   GET /example/Country

Data source request::

   GET https://example.com/api/countries?LANG=lt

With a value in the request URL, that value is sent instead:

.. code-block:: text

   GET /example/Country?lang='en'

Data source request::

   GET https://example.com/api/countries?LANG=en

A `query()` in a `resource` `source` URL is merged with the resource parameters
into one query string. If the same parameter is defined in both places,
``spinta check --check-query-conflicts`` reports it.


header()
--------

`header()` sends an HTTP header with every request to the data source. The
header name is the `source` of the `param` row, and the `prepare` formula gives
its value.

``header('value')``
    Sends the header with the given value.

Example: `header('secret')` on `cities.api_key` sends the header `X-Api-Key`:

.. code-block:: text

   GET https://example.com/api/cities?COUNTRY=LT
   X-Api-Key: secret

Header values that can only be resolved at request time (for example
`creds()`) are resolved when the request is sent.


param()
-------

`param()` links a property without a `source` to a resource parameter with the
same name as the property. The value of the property is then sent to the data
source as the query parameter, instead of being used as a row filter. The
value is shown in the response, so it is the value that was sent to the data
source.

``param()``
    Links the property to the resource parameter with the same name.

``param(name)`` or ``param('name')``
    Links the property to the resource parameter with the given name.

    Example: ``param(country)`` on `City.country` links it to the `country`
    parameter.

The linked property must have no `source` (the `source` column is empty).

Example, `City.country` is linked to the `country` query parameter. The
request sends the value to the data source, and every returned row shows
that value:

.. code-block:: text

   GET /example/City?country='LT'

Data source request::

   GET https://example.com/api/cities?COUNTRY=LT

Response::

   {"_data": [
     {"name": "Vilnius", "country": "LT"},
     {"name": "Riga", "country": "LT"}
   ]}

Rules for linked properties:

- A linked property can be used in a request as a top-level condition, for
  example ``country='LT'`` or ``eq(country,'LT')``.

- The same linked property cannot be used more than once in a request, for
  example ``country='LT'&country='LV'``, and it cannot be used inside
  ``or()``, ``and()`` or ``not()``. Such a request fails with ``400``.

- Only ``eq()`` gives a linked property a value for the data source. Other
  operators, such as ``!=``, do not.

  Example: ``/example/City?country!='LT'`` does not send a value for `COUNTRY`.
  `country` has no default in the example manifest, so the request fails with
  ``400`` and the data source is not called. If the parameter had a default,
  like ``query('LT')``, the default would be sent instead.
