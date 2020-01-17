.. default-role:: literal

RQL
###

Spinta uses an RQL-like query language, which also can be used to transform,
validate and define data.

Examples:

.. code-block:: python

    # Data definition
    create_table(
      report,
      column(status, string, choices=[ok, warn]),
    )

    # Query
    groupby(org)&status.lower()=ok|status.lower()=warn&sort(+name)

    # Data transformation and validation
    check(lower().in(ok, warn), "Unknown status {given}.", given:.).
    check(len() < 10, "Status name is too long.")
