.. default-role:: literal

Response limits
===============

Response limits are optional. By default, `default_limit_objects` and model
limits are unset, so streaming responses can return all rows. HTML keeps its
existing display cap without requiring this feature to be enabled.

Configuring limits
------------------

Set a deployment-wide default or a limit for an individual model in your
:ref:`configuration file <config-file>`:

.. code-block:: yaml

   default_limit_objects: 1000
   models:
     datasets/example/Country:
       limit: 200

A model limit overrides the deployment default. The output format may impose
a smaller limit, such as the HTML display cap. Configure limits as positive
integers; zero and negative model limits are rejected. Set `default_limit_objects` to `null`
and remove model limits to disable enforcement.

The default can also be set with `SPINTA_DEFAULT_LIMIT_OBJECTS=1000`. Explicit
model limits and the default apply to both internal and external backends.
They only apply to models that support pagination. Output format limits still
apply when model pagination is unavailable.

Requesting data
---------------

When enforcement is enabled, streaming `getall` and `changes` requests must
include `limit()` or `page()`. For example:

.. code-block:: text

   /datasets/example/Country?limit(100)
   /datasets/example/Country?page()

Requests without either option return `LimitOrPageIsRequired`. An explicit
limit above the effective maximum returns `ExceededMaximumLimit`.
Use the response's `_page.next` page token to continue downloading data:
pass it to `page('token')` on the same model endpoint.
For changelog requests, continue from the last `_cid`
using `/:changes/<last_cid + 1>` with a permitted `limit()`.

Push state and keymap synchronization automatically retry with a smaller
limit when the remote server reports `ExceededMaximumLimit`.

Generating model limits
-----------------------

For internal PostgreSQL models, Spinta can estimate a row limit from stored
column sizes and JSON serialization overhead:

.. code-block:: console

   spinta admin model_limit
   spinta admin model_limit --output model-limits.yml

Without `--output`, YAML is written to stdout and progress messages to stderr.
Output files must have a `.yml` or `.yaml` extension. An existing file is
protected; use `--destructive` to overwrite it. Use `--input models.txt` to
restrict generation to model names listed one per line.

`default_limit_bytes` controls the target JSON size and defaults to `1g`:

.. code-block:: yaml

   default_limit_bytes: 10m

The generated file uses the same `models` configuration shown above. Review it
and explicitly load it alongside your deployment configuration:

.. code-block:: console

   spinta -o config=deployment.yml,model-limits.yml run

Generating the file does not enable limits in the running server. Empty models
and models whose estimated row limit reaches their row count are omitted.
Generated limits are at least one row. These are estimates, not strict byte
caps; an individual large row can exceed the target size.

Automatic calculation currently supports only internal PostgreSQL backends.
External models are skipped during generation; configure their limits manually
or use `default_limit_objects`.
