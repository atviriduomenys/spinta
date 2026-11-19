.. default-role:: literal

#######
Clients
#######

Registered clients are YAML files stored under ``config_path/clients``
(`spinta/auth.py:get_clients_path()` and `query_client()`):

- ``clients/helpers/keymap.yml`` — maps client names to client ids;
- ``clients/id/<xx>/<xx>/<uuid>.yml`` — one file per client, sharded by
  the first four UUID characters (`get_client_file_path()`).

A `spinta/auth.py:Client` (an authlib `rfc6749.ClientMixin` subclass)
holds ``client_id``, ``client_name``, ``client_secret_hash``, ``scopes``,
``backends`` and ``contract_scopes``; auth method is
``client_secret_basic`` and the only grant is ``client_credentials``
(RFC 7591 client metadata). Secrets are stored as password hashes
(`spinta/utils/passwords.py`). Client files are managed with the CLI
(`spinta/cli/auth.py`):

- ``spinta client add`` — register a new client file;
- ``spinta key generate`` — generate the ``keys/private.json`` +
  ``keys/public.json`` pair (`gen_auth_server_keys()`, private key file
  permissions ``0o600``);
- ``spinta key download`` — download remote JWKS keys
  (`token_validation_keys_download_url`);
- ``spinta token get`` / ``spinta token decode`` — fetch a token from a
  server (using `credentials.cfg`, see :doc:`/manual/access`) or decode
  a token locally with the public key.

Testing helpers
***************

Tests do not need a running authorization server: the test `app`
fixture (`spinta/testing/client.py:TestClient`) provides:

- ``app.authorize([scope, ...])`` — sets the ``Authorization: Bearer``
  header with a token created directly with the private key
  (`spinta/auth.py:create_access_token()`); calling it multiple times
  accumulates scopes (unless ``strict_set=True``), and ``creds=`` can
  instead request a token through ``POST /auth/token`` with client
  credentials.
- ``app.authmodel(model, actions)`` — resolves scopes for a model with
  the `commands.get_model_scopes` command
  (`spinta/commands/auth.py`, generating both scope formats) and calls
  ``authorize()``. For example
  ``app.authmodel('Country', ['getall'])`` grants both
  ``uapi:/geo/Country/:getall`` and ``spinta_geo_country_getall``.
- ``app.unauthorize()`` — clears the scopes and the header.


See also
********

- :doc:`/manual/access` — user-facing client registration and scope
  configuration guide.
- :doc:`index` — auth overview, authorization server and testing
  helpers.
