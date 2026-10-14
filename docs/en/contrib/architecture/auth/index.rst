.. default-role:: literal

####
Auth
####

In C4 terms (see :doc:`/contrib/architecture/index`), auth is a :ref:`component
<contrib-components>` of the web app container; identity providers that
publish JWKS keys are external systems at C4 :ref:`system context
<contrib-system-context>` level.

Spinta authorizes clients with `OAuth 2.0`_ (RFC 6749). The implementation
lives in `spinta/auth.py` and is built on the `authlib` package: Spinta
subclasses `authlib.oauth2.rfc6749.AuthorizationServer` (the authorization
server that issues tokens) and
`authlib.oauth2.rfc6749.ResourceProtector` (the resource-server side that
validates tokens on every request). The user-facing documentation is in
:doc:`/manual/access` — this page explains how it is implemented in code.

Spinta implements only the *client credentials* grant (RFC 6749 section 4.4):
a client is a registered application (a YAML file in the `clients`
directory), there is no end-user login or browser redirect. Access tokens
are signed JWTs (RS512 by default) issued and validated by Spinta itself.

Pages
*****

- :doc:`authorization-server` — the token endpoint, introspection and
  server metadata (``/auth/*`` routes).
- :doc:`resource-server` — token validation on every request, scopes
  and the authorization logic.
- :doc:`clients` — client registration files, keys and the CLI helpers.
- :doc:`access-token` — access token content (claims), creation,
  validation and public keys.


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

External specifications
***********************

- RFC 6749 — The OAuth 2.0 Authorization Framework
  (https://www.rfc-editor.org/info/rfc6749, local extract
  `.tmp/sources/ietf/RFC6749.md`): the roles (section 2: authorization
  server, resource server, client) and the client credentials grant
  (section 4.4) that Spinta implements; the `scope` parameter (section
  3.3) is the basis of the scope formats above.
- RFC 6750 — OAuth 2.0 Bearer Token Usage: the ``Authorization: Bearer``
  scheme validated by `BearerTokenValidator`.
- RFC 7662 — OAuth 2.0 Token Introspection: the ``/auth/introspect``
  endpoint (`IntrospectionEndpoint`).
- RFC 8414 — OAuth 2.0 Authorization Server Metadata: the
  ``/.well-known/oauth-authorization-server`` endpoint.
- RFC 7591 — OAuth 2.0 Dynamic Client Registration: the client metadata
  fields used on `Client` (`token_endpoint_auth_method`,
  ``grant_types``).

.. _OAuth 2.0: https://www.rfc-editor.org/info/rfc6749

.. toctree::
   :maxdepth: 1

   Authorization Server <authorization-server>
   Resource <resource-server>
   Clients <clients>
   Access token <access-token>
