.. default-role:: literal

############
Access token
############

An access token is a signed JWT — the bearer token used by clients on
every data request (see :doc:`index` for how it is attached to the
request context as ``auth.token``). Spinta implements the
`client credentials`_ grant of `RFC 6749`_: the client authenticates
with its client secret (see :doc:`clients`), and the authorization
server responds with an access token as specified in RFC 6749 section
5.1 — a `Bearer` token, RFC 6750. No user login or browser redirect is
involved; there is no refresh token either (RFC 6749 section 4.4.3 says
a refresh token ``SHOULD NOT`` be included for this grant).

.. _client credentials: /contrib/architecture/auth/authorization-server.html
.. _RFC 6749: https://www.rfc-editor.org/info/rfc6749

Token creation
**************

Access tokens are created by `spinta/auth.py:create_access_token()` /
`create_client_access_token()` — signed with the private RSA key
(`config_path/keys/private.json`; RS512 header by default) with the
following claims:

- `iss` — the token issuer (`config.token_issuer`).
- `aud` — the resource server audience (`config.resource_server`).
- `sub` and `client_id` — the client acting on its own behalf (Spinta
  issues no per-user tokens; there is no user representative `sub`
  different from the client).
- `iat` / `exp` — issued at and expiration; `exp - iat` is the
  lifetime, which the authorization server sets to 10 days
  (`AuthorizationServer._get_expires_in`).
- `scope` — the space-separated list of scopes granted to the client
  (see :doc:`/manual/access` for the scope format).
- `jti` — a unique token id (a random UUID); used as the access log
  token reference (`accesslog.token`, see :doc:`/contrib/architecture/access-log/index`).

The issued token is returned by `POST /auth/token` as a JSON object
``{"access_token": ..., "token_type": "Bearer", "expires_in": ...}``
(translated by authlib into an HTTP response by
`AuthorizationServer.handle_response()`).

Token validation
****************

On every request, `spinta/auth.py:BearerTokenValidator` decodes the JWT
with one of the loaded *public* keys:

- the key is selected by the token header ``kid``, or by matching the
  algorithm — `ALLOWED_JWT_ALGORITHMS` allows RS256, RS384, RS512,
  ES256, ES384 and ES512 (see `spinta/auth.py:decode_kty_from_alg()`);
- claims are validated with a `joserfc` `JWTClaimsRegistry`: `iss` must
  equal `config.token_issuer`, `aud` must equal
  `config.resource_server`, and `client_id`, `exp` and `iat` are
  required;
- the result is a `spinta/auth.py:Token` object (an authlib
  `rfc6749.TokenMixin` subclass) with `get_scope()`, `valid_scope()`,
  `check_scope()` and similar accessors; `Token.check_scope()` warns
  when a token still uses the deprecated ``spinta_*`` scope format
  (see :doc:`/contrib/architecture/auth/resource-server`).

Public keys
***********

Public keys are loaded by `spinta/auth.py:load_all_public_keys()`, in
this order:

1. `config.token_validation_key` — a JWK or a JWKS ``{"keys": [...]}``
   dict set via configuration (mutually exclusive with option 2,
   checked at config load in `spinta/types/config.py`);
2. `config.token_validation_keys_download_url` — keys are downloaded
   from a remote JWKS endpoint (`download_and_store_public_keys()`) and
   stored in `config.downloaded_public_keys_file`;
3. otherwise the local key pair in `config_path/keys/` — generated with
   ``spinta key generate`` (`spinta/auth.py:gen_auth_server_keys()`).

CLI helpers
***********

- ``spinta key generate`` — generate the ``keys/private.json`` +
  ``keys/public.json`` pair (`gen_auth_server_keys()`, private key file
  permissions ``0o600``);
- ``spinta key download`` — download remote JWKS keys
  (`token_validation_keys_download_url`);
- ``spinta token get`` / ``spinta token decode`` — fetch a token from a
  server (using `credentials.cfg`, see :doc:`/manual/access`) or decode
  a token locally with the public key.

Introspection
*************

`POST /auth/introspect` (RFC 7662, implemented by
`spinta/auth.py:IntrospectionEndpoint`) lets a client holding the
``auth_introspect`` scope query the claims of a token (`sub`, `aud`,
`client_id`, `exp`, `iat`, `scope`). The JWT payload — what
introspection returns — is also what UAPI sees: the payload must
conform to `RFC 9068`_ (the JWT profile for OAuth 2.0 access tokens),
with the scope claim carrying UAPI scopes.

.. _RFC 9068: https://datatracker.ietf.org/doc/html/rfc9068

External specifications
***********************

- RFC 6749 (https://www.rfc-editor.org/info/rfc6749, local extract
  `.tmp/sources/ietf/RFC6749.md`) — section 4.4 (client credentials
  grant), 5.1 (successful token response) and 3.3 (the `scope`
  parameter).
- RFC 6750 — OAuth 2.0 Bearer Token Usage: the ``Authorization: Bearer``
  scheme.
- RFC 7662 — token introspection.
- RFC 9068 — JWT profile for OAuth 2.0 access tokens (the claim set
  above matches it).
- UAPI (https://ivpk.github.io/uapi, local extract
  `.tmp/sources/uapi/UAPI.md`) — the UAPI authorization flow consumes
  exactly such tokens: JWT access tokens with `jti`, `iss`, `aud`,
  `exp`, `iat`, `sub`, `client_id` and UAPI `scope` claims.

See also
********

- :doc:`index` — auth overview, resource protector binding and testing
  helpers.
- :doc:`resource-server` — where the token claims are checked and how
  scopes are evaluated.
- :doc:`/manual/access` — user-facing token and client documentation.
