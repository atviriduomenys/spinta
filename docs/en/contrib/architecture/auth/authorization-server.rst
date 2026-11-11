.. default-role:: literal

####################
Authorization server
####################

Authorization server
********************

`spinta/auth.py:AuthorizationServer` extends authlib's
`rfc6749.AuthorizationServer`:

- registers the `ClientCredentialsGrant` (RFC 6749 section 4.4);
- registers a `BearerToken` generator (`rfc6750.BearerToken`) whose
  access token is created by
  `spinta/auth.py:create_access_token()` — a JWT with claims `iss`
  (`config.token_issuer`), `aud` (`config.resource_server`), `sub` and
  `client_id` (the client), `iat`/`exp` (tokens expire in 10 days),
  `scope` and `jti`;
- registers the introspection endpoint
  `spinta/auth.py:IntrospectionEndpoint` (RFC 7662), used by
  `POST /auth/introspect`;
- reads the private token signing key at startup
  (`spinta/auth.py:load_key()`, `KeyType.private` — a JWK dict from
  `config_path/keys/private.json`, or nothing when public keys are
  provided via configuration, in which case `enabled()` returns `False`
  and `POST /auth/token` raises `NoAuthServer`).

The server is bound into the context as ``auth.server`` when the ASGI app
is created (`spinta/api/__init__.py`) and is used by these API routes
(see `spinta/api/__init__.py`):

- `POST /auth/token` — `auth_token()` calls
  `AuthorizationServer.create_token_response()` with a
  `StarletteOAuth2Request` built from `StarletteOAuth2Data` (a small
  adapter converting Starlette request data to authlib's `OAuth2Request`).
- `POST /auth/introspect` — `auth_introspect()`; the client must be
  authenticated with `client_secret_basic` and must have the
  ``auth_introspect`` scope (`Scopes.AUTH_INTROSPECT`).
- `/.well-known/jwks.json` — `get_verification_keys()` returns the public
  key(s) (`JWKS`).
- `/.well-known/oauth-authorization-server` —
  `authorization_server_metadata()` returns RFC 8414 metadata
  (`ClientCredentialsServerMetadata`).

