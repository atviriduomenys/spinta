.. default-role:: literal

###############
Resource server
###############

In OAuth 2.0 terms (RFC 6749 section 1.1) the resource server is the
server hosting the protected resources, capable of accepting and
responding to protected resource requests using access tokens. In
Spinta this role is implemented by `spinta/auth.py:ResourceProtector`
together with the `authorize` command overloads that check scopes on
every node access — see :doc:`index` for the overview and
:doc:`access-token` for the token itself.

`spinta/auth.py:ResourceProtector` extends authlib's
`rfc6749.ResourceProtector` and is bound to the context as
``auth.resource_protector``. It is configured with a single validator,
`spinta/auth.py:BearerTokenValidator`, which:

- decodes the JWT with one of the loaded *public* keys — the key is
  selected by the token header ``kid`` or by matching the algorithm
  (`ALLOWED_JWT_ALGORITHMS` — `RS256`, `RS384`, `RS512`, `ES256`,
  `ES384`, `ES512`);
- validates claims with `JWTClaimsRegistry`: `iss` must equal
  `config.token_issuer`, `aud` must equal `config.resource_server`, and
  `client_id`, `exp`, `iat` are required;
- returns a `spinta/auth.py:Token` (an `rfc6749.TokenMixin` subclass)
  with `get_scope()`, `valid_scope()`, `check_scope()` etc.

Public keys come from (in `spinta/auth.py:load_all_public_keys()`):

1. `config.token_validation_key` — a JWK or a JWKS ``{"keys": [...]}``
   dict set via configuration (mutually exclusive with option 2, checked
   at config load in `spinta/types/config.py`);
2. `config.token_validation_keys_download_url` — keys are downloaded
   from a remote JWKS endpoint (`download_and_store_public_keys()`) and
   stored in `config.downloaded_public_keys_file`;
3. otherwise the local key pair in `config_path/keys/` — generated with
   ``spinta key generate`` (`spinta/auth.py:gen_auth_server_keys()`).

The token itself is attached to the request context as ``auth.token``
by `spinta/api/__init__.py:_auth_client_context()` (via
`spinta/auth.py:get_auth_token()`); HTTP Basic client credentials can be
exchanged for a token on the fly (`get_token_from_http_basic_auth()`),
and a default client (`config.default_auth_client`) is used for
unauthenticated requests.



Scopes
******

Scopes are strings granting an *action* on a *node*. Two scope formats
are generated for every authorization check — the old format is
deprecated but still accepted (`Token.check_scope()` warns when a token
uses it):

- old format (deprecated): ``{scope_prefix}{name}_{action}``, where
  `scope_prefix` is ``spinta_`` by default — e.g.
  ``spinta_geo_country_getall``;
- UAPI format: ``{scope_prefix_udts}{name}/:{action}``, where
  `scope_prefix_udts` is ``uapi:/`` by default — e.g.
  ``uapi:/geo/Country/:getall``.

The scope name is produced by `spinta/auth.py:get_scope_name()` — the
default value of `config.scope_formatter` (a `ScopeFormatterFunc`
(`spinta/components.py`) taking `(context, node, action, is_udts)`). It
builds the scope from a template, replaces it with a shortened, hashed
variant if it exceeds `config.scope_max_length` (60 characters), and
sanitizes characters (lowercase, non-alphanumerics collapsed to ``_``)
in `spinta/utils/scopes.py:name_to_scope()`. In the UAPI format the
``insert`` action is spelled ``:create``. A custom scope formatter can
be plugged in via `config.scope_formatter` (e.g. the internal SQL
manifest defines `internal_scope_formatter` in
`spinta/manifests/internal_sql/commands/auth.py`).

Depending on the node type, the scope covers:

- a `Property` — ``uapi:/ns/Model/@place/:action`` (old format:
  ``spinta_ns_Model_place_action``);
- a `Model` — ``uapi:/ns/Model/:action``;
- a `Namespace` — ``uapi:/ns/:action``, which covers all models under
  the namespace (parent namespaces are checked too, see below);
- ``None`` (the configured `Scopes` values below) —
  ``uapi:/:auth_clients``, ``uapi:/:inspect`` and so on.

Available actions are `Action` enum values
(`spinta/core/enums.py`): `getone`, `getall`, `search`, `changes`,
`create` (from ``insert``), `update`, `patch`, `delete`, `wipe`,
`upsert`, `move`, `check`, `inspect` — see the action lists in
:doc:`/manual/access`.

Beside the data scopes, `spinta/auth.py:Scopes` defines special scopes
that are not tied to a node: ``auth_clients`` (manage client files via
API), ``auth_introspect``, ``client_backends_update_self``, ``inspect``,
``schema_write`` and ``set_meta_fields``.

Authorization logic
*******************

The core check is `spinta/auth.py:authorized(context, node, action)`:

1. if `config.access` is higher than the node's `access`
   (`Access` enum in `spinta/core/enums.py`), access is denied
   (`ModelNotFound` when `throw=True`);
2. unauthenticated requests (the default client) are denied unless
   `config.access` is `open`;
3. the required scope list is built from the node and its parents: for a
   property also its model and namespace (unless the property is
   ``hidden``), for a model also its namespace, then all parent
   namespaces via `ns.parents()`;
4. the client token must satisfy *at least one* of the generated scopes
   (`token.valid_scope()`), each in both formats above;
5. if `config.check_contract_scopes` is on, for nodes with access lower
   than `open` the token scopes must also be listed in the client's
   ``contract_scopes`` (see `Token.check_contract_scopes()` and
   `Client.get_all_contract_scopes()`).

`authorized()` is called from the `authorize` command overloads (e.g.
`spinta/types/model.py:authorize(Context, Action, Model)`, per property
and namespace too) whenever an API operation runs, and from helper
places like `spinta/datasets/helpers.py:authorized_or_system_request()`
(system-generated requests bypass the check).


See also
********

- :doc:`/manual/access` — user-facing scopes and access level
  documentation.
- :doc:`/contrib/architecture/manifests/logical/models` — where model
  scopes and access levels come from.
- :doc:`index` — how authorization fits into the request flow.
