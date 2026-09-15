.. default-role:: literal

.. _config-auth:

Authentication
##############

Basic auth
----------

If you don't want to use OAuth for user authentication you can enable `HTTP
Basic Auth`_. In order to enable HTTP Basic Auth,  you need to set
`http_basic_auth` configuration parameter to `true`.

.. _HTTP Basic Auth: https://datatracker.ietf.org/doc/html/rfc7617

When `http_basic_auth` is set to `true`, and if `default_auth_client` is not
set, then HTTP Basic Auth will be required for all requests.

Client name and secret will be used from `<config_path>/clients` directory.


Public verification keys download url (when spinta as Agent)
------------------------------------------------------------
If your server periodically rotates JWT public verification keys (also known as well-known or jwk), there is option to
use `token_validation_keys_download_url` values setting which once is set will retrieve and cache those keys from provided url.
Automatically handles cache wipe and refresh with new values.

Because these tokens are issued by an external server, set `token_issuer`
(see below) to the issuer identifier those tokens carry in their `iss` claim,
otherwise Spinta rejects them.

Example:

.. code-block:: yaml

    token_validation_keys_download_url: https://get-test.data.gov.lt/auth/token/.well-known/jwks.json
    token_issuer: https://get-test.data.gov.lt


Authorization server identifier (``token_issuer``)
--------------------------------------------------
`token_issuer` is the identifier of the authorization server that issues the
access tokens Spinta accepts — the value carried in the token's `iss` claim.
Spinta stamps it as the `iss` of the tokens it issues, and requires the `iss`
of tokens it validates to equal it; a token whose `iss` does not match is
rejected.

`token_issuer` is **required whenever Spinta issues or validates access
tokens** — that is, whenever it acts as an authorization server itself or
validates tokens from an external one. It has no default, and there is no
fallback to `server_url`: `server_url` is this server's public URL (often a
gateway in front of the resource server) and is not a valid issuer identity, so
using it as the `iss` would misconfigure every token. The requirement is
enforced at the moment a token is issued or validated (not at startup), so
operations that never touch tokens — inspecting a manifest, for example — do
not need it.

Set it to the authorization server's identifier:

- when validating tokens issued by an **external** authorization server (via
  `token_validation_key` or `token_validation_keys_download_url`), set it to
  that server's `iss` **exactly** — including a trailing slash if the issuer
  uses one (e.g. some providers publish ``https://issuer.example.com/``). The
  value is compared verbatim and is not normalised.
- when Spinta issues its **own** tokens, set it to Spinta's own authorization
  identity (the `iss` you want stamped on issued tokens).


Resource server identifier (``resource_server_url``)
----------------------------------------------------
`resource_server_url` is the identifier of this resource server — the value
carried in the token's `aud` (audience) claim. Spinta stamps it as the `aud`
of tokens it issues, and requires the `aud` of tokens it validates to contain
it; a token whose `aud` does not match is rejected. This is how Spinta ensures
a token was actually issued for *this* resource server and not some other one.

`resource_server_url` is **required whenever Spinta issues or validates access
tokens**, alongside `token_issuer`, and is enforced at the moment a token is
issued or validated rather than at startup. It has no default and, as with
`token_issuer`, there is no fallback to `server_url`: `server_url` is the public
URL (often a gateway in front of this resource server) and is not a valid
audience identity. Set it to the identifier the authorization server uses for
this resource server (its resource indicator). The value is compared verbatim
and is not normalised.

