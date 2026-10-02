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

Because these tokens are issued by an external server, set `auth_server_id`
(see below) to the issuer identifier those tokens carry in their `iss` claim,
otherwise Spinta rejects them.

Example:

.. code-block:: yaml

    token_validation_keys_download_url: https://get-test.data.gov.lt/auth/token/.well-known/jwks.json
    auth_server_id: https://get-test.data.gov.lt


Identifiers and URLs
--------------------
Spinta keeps the identifier of a server apart from its URL. An identifier is
the value carried in token claims and compared verbatim (it is not
normalised); a URL is where the server is reached.

.. code-block:: yaml

    auth_server_id: https://get-test.data.gov.lt
    auth_server_url: https://get-test.data.gov.lt
    resource_server_id: https://data.gov.lt/id/dcat/Agent/<uuid>
    resource_server_url: https://agent.example.com

`auth_server_id`
    Identifier of the authorization server that issues the access tokens
    Spinta accepts: the token's `iss` claim. Spinta stamps it as the `iss` of
    the tokens it issues and requires the `iss` of tokens it validates to equal
    it. When validating tokens of an **external** authorization server, set it
    to that server's `iss` **exactly**, including a trailing slash if the issuer
    uses one.

`auth_server_url`
    URL of the authorization server. When Spinta is its own authorization
    server, its metadata (`/.well-known/oauth-authorization-server`) advertises
    the token, introspection and JWKS endpoints under this URL.

`resource_server_id`
    Identifier of this resource server: the token's `aud` (audience) claim.
    Spinta requires the `aud` of tokens it validates to contain it, so a token
    issued for another resource server is rejected. Tokens Spinta issues carry
    `aud` as a list: the `resource` parameters of the token request (`RFC 8707`_)
    or, without them, `[resource_server_id]`.

`resource_server_url`
    Public URL of this resource server, which may be a gateway in front of it.
    It is used to build absolute links, for example the `Location` header of
    created objects. `server_url` is its old name and is still read when
    `resource_server_url` is not set.

`auth_server_id` and `resource_server_id` are **required whenever Spinta
issues or validates access tokens**. They have no default and do not fall back
to the URLs, because a URL (often a gateway) is not a valid identity. The
requirement is enforced at the moment a token is issued or validated (not at
startup), so operations that never touch tokens do not need them.

.. _RFC 8707: https://datatracker.ietf.org/doc/html/rfc8707


Catalog credentials (``credentials.cfg``)
-----------------------------------------
`spinta sync` reads the Catalog credentials from the `[katalogas]` section of
`credentials.cfg`, or from the old `[default]` section when there is none:

.. code-block:: ini

    [katalogas]
    auth_server_url = https://get-test.data.gov.lt
    resource_server_url = https://data.gov.lt/uapi/
    resource_server_id = https://data.gov.lt/uapi/
    client = <client>
    secret = <secret>
    scopes =
        uapi:/datasets/gov/vssa/ror/dcat/Dataset/:getall

`auth_server_url` and `resource_server_url` replace the old `server` and
`resource_server` options, which are still read when the new ones are not set.
`resource_server_id` is sent as the `resource` parameter of the token request,
so the authorization server issues a token for the Catalog.
