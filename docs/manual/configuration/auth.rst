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
----------
If your server periodically rotates JWT public verification keys (also known as well-known or jwk), there is option to
use `token_validation_keys_download_url` values setting which once is set will retrieve and cache those keys from provided url.
Automatically handles cache wipe and refresh with new values.

Example:

.. code-block:: yaml

    token_validation_keys_download_url: https://get-test.data.gov.lt/auth/token/.well-known/jwks.json

