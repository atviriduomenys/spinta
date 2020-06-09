.. default-role:: literal

.. _config-auth:

Authentication
##############

Basic auth
----------

If you don't want to use OAuth for user authentication you can enable `HTTP
Basic Auth <RFC7617>`_. In order to enable HTTP Basic Auth,  you need to set
`http_basic_auth` configuration parameter to `true`.

.. _RFC7617: https://tools.ietf.org/html/rfc7617

When `http_basic_auth` is set to `true`, and if `default_auth_client` is not
set, then HTTP Basic Auth will be required for all requests.

Client name and secret will be used from `<config_path>/clients` directory.
