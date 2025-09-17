.. default-role:: literal

Access control
##############

Spinta uses `OAuth 2.0 Authorization Framework`_ for client authorization.  In
order to access data you need to be registered in the `Authorization Server`_. 


Client registration
===================

Spinta provides a simple built-in `Authorization Server`_, A new client can be
registered like this::

    spinta client add -n client_id -s client_secret

Registered clients will be stored in `$SPINTA_CONFIG_PATH/clients/` directory as
YAML files. You can edit these files to manage client scopes.

If you use an external `Authorization Server`_, then you need to register
client there using whatever steps required by the authorization server.


Default client
==============

Spinta can be configured with a default client. Default client will be used
when access token is not given. Default client can be set using
`$SPINTA_DEFAULT_AUTH_CLIENT` configuration parameter and it should point to an
existing client in `$SPINTA_CONFIG_PATH/clients/`.

All unauthorized clients will be given default client permissions.

UDTS format Scopes
==================

Each client can be given list of scopes. Scopes names uses following pattern::

    {$SPINTA_SCOPE_PREFIX}{ns}/{Model}/@{property}/:{action}

`$SPINTA_SCOPE_PREFIX` can be set via configuration, default scope prefix is
`uapi:/`. `ns`, `Model` and `property` are all optional, only `action` is
required.


.. _available-actions-udts:

Following actions are available:

:getone:
  Client can get single object by id.

:getall:
  Client can get list of all objects, but can't use any search parameters.

:search:
  Client can get a list of all objects and use search parameters to filter objects.

:changes:
  Client can query whole model or single object changelog.

:create:
  Client can create new objects.

:update:
  Client can update existing objects by fully overwriting them.

:patch:
  Clients can update some properties of an existing objects by providing a patch.

:delete:
  Clients can do a soft delete, deleted objects will still be stored in
  changelog.

:wipe:
  Clients can do a hard delete, objects will be deleted permanently, without
  any trace in changelog. This is usually used in test environments and should
  not be used in production environments.


For example you can give read access to all data by giving client
these scopes::

    uapi:/:getone
    uapi:/:getall
    uapi:/:search

Or you can give read access to all models in a namespace::

    uapi:/geo/:getone
    uapi:/geo/:getall
    uapi:/geo/:search

Or you can give explicit read access to a model::

    uapi:/geo/Country/:getone
    uapi:/geo/Country/:getall
    uapi:/geo/Country/:search

Or to a property::

    uapi:/geo/Country/@code/:getone
    uapi:/geo/Country/@code/:getall
    uapi:/geo/Country/@code/:search

Old format scopes (Deprecated and will be removed)
==================================================

Each client can be given list of scopes. Scopes names uses following pattern::

    {$SPINTA_SCOPE_PREFIX}{ns}_{model}_{property}_{action}

`$SPINTA_SCOPE_PREFIX` can be set via configuration, default scope prefix is
`spinta_`. `ns`, `model` and `property` are all optional, only `action` is
required.

.. _available-actions:

Following actions are available:

:getone:
  Client can get single object by id.

:getall:
  Client can get list of all objects, but can't use any search parameters.

:search:
  Client can use search parameters to filter objects.

:changes:
  Client can query whole model or single object changelog.

:insert:
  Client can create new objects.

:upsert:
  Client can create or update existing objects using upsert operation.

:update:
  Client can update existing objects by fully overwriting them.

:patch:
  Clients can patch existing objects by providing a patch.

:delete:
  Clients can do a soft delete, deleted objects will still be stored in
  changelog.

:wipe:
  Clients can do a hard delete, objects will be deleted permanently, without
  any trace in changelog. This is usually used in test environments and should
  not be used in production environments.


For example you can give read access to all data by giving client
these scopes::

    spinta_getone
    spinta_getall
    spinta_search

Or you can give read access to all models in a namespace::

    spinta_geo_getone
    spinta_geo_getall
    spinta_geo_search

Or you can give explicit read access to a model::

    spinta_geo_country_getone
    spinta_geo_country_getall
    spinta_geo_country_search

Or to a property::

    spinta_geo_country_code_getone
    spinta_geo_country_code_getall
    spinta_geo_country_code_search


Access token
============

When you have a registered client with some scopes, then you can get access
token like this::

    http -a $client:$secret -f $server/auth/token grant_type=client_credentials scope="$scopes" | jq -r .access_token

Once you have an access token, then you can access data by passing token to
`Authorization` header like this::

    Authorization: Bearer $token


Access levels
=============

Access level can be set to models and properties in manifest YAML files. For
example:

.. code-block:: yaml

    type: model
    name: geo/country
    access: private
    properties:
      code:
        type: string
        access: private
      name:
        type: string
    
Here `country` model and `code` property have `access` set to `private`.

`access` can be one of following:

:private:
  Explicit model or property scope is required to access data. For example if
  client has `uapi:/geo/:getall` scope, `/geo/country` model data still can't
  be accessed, because model requires explicit `uapi:/geo/Country/:getall`
  scope. Same applies to properties. The only way to access `code` property is
  via subresource call `/geo/country/ID/code` and with explicit
  `uapi:/geo/Country/@code/:getall` scope.

  Private data can't be accessed directly, but can be used in filters or
  sorting.

  Do not confuse `private` access level, with `hidden` properties. `hidden`
  properties has nothing to do with authorization. `hidden` properties can only
  be accessed via subresources API.

:protected:
  Explicit scope is not required, model can be accessed if at least namespace
  scope is given and property can be accessed if at least model or namespace
  scope is given.

:public:
  Data can be accessed publicly, but access token is still required in order to
  check if user has read and accepted data usage terms and conditions. Default
  client `$SPINTA_DEFAULT_AUTH_CLIENT` can't be used to access data.

:open:
  Data can be accessed freely without any restrictions. Access token is not
  required if `$SPINTA_DEFAULT_AUTH_CLIENT` is set, scopes of the default
  client will be used.


.. _OAuth 2.0 Authorization Framework: https://tools.ietf.org/html/rfc6749
.. _Authorization Server: https://tools.ietf.org/html/rfc6749#section-1.1

.. _client-credentials:

Client credentials
==================

From client side, client credentials are stored in a `credentials.cfg` file
in :ref:`config_path`.

Here is an example `credentials.cfg` file:

.. code-block:: ini

    [client@example.com]
    server = https://example.com
    client = client
    secret = secret
    scopes =
      uapi:/:getall
      uapi:/:getone
      uapi:/:search
      uapi:/:changes

`credentials.cfg` is an `INI file`_. Each section of this file represents a
client credentials. Section is a name written between `[` and `]` symbols.
Section name can be in following forms:

.. _INI file: https://en.wikipedia.org/wiki/INI_file

client@host.name:port
    Client name, hostname and port.

client@host.name
    Client name and hostname.

host.name
    Just a hostname.

client:
    Just a client name.

It is a good idea to use `client@host.name` form, because, when you nee to
perform an operation on a remote Spinta instance, then client credentials
will be automatically found by comparing remote hostname and client. For
example if try to access remote Spinta using following URL::

    https://myclient@data.example.com/

Then client credentials will be looked up at `myclient@data.example.com`
section.

Client credentials will be used to get access token of that client.

In each section of `credentials.cfg` file you can use following parameters:

server
    Optional parameter, if not specified, server URL will be constructed from
    hostname in section. For example if section is `client@example.com`, then
    `server` will bet set to `https://example.com`.

client
    Client name.

secret
    Client secret.

scopes
    List of scopes to request in access token. Client must have all scopes on
    the server, if you request more scopes then available for this client, then
    you will get an error.



