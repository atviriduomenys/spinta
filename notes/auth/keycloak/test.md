# Spinta × KeyCloak end-to-end test

Spinta is run in external manifest mode (`access: protected`) against an
external KeyCloak authorization server started with
`docker-compose.keycloak.yml`. This document exercises:

1. docker compose up + KeyCloak readiness
2. KeyCloak realm/client/scope/user setup via Admin API
3. OpenID well-known metadata
4. SQLite external source seeding
5. `spinta key generate`, `spinta key download`, client setup, `spinta run`
6. unauthenticated request rejection
7. client credentials flow (KeyCloak as authorization server, Spinta as
   resource server)
8. authorization code flow + refresh_token grant
9. data queries with Bearer token
10. token introspection
11. token revocation

## Running

If you development environement does not have all the required tools from
`flake.nix` use development shell:

```
nix develop
```

Hint: run `make env` first, so poetry creates the `.venv` used inside the nix
shell.

All code blocks are executed by `notes/scripts/markout.py`:

```
poetry run python notes/scripts/markout.py notes/auth/keycloak/test.md
```

Or via the `markout` alias from `flake.nix`:

```
nix develop -c markout notes/auth/keycloak/test.md
```

Pass `--dry-run` to only list the detected code blocks without executing
anything, and `--no-write` to keep the file untouched. The script writes the
outputs back in place; if any block fails, the run stops at the first failure
and the file is not written.

To keep KeyCloak containers (and the realm data volume) running for manual
inspection after the test, comment out the `docker compose ... down` line in
the teardown block.

## Constants

All paths are relative to the repository root, so everything must run from
the repo root.

```nu
const DIR = "notes/auth/keycloak"

# Resource server
const SPINTA = "http://localhost:8000"
const RESOURCE = $SPINTA
const INSTANCE = "var/instances/auth/keycloak"

# Authorization server
const KEYCLOAK = "http://localhost:8080"
const REALM = "spinta"
const REDIRECT_URI = "http://127.0.0.1:8081/callback"
```

## Setup

Source the test library and point Spinta at the external-mode config. Note
that `SPINTA_CONFIG_PATH` (the directory where Spinta stores its runtime
state: keys, clients, ...) is the same as `$INSTANCE`, so all generated state
is kept in one place, ignored by git via `/var/`.

```nu
source $"($DIR)/test.nu"
load-env {
    SPINTA_CONFIG: $"($DIR)/config.yml",
    SPINTA_CONFIG_PATH: $INSTANCE,
}
```

## Preflight

Check that we are in the repository root and that `docker` and `nix` are
available.

```nu
assert ("flake.nix" | path exists)
assert ($"($DIR)/config.yml" | path exists)
docker --version
nix --version
```

## Load credentials

Parse `credentials.cfg` and check that all required values are present in all
sections.

```nu
let creds = open --raw $"($DIR)/credentials.cfg" | from ini
assert ($creds.client.server == $KEYCLOAK)
assert ($creds.user.server == $KEYCLOAK)
assert ("client" in $creds.client)
assert ("secret" in $creds.client)
assert ("scopes" in $creds.client)
assert ("user" in $creds.user)
assert ("secret" in $creds.user)
assert ("scopes" in $creds.user)
assert ("client" in $creds.spinta)
assert ("secret" in $creds.spinta)
assert ("scopes" in $creds.spinta)
print ($creds | select client.client user.user spinta.client)
```

<!-- output: -->
╭───────────────┬────────╮
│ client.client │ client │
│ user.user     │ user   │
│ spinta.client │ spinta │
╰───────────────┴────────╯
<!-- output:end -->

## Clean runtime state

Remove the instance directory with all the generated runtime state: keys
contain private material, client files contain secrets, everything there must
not survive between runs. Also stop a Spinta server left over from an aborted
previous run.

```nu
ps | where name == "spinta" | each {|p| kill --force $p.pid} | ignore
rm -rf $INSTANCE
```

## Start KeyCloak

Bring up the KeyCloak containers with docker compose:

```nu
docker compose -f docker-compose.keycloak.yml up -d
```

<!-- output:code -->
```
[+] up 1/1
 ✔ Container spinta-keycloak-1 Running                                                                                                                                                                                                      0.0s
```
<!-- output:end -->

Wait until the master realm is ready:

```nu
poll-url $"($KEYCLOAK)/realms/master/.well-known/openid-configuration" 90
```

## KeyCloak setup

The realm, two clients (the `client` client and the `spinta` resource server
client), the client scope, the resource indicator mapper, the default client
scope removal and the user are handled via the KeyCloak Admin API. Each step
is a separate request below, with its response shown right after the code.

### Authenticate as the admin

Get an Admin API token via the password grant of the built-in `admin-cli`
client. The token is kept in the `$admin` header record and is not printed —
it is a secret.

```nu
let admin = admin-auth $KEYCLOAK
let base = $"($KEYCLOAK)/admin/realms/($REALM)"
```

### Recreate the realm

The realm is deleted on every run, so the test is idempotent. The first run
gets a `404`, subsequent runs a `204`.

```nu
let resp = http delete -e -f -H $admin $base
$resp | to-http
```

<!-- output:http -->
```http
HTTP/1.1 204 No Content
x-robots-tag: none
referrer-policy: no-referrer
x-content-type-options: nosniff
strict-transport-security: max-age=31536000; includeSubDomains
```
<!-- output:end -->

### Create the realm

`sslRequired: none` lets KeyCloak work over plain HTTP (cookies without the
`Secure` attribute).

```nu
let resp = http post -e -f -H $admin -t application/json $"($KEYCLOAK)/admin/realms" {
    realm: $REALM,
    enabled: true,
    sslRequired: none
}
assert ($resp.status == 201)
$resp | to-http
poll-url $"($KEYCLOAK)/realms/master/.well-known/openid-configuration"
```

<!-- output:http -->
```http
HTTP/1.1 201 Created
x-content-type-options: nosniff
referrer-policy: no-referrer
location: http://localhost:8080/admin/realms/spinta
strict-transport-security: max-age=31536000; includeSubDomains
x-robots-tag: none
x-frame-options: SAMEORIGIN
content-length: 0
```
<!-- output:end -->

### Create the client

The client is confidential, with service accounts (client_credentials grant)
and standard flow (authorization code grant) enabled, direct access (password
grant) disabled. Afterwards its id is fetched into `$cid` for later steps.

```nu
let resp = http post -e -f -H $admin -t application/json $"($base)/clients" {
    clientId: $creds.client.client,
    protocol: "openid-connect",
    secret: $creds.client.secret,
    publicClient: false,
    serviceAccountsEnabled: true,
    standardFlowEnabled: true,
    directAccessGrantsEnabled: false,
    redirectUris: [$REDIRECT_URI]
}
assert ($resp.status == 201)
$resp | to-http
```

<!-- output:http -->
```http
HTTP/1.1 201 Created
location: http://localhost:8080/admin/realms/spinta/clients/3cb14b72-15f6-49b7-ad3a-283e0cc1bdc1
content-length: 0
x-robots-tag: none
strict-transport-security: max-age=31536000; includeSubDomains
referrer-policy: no-referrer
x-frame-options: SAMEORIGIN
x-content-type-options: nosniff
```
<!-- output:end -->

```nu
let cid = (
    http get -e -f -H $admin $"($base)/clients?clientId=($creds.client.client)"
    | get body
    | first
    | get id
)
$cid
```

<!-- output: -->
3cb14b72-15f6-49b7-ad3a-283e0cc1bdc1
<!-- output:end -->


### Create the resource server client

A separate confidential client named exactly like the resource indicator
(`spinta`). All flows are disabled — the client is never used to log anyone
in. Its only role is to be named in the token `aud` claim as the resource
server (see the audience mapper below) and to authenticate against the
introspection endpoint: KeyCloak allows a client to introspect tokens whose
`aud` contains the client id, so tokens carrying this client in `aud` can be
introspected with its credentials, without disabling the audience check.
Afterwards its id is fetched into `$rid` for later steps.

```nu
let resp = http post -e -f -H $admin -t application/json $"($base)/clients" {
    clientId: $creds.spinta.client,
    protocol: "openid-connect",
    secret: $creds.spinta.secret,
    publicClient: false,
    serviceAccountsEnabled: false,
    standardFlowEnabled: false,
    directAccessGrantsEnabled: false
}
assert ($resp.status == 201)
$resp | to-http
```

<!-- output:http -->
```http
HTTP/1.1 201 Created
content-length: 0
referrer-policy: no-referrer
location: http://localhost:8080/admin/realms/spinta/clients/bc409e8c-fea1-4afc-8bf1-f4749a31c8db
x-content-type-options: nosniff
x-robots-tag: none
x-frame-options: SAMEORIGIN
strict-transport-security: max-age=31536000; includeSubDomains
```
<!-- output:end -->

```nu
let rid = (
    http get -e -f -H $admin $"($base)/clients?clientId=($creds.spinta.client)"
    | get body
    | first
    | get id
)
$rid
```

<!-- output: -->
bc409e8c-fea1-4afc-8bf1-f4749a31c8db
<!-- output:end -->

### Create the client scope

A client scope named exactly like the Spinta scope (`uapi:/:getall`), so it
can be attached to the client in the next step.

```nu
let resp = http post -e -f -H $admin -t application/json $"($base)/client-scopes" {
    name: "uapi:/:getall",
    protocol: "openid-connect"
}
assert ($resp.status == 201)
$resp | to-http
```

<!-- output:http -->
```http
HTTP/1.1 201 Created
x-robots-tag: none
x-content-type-options: nosniff
location: http://localhost:8080/admin/realms/spinta/client-scopes/1b3d6de3-2ec0-4363-bab3-00cda03b43fa
content-length: 0
strict-transport-security: max-age=31536000; includeSubDomains
x-frame-options: SAMEORIGIN
referrer-policy: no-referrer
cache-control: no-cache
```
<!-- output:end -->

```nu
let sid = (
    http get -e -f -H $admin $"($base)/client-scopes"
    | get body
    | where name == "uapi:/:getall"
    | first
    | get id
)
$sid
```

<!-- output: -->
1b3d6de3-2ec0-4363-bab3-00cda03b43fa
<!-- output:end -->

### Assign the scope as an optional client scope

The scope is attached to the client as an *optional* client scope. Optional
client scopes are not included in tokens automatically — the client must
always request them via the `scope` parameter when getting an access token.

```nu
let resp = http put -e -f -H $admin $"($base)/clients/($cid)/optional-client-scopes/($sid)" ""
assert ($resp.status == 204)
$resp | to-http
```

<!-- output:http -->
```http
HTTP/1.1 204 No Content
strict-transport-security: max-age=31536000; includeSubDomains
cache-control: no-cache
referrer-policy: no-referrer
x-robots-tag: none
x-content-type-options: nosniff
```
<!-- output:end -->

### Map the resource indicator to the token audience

Tokens must be issued for a specific resource server: per RFC 8707 the client
must pass a `resource` parameter — an identifier of Spinta, the resource
server — with every authorization and token request, and the resource value
must be added to the token `aud` claim. KeyCloak validates the `resource`
parameter at the authorization endpoint — it must be an `http(s)` URL — but
does not map it to the token `aud` claim yet
([keycloak/keycloak#14355](https://github.com/keycloak/keycloak/issues/14355)),
so the audience is added by an audience mapper — the workaround recommended in
the KeyCloak documentation. The mapper is attached to the `client` client and
adds the `spinta` resource server client (created above) as the token
audience.

```nu
let resp = http post -e -f -H $admin -t application/json $"($base)/clients/($cid)/protocol-mappers/models" {
    name: "resource-indicator",
    protocol: "openid-connect",
    protocolMapper: "oidc-audience-mapper",
    consentRequired: false,
    config: {
        "included.client.audience": $creds.spinta.client,
        "id.token.claim": false,
        "access.token.claim": true,
        "lightweight.claim": false
    }
}
assert ($resp.status == 201)
$resp | to-http
```

<!-- output:http -->
```http
HTTP/1.1 201 Created
x-robots-tag: none
content-length: 0
strict-transport-security: max-age=31536000; includeSubDomains
cache-control: no-cache
x-content-type-options: nosniff
x-frame-options: SAMEORIGIN
location: http://localhost:8080/admin/realms/spinta/clients/3cb14b72-15f6-49b7-ad3a-283e0cc1bdc1/protocol-mappers/models/f3756fe2-2f1f-43b5-930a-c5ac468a1175
referrer-policy: no-referrer
```
<!-- output:end -->

### Remove the roles default client scope

The `roles` default client scope adds the `realm_access` and `resource_access`
role claims to every token, none of which are needed here. Removing it from
the client also removes the roles (`account`) audience from the token `aud`
claim and the `roles` scope from the issued scope list, so tokens carry only
the actually used claims. The `roles` scope stays a realm-wide default — it is
detached from this client only.

```nu
let resp = http get -e -f -H $admin $"($base)/clients/($cid)/default-client-scopes"
$resp | to-http
let roles_sid = (
    $resp.body
    | where name == "roles"
    | first
    | get id
)
```

<!-- output:http -->
```http
HTTP/1.1 200 OK
content-type: application/json;charset=UTF-8
x-frame-options: SAMEORIGIN
cache-control: no-cache
strict-transport-security: max-age=31536000; includeSubDomains
referrer-policy: no-referrer
content-length: 444
x-content-type-options: nosniff
x-robots-tag: none

[
  {
    "id": "2827745f-ad8e-4d4c-b292-49f13fbcd598",
    "name": "web-origins"
  },
  {
    "id": "6a92a11c-1560-4956-82a3-072398cb9fde",
    "name": "service_account"
  },
  {
    "id": "6e4cc0b8-224f-4922-9567-e9c2e09078b7",
    "name": "acr"
  },
  {
    "id": "6850c6fd-3a48-4519-98f1-286fa62f8a01",
    "name": "roles"
  },
  {
    "id": "c6debad0-3932-4d40-81f5-4b056e1c0725",
    "name": "profile"
  },
  {
    "id": "fdb34033-e881-46ae-a740-5201abad65a6",
    "name": "basic"
  },
  {
    "id": "993127cd-66c7-433e-b9cd-948dbd635f83",
    "name": "email"
  }
]
```
<!-- output:end -->


```nu
let resp = http delete -e -f -H $admin $"($base)/clients/($cid)/default-client-scopes/($roles_sid)"
assert ($resp.status == 204)
$resp | to-http
```

<!-- output:http -->
```http
HTTP/1.1 204 No Content
referrer-policy: no-referrer
cache-control: no-cache
x-robots-tag: none
x-content-type-options: nosniff
strict-transport-security: max-age=31536000; includeSubDomains
```
<!-- output:end -->

### Create a user

A user with a password is created for the authorization code flow.

```nu
let resp = http post -e -f -H $admin -t application/json $"($base)/users" {
    username: $creds.user.user,
    enabled: true,
    emailVerified: true
}
assert ($resp.status == 201)
$resp | to-http
let uid = (
    http get -e -f -H $admin $"($base)/users?username=($creds.user.user)&exact=true"
    | get body
    | first
    | get id
)
```

<!-- output:http -->
```http
HTTP/1.1 201 Created
content-length: 0
location: http://localhost:8080/admin/realms/spinta/users/6d28dd71-037f-48da-a2a8-d9e419867431
x-content-type-options: nosniff
strict-transport-security: max-age=31536000; includeSubDomains
x-frame-options: SAMEORIGIN
x-robots-tag: none
referrer-policy: no-referrer
```
<!-- output:end -->

### Set the user password

The password cannot be set together with the user: the create-user endpoint
(`POST /admin/realms/{realm}/users`) silently ignores the `credentials` field
of the user representation, so a password passed at creation is never stored
and the later login (the authorization code flow step below) would fail with
invalid credentials. That is also why the user id is fetched into `$uid` in
the previous step — it is needed for the dedicated reset-password endpoint
that sets credentials on an existing user. `temporary: false` marks the
password as permanent, otherwise KeyCloak would additionally require the
user to change it on first login, which would break the scripted login.

```nu
let resp = http put -e -f -H $admin -t application/json $"($base)/users/($uid)/reset-password" {
    type: "password",
    value: $creds.user.secret,
    temporary: false
}
assert ($resp.status == 204)
$resp | to-http
```

<!-- output:http -->
```http
HTTP/1.1 204 No Content
strict-transport-security: max-age=31536000; includeSubDomains
x-content-type-options: nosniff
referrer-policy: no-referrer
x-robots-tag: none
x-frame-options: SAMEORIGIN
```
<!-- output:end -->

### Disable the VERIFY_PROFILE required action

KeyCloak 26 asks newly created users to "verify profile" on first login,
which would break the scripted authorization code flow.

```nu
let resp = http get -e -f -H $admin $"($base)/authentication/required-actions"
let verify = ($resp.body | where alias == "VERIFY_PROFILE" | first)
let updated = ($verify | update enabled false)
let resp = (
    http put -e -f -H $admin -t application/json
    $"($base)/authentication/required-actions/VERIFY_PROFILE"
    $updated
)
assert ($resp.status == 204)
$resp | to-http
```

<!-- output:http -->
```http
HTTP/1.1 204 No Content
x-robots-tag: none
strict-transport-security: max-age=31536000; includeSubDomains
x-content-type-options: nosniff
referrer-policy: no-referrer
x-frame-options: SAMEORIGIN
```
<!-- output:end -->

## Well-known metadata

Fetch the OpenID well-known metadata, check that all endpoints used later are
present and show the ones used in this document.

```nu
let resp = http get -e -f $"($KEYCLOAK)/realms/($REALM)/.well-known/openid-configuration"
let wk = $resp.body
for field in [token_endpoint authorization_endpoint introspection_endpoint revocation_endpoint jwks_uri] {
    assert ($field in ($wk | columns))
}
(
    $wk
    | select
        issuer
        token_endpoint
        authorization_endpoint
        introspection_endpoint
        revocation_endpoint
        jwks_uri
)
```

<!-- output: -->
╭────────────────────────┬──────────────────────────────────────────────────────────────────────────────╮
│ issuer                 │ http://localhost:8080/realms/spinta                                          │
│ token_endpoint         │ http://localhost:8080/realms/spinta/protocol/openid-connect/token            │
│ authorization_endpoint │ http://localhost:8080/realms/spinta/protocol/openid-connect/auth             │
│ introspection_endpoint │ http://localhost:8080/realms/spinta/protocol/openid-connect/token/introspect │
│ revocation_endpoint    │ http://localhost:8080/realms/spinta/protocol/openid-connect/revoke           │
│ jwks_uri               │ http://localhost:8080/realms/spinta/protocol/openid-connect/certs            │
╰────────────────────────┴──────────────────────────────────────────────────────────────────────────────╯
<!-- output:end -->

## Seed SQLite source database

Create a small SQLite database (two tables: `countries` and `cities`) that
serves as the external data source for the Spinta manifest.

### Countries table

```nu
mkdir $INSTANCE
rm -f $"($INSTANCE)/source.db"
let countries = [
    [id  name      ];
    [1   Lithuania ]
    [2   Latvia    ]
]
$countries | into sqlite $"($INSTANCE)/source.db" -t countries | ignore
let db = open $"($INSTANCE)/source.db"
assert (($db.countries | length) == 2)
$countries | to md -p
```

<!-- output: -->
| id  | name      |
| --- | --------- |
| 1   | Lithuania |
| 2   | Latvia    |
<!-- output:end -->

### Cities table

```nu
let cities = [
    [id  name     country_id ];
    [1   Vilnius  1          ]
    [2   Kaunas   1          ]
    [3   Riga     2          ]
]
$cities | into sqlite $"($INSTANCE)/source.db" -t cities | ignore
let db = open $"($INSTANCE)/source.db"
assert (($db.cities | length) == 3)
$cities | to md -p
```

<!-- output: -->
| id  | name    | country_id |
| --- | ------- | ---------- |
| 1   | Vilnius | 1          |
| 2   | Kaunas  | 1          |
| 3   | Riga    | 2          |
<!-- output:end -->

## Spinta keys, client and checks

Generate local token validation keys. Needed, because `BearerTokenValidator`
unconditionally loads a local public key (`spinta/auth.py`), even when tokens
are issued by an external authorization server.

Then download KeyCloak realm public keys into a local JWKS file. Keys are
read once at app init, so this must run **before** `spinta run`, and the
realm must already exist.

Finally register the `default_auth_client` client file, and run `spinta
show` / `spinta check` to validate the manifest.

### Generate local keys

FIXME: When external Authorization server is used, Spinta internal keys should not be generated.

Internal-AS behavior leaks into external-AS mode in several places

**1. The internal AS stays enabled.** `AuthorizationServer.enabled()` (spinta/auth.py:149) only checks whether a local private key exists — it knows nothing about the external AS. Worse, `spinta run` auto-generates the keypair if missing (spinta/cli/server.py:36 → `_ensure_config_dir`, spinta/cli/helpers/store.py:44-48), so merely running the server turns the internal AS on. `/auth/token` (spinta/api/__init__.py:85-104) then happily issues internal RS512 tokens signed by `keys/private.json`.

**2. Acceptance of internal tokens is accidental, not policy.** `BearerTokenValidator` takes keys from exactly one source (spinta/auth.py:195-211): `token_validation_key` config → config keys only; `token_validation_keys_download_url` → downloaded KeyCloak keys only; neither → local `keys/public.json` only. In the notes' config, internal tokens are rejected *only because* the download-URL branch excludes the local public key — remove that config option and internal tokens become accepted. Conversely, `spinta key generate` doesn't make internal tokens acceptable; it just satisfies the unconditional `load_key(..., required=True)` at spinta/auth.py:264 (the workaround documented in the notes) and, as a side effect, enables the internal token endpoint.

**3. Other internal-AS leaks in external mode:**
- `default_auth_client` fallback mints a local token for every unauthenticated request (spinta/auth.py:624-629) — that's the confusing `InvalidToken: unsupported_key_alg` 401 in test.md instead of a clean 401.
- `http_basic_auth` mints local tokens too (spinta/auth.py:631-635).
- `/.well-known/jwks.json` advertises the local public key even when only downloaded keys are trusted (spinta/api/__init__.py:107-118).
- No `iss` check anywhere — acceptance is tied to "keys that happen to be loaded", not to KeyCloak specifically.

This should be fixed in [atviriduomenys/spinta#2013](https://github.com/atviriduomenys/spinta/pull/2013).


```nu
spinta key generate
```

<!-- output:code -->
```
Private key saved to var/instances/auth/keycloak/keys/private.json.
Public key saved to var/instances/auth/keycloak/keys/public.json.
```
<!-- output:end -->

### Download KeyCloak realm public keys

`spinta key download` prints the whole downloaded JWKS, which is too noisy to
show — instead, the downloaded file is inspected in the next step. The file
path is configured with `downloaded_public_keys_file` in `config.yml`.

```nu
spinta key download
assert ($"($INSTANCE)/downloaded-well-knowns.json" | path exists)
```

```nu
open $"($INSTANCE)/downloaded-well-knowns.json" | to json
```

<!-- output:json -->
```json
{
  "keys": [
    {
      "kid": "lnv1rmUsI69ahMc7aWdgrLIQjByPgBLZMdpiEUpkrJk",
      "kty": "RSA",
      "alg": "RSA-OAEP",
      "use": "enc",
      "x5c": [
        "MIICmzCCAYMCBgGgrdXZgzANBgkqhkiG9w0BAQsFADARMQ8wDQYDVQQDDAZzcGludGEwHhcNMjYwOTE3MDUyNjA4WhcNMzYwOTE3MDUyNzQ4WjARMQ8wDQYDVQQDDAZzcGludGEwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDz0HaiUDlH/UV5Dp5JhIcbscwsNBDNxVxlDpeEoy4TnZPPwX8AoTae/E8HwjcPVRCI85vbgQsg3k+HH+lFi+DQVk9bEDk2cLv413KoP4gUKczHJF7HZL2f5DZnIlImrzNYFsmuHHR7MBF+NgnhG0BV8/3o0y72SOOWkYZpd9voAbs8/spXyRrnFHQIavv4yU9fcRSkdRxBDXCivVkhH1Vulv07TdyGgobuZWKJbU9KTPnVQxF4VCivwvn1auVIlGOBMnwBNxbl4h7IaERB3IcXwVkpXoysdbIHINCTufIy5pb1MJxUmshJFGFNXmS07Nyr+NYuJnp67fYy2s51hzWlAgMBAAEwDQYJKoZIhvcNAQELBQADggEBAGQ4VrHI3fl8SEgcmzhuzZlnlfVUJo19nVGQS1IWGtu4BnpB3An404mMT6MggdjiKQtYTsOcDSSoMCxMaGs9nCqRDfhQ1O1HjzuNosv4/G1PgLajA3Rp1HeuvTNRfWvYgYeA4cUb7/VBcS0nZjJU/SptsP3LVm/U6wpe4rrGyK+/lOD1IISduWoMsPlcjDUe1yjnWJLBv6V/5XUAOULkxvMHY47a5derP3LK7U9FlShzF6W2kWeybkBBqrY4oOQb0KHx9/Z9ghC2+jYfYQrtuOuTAqUG3Y/0cuRaKtpzySEzV8Ozx2OZHhSQ3IYVFu5zWwgT81GYpqZJ+iNUgDWHYpY="
      ],
      "x5t": "aYRkHiJKGiXzZwjbi-ATZlvyxoo",
      "x5t#S256": "K3F7InuSrHhq1q5rVeDCbQSZcN8I7YJyngo2UY4-3CU",
      "n": "89B2olA5R_1FeQ6eSYSHG7HMLDQQzcVcZQ6XhKMuE52Tz8F_AKE2nvxPB8I3D1UQiPOb24ELIN5Phx_pRYvg0FZPWxA5NnC7-NdyqD-IFCnMxyRex2S9n-Q2ZyJSJq8zWBbJrhx0ezARfjYJ4RtAVfP96NMu9kjjlpGGaXfb6AG7PP7KV8ka5xR0CGr7-MlPX3EUpHUcQQ1wor1ZIR9Vbpb9O03choKG7mViiW1PSkz51UMReFQor8L59WrlSJRjgTJ8ATcW5eIeyGhEQdyHF8FZKV6MrHWyByDQk7nyMuaW9TCcVJrISRRhTV5ktOzcq_jWLiZ6eu32MtrOdYc1pQ",
      "e": "AQAB"
    },
    {
      "kid": "HOZXt9TwUac4KZqsfD4DjbTiQx7BxownmJJjpKjiWHM",
      "kty": "RSA",
      "alg": "RS256",
      "use": "sig",
      "x5c": [
        "MIICmzCCAYMCBgGgrdXY6TANBgkqhkiG9w0BAQsFADARMQ8wDQYDVQQDDAZzcGludGEwHhcNMjYwOTE3MDUyNjA4WhcNMzYwOTE3MDUyNzQ4WjARMQ8wDQYDVQQDDAZzcGludGEwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDSguTD8vmDz+TLbaFl/QuZCJpjLZuVU2nL8tNX/h9JrmJlJf5zxOmvHNwBbg2KANAlEw7KLe/Wg2av6sQqGL+kKKfA3y2Iyyl2IVTZo7rdY0+zIfd0BVfhhCJOUNquoN1+oS9ltTnysR/PGzEFPasY9P3EdaOKW3TfF1jj6rW2JQr7o1dnqltU5/a4ZMhly9rY7C1pU0GKa3SVVIRrGcNxeNGewoJyYXkQwGhvCo0/m+ZINgLUqZK1kYr+5cugaSKkYJfi3i2NkuKtktDS+7JB8SmjHAMtTmodOZ7hwMuKxRJrT3TZWx+fFedaD9A6QjAcWp+4UzZVM9FK3I7eROsFAgMBAAEwDQYJKoZIhvcNAQELBQADggEBALI0p1863yzOjug8UFFpvCPh0L1vxxsvByAMc8b1BtklJwd9oKghfob+bw+O7iwwyF/ReJdXirRCqZGAt4A7ANb2ROVm3UHkpwthm1MJBJ5jda8349oCnaobsg1aUYikJRohp1I1/ztxrYLc8gxCrls6VU2QNCK54zdke0OWYWu0W+hzF2uaaz0caJSLvvGznRDGBGMVThUs9ZdSEPbCKyVEyDsweT6PYOiOr1Xf7UsZBl+CDir/Ef/MgtMI27S7kD2TTdhPPoF5XBNTVFvxxVLCim0f3DGLeyksjBlAcS/x0s3rK4fM5SiBqbGdkqRdjDVBvqVAYBJ+7I4H9J5rtTg="
      ],
      "x5t": "XbGIdH7jRwQeHwQ0xCwDlsn6khU",
      "x5t#S256": "imHz40auZjzX8itGj5w8ptGw61qgvRI3p2GPlE8XEzY",
      "n": "0oLkw_L5g8_ky22hZf0LmQiaYy2blVNpy_LTV_4fSa5iZSX-c8TprxzcAW4NigDQJRMOyi3v1oNmr-rEKhi_pCinwN8tiMspdiFU2aO63WNPsyH3dAVX4YQiTlDarqDdfqEvZbU58rEfzxsxBT2rGPT9xHWjilt03xdY4-q1tiUK-6NXZ6pbVOf2uGTIZcva2OwtaVNBimt0lVSEaxnDcXjRnsKCcmF5EMBobwqNP5vmSDYC1KmStZGK_uXLoGkipGCX4t4tjZLirZLQ0vuyQfEpoxwDLU5qHTme4cDLisUSa0902VsfnxXnWg_QOkIwHFqfuFM2VTPRStyO3kTrBQ",
      "e": "AQAB"
    }
  ]
}
```
<!-- output:end -->

### Inspect the downloaded keys

```nu
open $"($INSTANCE)/downloaded-well-knowns.json"
| get keys
| select kid kty alg use
| to md -p
```

<!-- output: -->
| kid                                         | kty | alg      | use |
| ------------------------------------------- | --- | -------- | --- |
| lnv1rmUsI69ahMc7aWdgrLIQjByPgBLZMdpiEUpkrJk | RSA | RSA-OAEP | enc |
| HOZXt9TwUac4KZqsfD4DjbTiQx7BxownmJJjpKjiWHM | RSA | RS256    | sig |
<!-- output:end -->

### Register the Spinta default client

FIXME: `default` client should be registered only if spinta is configured as
`access: open`.

The `default` client is registered on the first run; on repeated runs it
already exists, which is asserted here.

```nu
let add = $creds.client.scopes | spinta client add -n default --add-secret --scope - | complete
assert ("ClientWithNameAlreadyExists" in $add.stderr)
```

### Validate the manifest

```nu
spinta config manifests
```

<!-- output:code -->
```
Origin                          Name                       Value                           
------------------------------  -------------------------  --------------------------------
notes/auth/keycloak/config.yml  manifests.default.type     ascii                           
notes/auth/keycloak/config.yml  manifests.default.path     notes/auth/keycloak/manifest.txt
notes/auth/keycloak/config.yml  manifests.default.mode     external                        
notes/auth/keycloak/config.yml  manifests.default.keymap   default                         
notes/auth/keycloak/config.yml  manifests.default.backend  default                         
```
<!-- output:end -->


```nu
spinta show
```

<!-- output:code -->
```
id | d | r | b | m | property | type    | ref     | source                                          | source.type | prepare | origin | count | level | status | visibility | access    | uri | eli | title | description
   | auth/keycloak            |         |         |                                                 |             |         |        |       |       |        |            |           |     |     |       |
   |   | data                 | sql     |         | sqlite:///var/instances/auth/keycloak/source.db |             |         |        |       |       |        |            |           |     |     |       |
   |                          |         |         |                                                 |             |         |        |       |       |        |            |           |     |     |       |
   |   |   |   | Country      |         | id      | countries                                       |             |         |        |       | 4     |        |            |           |     |     |       |
   |   |   |   |   | id       | integer |         | id                                              |             |         |        |       | 4     |        |            | protected |     |     |       |
   |   |   |   |   | name     | string  |         | name                                            |             |         |        |       | 4     |        |            | protected |     |     |       |
   |                          |         |         |                                                 |             |         |        |       |       |        |            |           |     |     |       |
   |   |   |   | City         |         | id      | cities                                          |             |         |        |       | 4     |        |            |           |     |     |       |
   |   |   |   |   | id       | integer |         | id                                              |             |         |        |       | 4     |        |            | protected |     |     |       |
   |   |   |   |   | name     | string  |         | name                                            |             |         |        |       | 4     |        |            | protected |     |     |       |
   |   |   |   |   | country  | ref     | Country | country_id                                      |             |         |        |       | 3     |        |            | protected |     |     |       |
```
<!-- output:end -->

### Validate the manifest with `spinta check`

```nu
spinta check
```

<!-- output:code -->
```
Loading AsciiManifest manifest default (notes/auth/keycloak/manifest.txt)...
OK
```
<!-- output:end -->

## Start Spinta

### Run the Spinta server

Run Spinta in the background (output goes to
`var/instances/auth/keycloak/spinta.log`) and poll `/version` until it is
ready.

```nu
let log = $"($INSTANCE)/spinta.log"
rm -f $log
let job = job spawn {
    spinta run o+e> $log
}
poll-url $"($SPINTA)/version" 120
```

### Check the server version

```nu
http get -e -f $"($SPINTA)/version" | to-http
```

<!-- output:http -->
```http
HTTP/1.1 200 OK
date: Thu, 17 Sep 2026 05:28:04 GMT
strict-transport-security: max-age=31536000; includeSubDomains
server: uvicorn
content-length: 107
content-type: application/json

{
  "implementation": {
    "name": "Spinta",
    "version": "1.2.0"
  },
  "uapi": {
    "version": "0.1.0"
  },
  "dsa": {
    "version": "0.1.0"
  }
}
```
<!-- output:end -->

## Unauthenticated request is rejected

FIXME: `default` client and internal-AS leak issue

1. **The `default_auth_client` fallback mints a token for you.** In `get_auth_token` (spinta/auth.py:624-629): since `config.default_auth_client` is set (`default` in notes/auth/keycloak/config.yml:15) and there's no `Authorization` header, Spinta looks up the default client id in the keymap and **mints a fresh internal token** via `create_client_access_token` → `create_access_token` (spinta/auth.py:745-748), signing it with the local `keys/private.json` using **`alg: RS512`** — then injects it as `Authorization: Bearer ...`. So your "unauthenticated" request actually carries a locally forged token.

2. **That forged RS512 token is validated against Keycloak's RS256 keys.** `BearerTokenValidator.decode_token` (spinta/auth.py:267-291) loops over `_all_public_keys`, which in your setup contains only the keys downloaded from `token_validation_keys_download_url` (the `load_all_public_keys` branch at spinta/auth.py:208-209 — local `keys/public.json` is *not* included in that branch):
   - kid match fails (internal token has no `kid`, Keycloak keys do),
   - `decode_kty_from_alg("RS512")` → `"RSA"`, so `is_same_algorithm_type` matches the Keycloak RS256 key,
   - `jwt.decode` is called, and joserfc refuses because the key object carries `"alg": "RS256"` → `unsupported_key_alg: This key is designed for algorithm 'RS256'`.

3. That `JoseError` is caught at spinta/auth.py:289-290 and re-raised as `InvalidToken(error=...)` → 401 with `www-authenticate: Bearer error="invalid_token"`.

So the `InvalidToken` / `unsupported_key_alg` is not about the missing header at all — it's the locally minted RS512 token failing validation against the Keycloak RS256 keys. The clean 401 you'd expect is masked by this mint-then-fail dance.

A request without a Bearer token must not be served.

```nu
let resp = http get -e -f $"($SPINTA)/auth/keycloak/Country"
assert ($resp.status in [401 403])
$resp | to-http
```

<!-- output:http -->
```http
HTTP/1.1 401 Unauthorized
cache-control: no-store
www-authenticate: Bearer error="invalid_token"
strict-transport-security: max-age=31536000; includeSubDomains
server: uvicorn
date: Thu, 17 Sep 2026 05:28:04 GMT
content-length: 191
content-type: application/json

{
  "errors": [
    {
      "type": "system",
      "code": "InvalidToken",
      "template": "Invalid token",
      "context": {
        "error": "unsupported_key_alg: This key is designed for algorithm 'RS256'"
      },
      "message": "Invalid token"
    }
  ]
}
```
<!-- output:end -->

## Client authorization

Exchange the client id/secret for an access token (KeyCloak is the
authorization server, Spinta is the resource server). The raw response
carries the JWT itself — it is too long to be useful in output, so only the
remaining response fields and the decoded token claims are shown.

KeyCloak calls this flow as Service account roles (`serviceAccountsEnabled =
true`).

OAuth calls this `client_credentials` grant.

### Request the token

The client must request the `uapi:/:getall` scope explicitly, because it is
assigned as an optional client scope and is not granted automatically. It also
must pass the `resource` parameter identifying Spinta, the resource server.

```nu
let resp = http form -e -f $wk.token_endpoint {
    grant_type: "client_credentials",
    client_id: $creds.client.client,
    client_secret: $creds.client.secret,
    resource: $RESOURCE,
    scope: $creds.client.scopes
}
assert ($resp.status == 200)
let token = $resp.body.access_token
$resp.body | reject access_token | to json
```

<!-- output:json -->
```json
{
  "expires_in": 300,
  "refresh_expires_in": 0,
  "token_type": "Bearer",
  "not-before-policy": 0,
  "scope": "uapi:/:getall profile email"
}
```
<!-- output:end -->

FIXME: Probably we don't need extra scopes `profile` and `email`, maybe these
should be removed?

### Access token claims

Check that the token carries the `uapi:/:getall` scope claim and is issued for
the Spinta resource server — the audience mapper adds the `spinta` resource
server client to the `aud` claim.

```nu
let claims = jwt-claims $token
assert ($claims.scope | str contains "uapi:/:getall")
assert ($claims.aud == $creds.spinta.client)
assert ($claims.azp == $creds.client.client)
assert ($claims.client_id == $creds.client.client)
assert ($claims.preferred_username == $"service-account-($creds.client.client)")
$claims | to json
```

<!-- output:json -->
```json
{
  "exp": 1789623185,
  "iat": 1789622885,
  "jti": "trrtcc:b3670055-77c1-ee46-92d0-cde678d240fb",
  "iss": "http://localhost:8080/realms/spinta",
  "aud": "spinta",
  "sub": "dbbadad7-26f8-4e95-a680-7c692584e063",
  "typ": "Bearer",
  "azp": "client",
  "acr": "1",
  "allowed-origins": [
    "http://127.0.0.1:8081"
  ],
  "scope": "uapi:/:getall profile email",
  "email_verified": false,
  "clientHost": "172.18.0.1",
  "preferred_username": "service-account-client",
  "clientAddress": "172.18.0.1",
  "client_id": "client"
}
```
<!-- output:end -->

### Service account users

`sub` comes from the Keycloak **service account user** that Keycloak creates
automatically for the `client` client (because it's created with
`serviceAccountsEnabled: true` — test.md:237, 978).

When the client-credentials grant is used, Keycloak acts as if that hidden user
logged in:

- The service account user is named `service-account-<client_id>` →
  `service-account-client` (which is exactly the `preferred_username` in the
  claims).
- `sub` is the Keycloak **user ID** of that service account user
  (`d08fa85a-...`), not the client ID.
- That's also why the same `sub`/`preferred_username` pair reappears at
  test.md:1488-1499 (the token introspection output shows `username:
  service-account-client`).

Such users are hidden from the default Users list in the KeyCloak admin console
— they are linked to the client and shown in the `Service accounts roles`
client tab. Via the Admin API the dedicated user is available at the
`clients/{id}/service-account-user` endpoint:

```nu
let resp = http get -e -H $admin $"($base)/clients/($cid)/service-account-user"
assert ($resp.id == $claims.sub)
$resp | to json
```

<!-- output:json -->
```json
{
  "id": "dbbadad7-26f8-4e95-a680-7c692584e063",
  "username": "service-account-client",
  "emailVerified": false,
  "enabled": true,
  "createdTimestamp": 1789622868496,
  "totp": false,
  "disableableCredentialTypes": [],
  "requiredActions": [],
  "notBefore": 0
}
```
<!-- output:end -->

## User authorization

KeyCloak calls this flow as Standard flow (`standardFlowEnabled =
true`).

OAuth calls this `authorization_code` grant.

Simulate a browser login:

1. GET the authorization endpoint,
2. parse the KeyCloak login form,
3. POST user credentials (with cookies collected from response headers —
   KeyCloak 26.x sets `Secure` on its cookies even over plain HTTP, so they are
   sent manually), and
4. read the authorization `code` from the redirect `Location` header.

### Get authorization code

```nu
let code = (
    get-auth-code
        $wk.authorization_endpoint
        $REDIRECT_URI
        $creds.client.client
        $creds.user.user
        $creds.user.secret
        $"openid ($creds.client.scopes)"
        $RESOURCE
)
assert (($code | str length) > 10)
$code
```

<!-- output:code -->
```
c9725f09-f3ea-bd6d-4b96-68b05ec20f8d.-Tm0g4Bn4ko4UQXQchK4iw2D.3cb14b72-15f6-49b7-ad3a-283e0cc1bdc1
```
<!-- output:end -->

### Exchange the code for tokens

The token response contains the access, refresh and ID tokens — only the
remaining fields are shown, the tokens themselves are decoded in the
[Token claims](#token-claims) section below.

```nu
let resp = http form -e -f -m 30sec $wk.token_endpoint {
    grant_type: "authorization_code",
    code: $code,
    redirect_uri: $REDIRECT_URI,
    client_id: $creds.client.client,
    client_secret: $creds.client.secret,
    resource: $RESOURCE,
}
assert ($resp.status == 200)
assert ("refresh_token" in $resp.body)
let tokens = $resp.body
$resp.body | reject access_token refresh_token id_token | to json
```

<!-- output:json -->
```json
{
  "expires_in": 300,
  "refresh_expires_in": 1800,
  "token_type": "Bearer",
  "not-before-policy": 0,
  "session_state": "-Tm0g4Bn4ko4UQXQchK4iw2D",
  "scope": "openid uapi:/:getall profile email"
}
```
<!-- output:end -->

### Refresh token grant

Refresh the tokens and check that a new (different) access token with the
`uapi:/:getall` scope is issued.

```nu
let resp = http form -e -f -m 30sec $wk.token_endpoint {
    grant_type: "refresh_token",
    refresh_token: $tokens.refresh_token,
    client_id: $creds.client.client,
    client_secret: $creds.client.secret,
    resource: $RESOURCE,
}
assert ($resp.status == 200)
assert ($resp.body.access_token != $tokens.access_token)
assert ($resp.body.scope | str contains "uapi:/:getall")
let tokens = $resp.body
$resp.body | reject id_token access_token refresh_token | to json
```

<!-- output:json -->
```json
{
  "expires_in": 300,
  "refresh_expires_in": 1800,
  "token_type": "Bearer",
  "not-before-policy": 0,
  "session_state": "-Tm0g4Bn4ko4UQXQchK4iw2D",
  "scope": "openid uapi:/:getall profile email"
}
```
<!-- output:end -->

## Token claims

Decode and display the ID, access and refresh token claims. Signatures are
not verified — tokens are decoded to inspect the claims only.

### ID token claims

```nu
let claims = decode-jwt $tokens.id_token | get payload
assert ($claims.aud == $creds.client.client)
assert ($claims.sub == $uid)
assert ($claims.azp == $creds.client.client)
assert ($claims.preferred_username == $creds.user.user)
$claims | to json
```

<!-- output:json -->
```json
{
  "exp": 1789623186,
  "iat": 1789622886,
  "auth_time": 1789622885,
  "jti": "cc2ba8c3-a515-0070-1c3e-e48a6cd4ca9b",
  "iss": "http://localhost:8080/realms/spinta",
  "aud": "client",
  "sub": "6d28dd71-037f-48da-a2a8-d9e419867431",
  "typ": "ID",
  "azp": "client",
  "sid": "-Tm0g4Bn4ko4UQXQchK4iw2D",
  "at_hash": "JhSgzskSWpVPiBq01qHRVg",
  "acr": "1",
  "email_verified": true,
  "preferred_username": "user"
}
```
<!-- output:end -->

### Access token claims

```nu
let claims = decode-jwt $tokens.access_token | get payload
assert ($claims.aud == "spinta")
assert ($claims.sub == $uid)
assert ($claims.azp == $creds.client.client)
assert ($claims.preferred_username == $creds.user.user)
$claims | to json
```

<!-- output:json -->
```json
{
  "exp": 1789623186,
  "iat": 1789622886,
  "auth_time": 1789622885,
  "jti": "onrtrt:d24ffb1b-7652-0c37-7dad-4b43218b4492",
  "iss": "http://localhost:8080/realms/spinta",
  "aud": "spinta",
  "sub": "6d28dd71-037f-48da-a2a8-d9e419867431",
  "typ": "Bearer",
  "azp": "client",
  "sid": "-Tm0g4Bn4ko4UQXQchK4iw2D",
  "acr": "1",
  "allowed-origins": [
    "http://127.0.0.1:8081"
  ],
  "scope": "openid uapi:/:getall profile email",
  "email_verified": true,
  "preferred_username": "user"
}
```
<!-- output:end -->

### Refresh token claims

```nu
let claims = decode-jwt $tokens.refresh_token | get payload
assert ($claims.azp == $creds.client.client)
$claims | to json
```

<!-- output:json -->
```json
{
  "exp": 1789624686,
  "iat": 1789622886,
  "jti": "2726d1d0-39e6-e5d0-1e76-743689fe2c57",
  "iss": "http://localhost:8080/realms/spinta",
  "aud": "http://localhost:8080/realms/spinta",
  "sub": "6d28dd71-037f-48da-a2a8-d9e419867431",
  "typ": "Refresh",
  "azp": "client",
  "sid": "-Tm0g4Bn4ko4UQXQchK4iw2D",
  "scope": "openid acr uapi:/:getall web-origins service_account profile email basic",
  "aud_x": "spinta",
  "prov": "default"
}
```
<!-- output:end -->

## Query data with the user token

The authorization code flow (user) token must also grant access to Spinta
data.

```nu
let resp = http get -e -f -H {Authorization: $"Bearer ($tokens.access_token)"} $"($SPINTA)/auth/keycloak/Country"
# assert ($resp.body | str contains "Lithuania")
$resp | to-http
```

<!-- output:http -->
```http
HTTP/1.1 200 OK
strict-transport-security: max-age=31536000; includeSubDomains
date: Thu, 17 Sep 2026 05:28:05 GMT
content-type: application/json
transfer-encoding: chunked
server: uvicorn

{
  "_data": [
    {
      "_type": "auth/keycloak/Country",
      "_id": "e2f740bb-878e-4b19-95e8-1836b1bb19d5",
      "_revision": null,
      "id": 1,
      "name": "Lithuania"
    },
    {
      "_type": "auth/keycloak/Country",
      "_id": "9aeb2c5c-aa2e-4978-9a7b-02b87ebed6c0",
      "_revision": null,
      "id": 2,
      "name": "Latvia"
    }
  ],
  "_page": {
    "next": "WzJd"
  }
}
```
<!-- output:end -->

## Data queries

Query the `Country` and `City` models with the Bearer token obtained from
the client credentials grant.

```nu
let client = {Authorization: $"Bearer ($token)"}
```

### Query countries

```nu
let country = http get -e -f -H $client $"($SPINTA)/auth/keycloak/Country"
assert (($country.body._data | length) == 2)
$country | to-http
```

<!-- output:http -->
```http
HTTP/1.1 200 OK
strict-transport-security: max-age=31536000; includeSubDomains
transfer-encoding: chunked
content-type: application/json
server: uvicorn
date: Thu, 17 Sep 2026 05:28:06 GMT

{
  "_data": [
    {
      "_type": "auth/keycloak/Country",
      "_id": "e2f740bb-878e-4b19-95e8-1836b1bb19d5",
      "_revision": null,
      "id": 1,
      "name": "Lithuania"
    },
    {
      "_type": "auth/keycloak/Country",
      "_id": "9aeb2c5c-aa2e-4978-9a7b-02b87ebed6c0",
      "_revision": null,
      "id": 2,
      "name": "Latvia"
    }
  ],
  "_page": {
    "next": "WzJd"
  }
}
```
<!-- output:end -->

### Query cities

```nu
let city = http get -e -f -H $client $"($SPINTA)/auth/keycloak/City"
assert (($city.body._data | length) == 3)
$city | to-http
```

<!-- output:http -->
```http
HTTP/1.1 200 OK
server: uvicorn
content-type: application/json
strict-transport-security: max-age=31536000; includeSubDomains
transfer-encoding: chunked
date: Thu, 17 Sep 2026 05:28:06 GMT

{
  "_data": [
    {
      "_type": "auth/keycloak/City",
      "_id": "a6ffa014-a6b7-4365-ad76-8e87552a2aba",
      "_revision": null,
      "id": 1,
      "name": "Vilnius",
      "country": {
        "id": 1
      }
    },
    {
      "_type": "auth/keycloak/City",
      "_id": "aa00d261-47f9-4b6e-a78c-f14bb4291f5c",
      "_revision": null,
      "id": 2,
      "name": "Kaunas",
      "country": {
        "id": 1
      }
    },
    {
      "_type": "auth/keycloak/City",
      "_id": "d95231f2-e3ac-40d6-9be3-c71c7c0aa36b",
      "_revision": null,
      "id": 3,
      "name": "Riga",
      "country": {
        "id": 2
      }
    }
  ],
  "_page": {
    "next": "WzNd"
  }
}
```
<!-- output:end -->

## Token introspection

Ask KeyCloak if the token is active and check the introspected scope.

```nu
let resp = http form -e -f $wk.introspection_endpoint {
    token: $token,
    client_id: $creds.spinta.client,
    client_secret: $creds.spinta.secret
}
assert ($resp.body.active == true)
assert ("uapi:/:getall" in $resp.body.scope)
$resp | to-http
```

<!-- output:http -->
```http
HTTP/1.1 200 OK
x-content-type-options: nosniff
x-robots-tag: none
referrer-policy: no-referrer
cache-control: no-cache
strict-transport-security: max-age=31536000; includeSubDomains
content-type: application/json
content-length: 475
x-frame-options: SAMEORIGIN

{
  "exp": 1789623185,
  "iat": 1789622885,
  "jti": "trrtcc:b3670055-77c1-ee46-92d0-cde678d240fb",
  "iss": "http://localhost:8080/realms/spinta",
  "aud": "spinta",
  "sub": "dbbadad7-26f8-4e95-a680-7c692584e063",
  "typ": "Bearer",
  "azp": "client",
  "acr": "1",
  "allowed-origins": [
    "http://127.0.0.1:8081"
  ],
  "scope": "uapi:/:getall profile email",
  "email_verified": false,
  "preferred_username": "service-account-client",
  "client_id": "client",
  "username": "service-account-client",
  "token_type": "Bearer",
  "active": true
}
```
<!-- output:end -->

## Token revocation

Revoke the token and verify via introspection that it is no longer active.

Then observe how Spinta treats a revoked (but not yet expired) token. Spinta
is a resource server that validates JWTs locally (`Token.is_revoked()`
always returns `False`) and does not call KeyCloak introspection, so
revocation is not detected — the actual behavior is recorded below.

### Revoke the token

```nu
let resp = http form -e -f $wk.revocation_endpoint {
    token: $token,
    client_id: $creds.client.client,
    client_secret: $creds.client.secret
}
assert ($resp.status in [200 204])
$resp | to-http
```

<!-- output:http -->
```http
HTTP/1.1 200 OK
referrer-policy: no-referrer
strict-transport-security: max-age=31536000; includeSubDomains
x-frame-options: SAMEORIGIN
x-robots-tag: none
x-content-type-options: nosniff
content-security-policy: frame-src 'self'; frame-ancestors 'self'; object-src 'none';
content-length: 0
```
<!-- output:end -->

### Verify revocation via introspection

```nu
let resp = http form -e -f $wk.introspection_endpoint {
    token: $token,
    client_id: $creds.spinta.client,
    client_secret: $creds.spinta.secret
}
assert ($resp.body.active == false)
$resp | to-http
```

<!-- output:http -->
```http
HTTP/1.1 200 OK
content-length: 16
referrer-policy: no-referrer
x-frame-options: SAMEORIGIN
cache-control: no-cache
content-type: application/json
x-content-type-options: nosniff
strict-transport-security: max-age=31536000; includeSubDomains
x-robots-tag: none

{
  "active": false
}
```
<!-- output:end -->

### Spinta still serves the revoked token

FIXME: Spinta does not check KeyCloak revocation, tokens stay valid until they expire.

```nu
let resp = http get -e -f -H {Authorization: $"Bearer ($token)"} $"($SPINTA)/auth/keycloak/Country"
$resp | to-http
```

<!-- output:http -->
```http
HTTP/1.1 200 OK
server: uvicorn
strict-transport-security: max-age=31536000; includeSubDomains
content-type: application/json
transfer-encoding: chunked
date: Thu, 17 Sep 2026 05:28:06 GMT

{
  "_data": [
    {
      "_type": "auth/keycloak/Country",
      "_id": "e2f740bb-878e-4b19-95e8-1836b1bb19d5",
      "_revision": null,
      "id": 1,
      "name": "Lithuania"
    },
    {
      "_type": "auth/keycloak/Country",
      "_id": "9aeb2c5c-aa2e-4978-9a7b-02b87ebed6c0",
      "_revision": null,
      "id": 2,
      "name": "Latvia"
    }
  ],
  "_page": {
    "next": "WzJd"
  }
}
```
<!-- output:end -->

## Teardown

### Stop Spinta

```nu
try { job kill $job } catch { }
```
