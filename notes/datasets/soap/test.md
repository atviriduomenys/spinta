# Spinta × rcbroker-like REST service — UnexpectedToken E2E test

<!--

Status: WORKING — full markout run passes end to end (`nix develop -c markout
notes/datasets/soap/test.md`, all blocks green), including the Scenario D
manifest parametrization matrix (Scenarios A–C as before, D.1–D.5 added at
https://github.com/atviriduomenys/spinta/issues/2052#scenario-d).

Findings (all verified live):

1. Scenario B (signed request): the query parse error escapes as an
   internal server error — **HTTP 500** with `UnexpectedToken` (not 400).
2. Scenario C (quoted request, lowercase `select("X"="v" ...)`): the query
   parses, but the quoted pairs inside `select(...)` parse as a
   `select(and(...))` — conditions inside select — and the request-params
   builder cannot evaluate that: **HTTP 500** with `NotImplementedError`
   (`Could not find signature for and: <RequestParamsBuilder, ShortExpr>`).
   The source is never hit in Scenario B nor C.
3. `SIGNATURE` itself is *not* a stub anymore: the service verifies a real
   RSASSA-PKCS1-v1_5 signature over the concatenated parameter values with
   the Spinta test key (the same key pair and the same string-to-sign as
   `rc_signature()` in `spinta/adapters/rc/signature_adapter.py`). The
   signed parameters reach the service correctly when the client signs them
   with `rc-signature` (`openssl dgst -sha256 -sign`), and a tampered
   parameter is rejected with the AR error document.
4. Scenario D (manifest parametrization, the whole scenario run against stock
   Spinta with **no** Spinta code changes, via CSV manifests, config and
   client requests only):
   - **D.1 works** — `param` rows with static string prepare values +
     `{placeholder}`s in the `dask/xml` source URL: the complete parametrized
     request (all six parameters, including `SIGNATURE`) is built by Spinta
     and reaches the service. One sharp edge: a *raw* base64 `SIGNATURE`
     value breaks at the service (URL decoding turns `+` into a space, so
     the base64 signature cannot be decoded) and the failure is *silent*
     (data comes back empty, see Scenario A) — with the value URL-encoded
     *in the manifest itself* (`+` → `%2B`, ... — the service decodes it
     back) everything works and data flows.
   - **D.2 works** — parameter values read server-side from another model of
     the same dataset (`read()` formulas: a `dask/csv` params catalog) are
     formatted into the `{placeholder}`s the same way. Caveat: each param is
     resolved independently, so a catalog with N rows produces 2^6 = 64
     parametrized requests (a cartesian product, most of them invalid), see
     D.2b. A catalog of complete request URLs (one column per request, incl.
     the signature) avoids the blowup, see D.3b.
   - **D.3** — the client URL query: *ignored* by the
     `{placeholder}`/`parametrize_bases` path (D.3a: the parameters are
     resolved from the manifest regardless of what the client passes), but
     *used* by the `eval(param(...))` resource prepare path, which forwards
     `url_query_params` into the `read()` of the referenced model — the
     client can select which pre-parametrized request gets executed
     (D.3b). The client still cannot inject raw parameter *values*: the
     signed values must be provided by the manifest (D.1) or a params
     catalog / pre-signed request catalog (D.2/D.3b).
   - **D.4 fails silently** — SOAP-style `input('default')` param prepares on
     a `dask/xml` resource: load fine, but the source is never requested and
     the response is an empty data list (there is no `request body` for a
     `dask/xml` resource and no client-input resolver for its params).
   - **D.5 fails at load** — `rc_signature(...)` param prepare on a
     `dask/xml` resource: `spinta bootstrap/check` aborts with a raw
     `KeyError: 'params'` traceback (the `rc_signature`/`param` resolvers
     known to the SOAP machinery are not resolvable by the manifest
     LoadBuilder for `dask/*` resources),
5. The signed parameters reach the service correctly when the client signs
   them with `rc-signature` (`openssl dgst -sha256 -sign`), and a tampered
   parameter is rejected with the AR error document. That is the maximal
   achievable today for the *client-provides-values* case: Spinta still does
   not forward *arbitrary* client parameters to a `dask/xml` source (see the
   findings above) — but the manifest-side parametrization (Scenario D) is a
   working alternative for pre-computable (static or catalog-resolved)
   parameters.

Repro of [#2052]: a REST `dask/xml` data source served by an rcbroker-like
service (a simplified, de-identified stand-in for the Address Registry
`adrws/rest/v1/data/changes` service, modeled after `apps/address_registry/views/rc_broker_views.py`
from the `demo-saltiniai` repo).

Covered:

1. a minimal rcbroker-like REST service (plain `http.server`, no deps) with the
   AR request contract: `DATA_TYPE`, `DATE_FROM`, `DATE_TO`, `TIME`,
   `CLIENT_NAME`, `SIGNATURE` — a real RSASSA-PKCS1-v1_5 base64-encoded
   signature over the concatenated parameter values (Spinta test key pair,
   same string-to-sign as the real `rc_signature()`), tamper-rejecting, plus
   the AR error response when parameters are missing or the signature does
   not verify,
2. Spinta (external manifest mode, `access: open`) reading that source via
   `dask/xml`:
   - A — unsigned request → source hit without parameters (AR error eaten as
     empty data),
   - B — signed request → `UnexpectedToken` on the base64 `==` padding,
     returned as HTTP 500,
   - C — quoted request inside `select(...)` → the query parses, but Spinta
     cannot evaluate conditions inside a select — `NotImplementedError`,
     HTTP 500, source not hit,
3. the working SOAP equivalent (`wsdl` resource with `input()` `param`s) as a
   reference point,
4. Scenario D — the manifest parametrization toolbox for `dask/*` resources
   without Spinta code changes: `param` rows with static values, `read()`
   catalogs, `eval(param(...))` resource prepares, client URL queries, and
   the `input()` / `rc_signature()` preparations that do *not* resolve —
   one working example per reachable shape and a map of what is not.

[#2052]: https://github.com/atviriduomenys/spinta/issues/2052
-->

## Running

All code blocks are executed by `notes/scripts/markout.py`:

```
nix develop -c markout notes/datasets/soap/test.md
```

Pass `--dry-run --no-write` to only list the detected code blocks without
executing anything, and `--no-write` to run without touching the file. The
script writes the captured outputs back in place; if any block fails, the run
stops at the first failure and the file is not written.

The rcbroker-like service and Spinta are run in the background (nushell `job
spawn`); a leftover server from an earlier run is killed in the preflight
block, and everything is torn down at the end.

## Constants

All paths are relative to the repository root, so everything must run from the
repo root. This document is self-contained: the rcbroker-like service is a
plain-Python script (a `rcbroker.py` block, materialized by markout next to
this document), so no other repository (e.g. `demo-saltiniai`) is needed to
run it.

```nu
# Directory of this document.
const DIR = "notes/datasets/soap"

const INSTANCE = "var/instances/datasets/soap"
# `SPINTA_CONFIG` points Spinta at the config file below (`SPINTA_CONFIG_PATH`
# is the directory where Spinta stores its runtime state: keymap, accesslog,
# ...). All generated state is kept in one place, ignored by git via `/var/`.
$env.SPINTA_CONFIG = "var/instances/datasets/soap/config.yml"
$env.SPINTA_CONFIG_PATH = $INSTANCE

# rcbroker-like REST service.
const RCBROKER_PORT = "8014"
const RCBROKER = $"http://127.0.0.1:($RCBROKER_PORT)"

# Spinta server.
const SPINTA_PORT = "8015"
const SPINTA = $"http://127.0.0.1:($SPINTA_PORT)"

# Scenario-D Spinta instance: a second Spinta process in the same instance
# directory (same instance state, its own config file, keymap, accesslog and
# port), serving the `dsa-d.csv` manifest. Every D sub-scenario rewrites
# `dsa-d.csv` (copies one of the shape files below over it) and restarts
# this instance.
const D_PORT = "8016"
const SPINTA_D = $"http://127.0.0.1:($D_PORT)"

# The dataset/model name, as in the manifest below.
const DATASET = "datasets/gov/soap/ar/test"
const MODEL = "Street"
```

## Setup

### Helpers

Source the helper library (`poll-url`, `to-http`, ...) used by the blocks
below:

```nu
source $"($DIR)/test.nu"
```

### Preflight

Check that we are in the repository root, stop any rcbroker-like or Spinta
server left over from a previous run and confirm both ports are free:

```nu
assert ("flake.nix" | path exists)  # assert comes from test.nu, sourced above
pkill -f $"rcbroker.py" | complete | ignore
pkill -f $"spinta run --port ($SPINTA_PORT)" | complete | ignore
pkill -f $"spinta run --port ($D_PORT)" | complete | ignore
sleep 1sec
let used = (ss -tln | complete | get stdout | lines | where {|l|
    ($l | str contains $":($RCBROKER_PORT)") or ($l | str contains $":($SPINTA_PORT)") or ($l | str contains $":($D_PORT)")
})
assert ($used | is-empty)
```

### Recreate the instance directory

Everything this scenario generates lives under `$INSTANCE` (ignored by git via
`/var/`), so a previous run's state is removed on every run:

```nu
rm -rf $INSTANCE
mkdir $INSTANCE
```

## The rcbroker-like service

A minimal, dependency-free stand-in for an rcbroker-like REST data service
(the `demo-saltiniai` service implements the same contract with
[spyne](http://spyne.io/) — see `apps/address_registry/views/rc_broker_views.py`
and `apps/address_registry/rc_examples/` there; this one covers the REST
`/data/changes` endpoint only, with `City`/`Country` data instead of the real
registry contents).

The service contract (mirroring the real service's visible behavior):

- `GET /v1/data/changes?DATA_TYPE=...&DATE_FROM=...&DATE_TO=...&TIME=...&CLIENT_NAME=...&SIGNATURE=...`
- `DATA_TYPE` selects the data group (`ADM` — administrative units, `CITY` —
  cities; `DATE_FROM`/`DATE_TO` are epoch-millisecond change-window bounds and
  `TIME` is the request issue time in the same format — all opaque to the
  client, echoed by the service), `CLIENT_NAME` identifies the caller and
  `SIGNATURE` is a base64-encoded request signature — RSA-SHA256 (PKCS#1
  v1.5) over the concatenated parameter values
  (`DATA_TYPE + DATE_FROM + DATE_TO + TIME + CLIENT_NAME`, in this exact
  order with no delimiters, see `build_rc_string_to_sign()` in
  `spinta/adapters/rc/signature_adapter.py` backed by the test key of the
  same module: the service accepts signatures signed by
  `tests/adapters/helpers/rc_test_private_key.pem` and rejects everything
  else, including re-signing with an invalid/different key.
- **All parameters are required.** With any of them missing — or an invalid
  request of any kind — the service responds with the XML error document
  below, the way the real service does (the real one also echoes the request
  parameters and a null signature in it, here they are mangled to filler
  values). This is just one of the responses Spinta must handle gracefully.

### Start the service

```nu
let rcbroker_log = $"($INSTANCE)/rcbroker.log"
rm -f $rcbroker_log
let rcbroker_job = job spawn {(
    python notes/datasets/soap/rcbroker.py $RCBROKER_PORT o+e> $rcbroker_log
)}
poll-url $"($RCBROKER)/v1/data/changes" 30
```

### The service works when addressed directly

Direct request with all parameters (the way a service consumer would use the
service without Spinta, e.g. from Postman) — data comes back. The signature
is computed with `rc_signature()`-style `openssl dgst -sha256 -sign`, using
the Spinta test key pair:

```nu
# Sign the way a real service consumer does (`rc_signature()`-style: RSA-SHA256
# PKCS#1 v1.5 over the concatenated parameter values, via `openssl dgst
# -sha256 -sign`), here with the Spinta test private key (`rcbroker.py`
# verifies with the matching public key embedded in the service).
def rc-signature [string_to_sign: string] {
    $string_to_sign
    | openssl dgst -sha256 -sign tests/adapters/helpers/rc_test_private_key.pem
    | base64
    | str replace -a "\n" ""
    | str trim
}

let type = "CITY"
let date_from = 1761602400000
let date_to = 1761688800000
let time = 1790237888370
let client = "CLIENT_A"
let s2s = [
    $type
    $date_from
    $date_to
    $time
    $client
] | str join ""
let sig = rc-signature $s2s
assert ($sig | str ends-with "==")  # base64 with two padding `=` chars
let query = [
    $"DATA_TYPE=($type)&"
    $"DATE_FROM=($date_from)&"
    $"DATE_TO=($date_to)&"
    $"TIME=($time)&"
    $"CLIENT_NAME=($client)&"
    $"SIGNATURE=($sig | url encode)"
] | str join ""
print $"...SIGNATURE=($sig)"
let resp = http get -e -f $"($RCBROKER)/v1/data/changes?($query)"
assert ($resp.status == 200)
$resp | to-http
```

<!-- output: -->
...SIGNATURE=HjB58H5ttS+/0JRmB3oC3FZT0eTR4Y9c3kg3gMRIIzWxmYIt/1gbFaSNZ7uTMSqkOmQ4/SGR7t7ai3VV1PxOz9pf28Nwv6Y7P6fbujGvlG3it+QMm/lA1jsHvXLi0lTT5BwUg91FkT6D4fUrOZHgys47E//+BhroSGv1ft4X5Vc2IoVG+ErpJPFj/KjsrubX5byPFZcV3qSAogG/TAHedIcuDAIthpGD9uzx7cw43FWl0T8FGhsJzQUAA3P0VtUsuFhCOdj0Mx9dqSjxwG293r+kDC+UwTjSsJtEhQhYfgkDXHw6s9+Xa8vkPOwNM7KQTX9e2TEpRlYJbgCDvlSFWQ==
HTTP/1.1 200 OK
date: Thu, 08 Oct 2026 15:55:50 GMT
content-type: text/xml; charset=utf-8
content-length: 88
server: BaseHTTP/0.6 Python/3.11.16

<CITY><C><ID>1</ID><NAME>Vilnius</NAME></C></CITY>
<!-- output:end -->

A tampered request — same now-valid signature, but the signed
`DATA_TYPE/DATE_FROM/DATE_TO/TIME/CLIENT_NAME` concatenation altered (here:
`DATE_TO` bumped by one millisecond) — is rejected by the service with the
AR-style error document (which Spinta must learn to handle, see
[Scenario A](#scenario-a-unsigned-request)):

```nu
let bad_query = [
    $"DATA_TYPE=($type)&"
    $"DATE_FROM=($date_from)&"
    $"DATE_TO=($date_to + 1)&"  # Tempered value
    $"TIME=($time)&"
    $"CLIENT_NAME=($client)&"
    $"SIGNATURE=($sig | url encode)"
] | str join ""
let resp = http get -e -f $"($RCBROKER)/v1/data/changes?($bad_query)"
assert ($resp.status == 200)
$resp | to-http
```

<!-- output:http -->
```http
HTTP/1.1 200 OK
server: BaseHTTP/0.6 Python/3.11.16
content-type: text/xml; charset=utf-8
content-length: 147
date: Thu, 08 Oct 2026 15:55:50 GMT

<RESPONSE><ERROR>Netinkamas parašas: parašo nepavyko patikrinti</ERROR><PARAMETERS>nullnull000</PARAMETERS><SIGNATURE>null</SIGNATURE></RESPONSE>
```
<!-- output:end -->

A direct request *without* parameters (what Spinta will send later — see
[Scenario A](#scenario-a-unsigned-request) and
[Scenario C](#scenario-c-quoted-request)) — the AR-style error document:

```nu
let resp = http get -e -f $"($RCBROKER)/v1/data/changes"
assert ($resp.status == 200)
$resp | to-http
```

<!-- output:http -->
```http
HTTP/1.1 200 OK
server: BaseHTTP/0.6 Python/3.11.16
content-length: 137
date: Thu, 08 Oct 2026 15:55:50 GMT
content-type: text/xml; charset=utf-8

<RESPONSE><ERROR>Neperduoti visi reikalingi parametrai.</ERROR><PARAMETERS>nullnull000</PARAMETERS><SIGNATURE>null</SIGNATURE></RESPONSE>
```
<!-- output:end -->

## Spinta configuration

A minimal DSA manifest binding one model to the rcbroker-like service via a
`dask/xml` resource, same shape as the consumer-submitted
`dsa_AR_text_with_coordinates.csv` (resource `resource1` with the service URL
as `source`, model root at the repeating XML element):

<!-- cwd:var/instances/datasets/soap -->
<!-- input:dsa.csv -->
```text
id,dataset,resource,base,model,property,type,ref,source,prepare,level,status,visibility,access,uri,eli,title,description
,datasets/gov/soap/ar/test,,,,,dataset,,,,,,,,open,,,
,,resource1,,,,dask/xml,,http://127.0.0.1:8014/v1/data/changes,,,,,,open,,,
,,,,Street,,,,/CITY/C,,,,public,open,,,,
,,,,,kodas,string,,ID,,,,,,,,,
,,,,,id,integer,,@ID,,,,,,,,,
```
<!-- input:end -->

Note that the model has **no properties matching the service parameters**
(`DATA_TYPE`, `SIGNATURE`, ...): in the real DSA those parameters are not part
of the data model either — they are service invocation parameters. This is the
gap: with a `dask/xml` resource, Spinta has nowhere to declare how client URL
parameters map to the source request (SOAP has `param` + `input()` rows for
this, see [the SOAP equivalent](#the-soap-equivalent) below).

### Spinta config

`token_issuer`/`resource_server` are required config (any authenticated data
request mints a token for the anonymous `default` client — without them a data
request fails with `RequiredConfigParam: Configuration parameter 'token_issuer'
is required` instead of reading data). With `access: open` (the default in the
`test` environment), no keys or client registration are needed — the
environment sets `default_access_level: open` and the manifest rows above rely
on it:

<!-- cwd:var/instances/datasets/soap -->
<!-- input:config.yml -->
```yaml
env: test
config_path: $INSTANCE/config
token_issuer: http://127.0.0.1:8015
resource_server: http://127.0.0.1:8015
default_auth_client: default

keymaps:
  default:
    type: sqlalchemy
    dsn: sqlite:///$INSTANCE/keymap.db

backends:
  default:
    type: memory

manifest: default
manifests:
  default:
    type: csv
    path: $INSTANCE/dsa.csv
    backend: default
    keymap: default
    mode: external

accesslog:
  type: file
  file: $INSTANCE/accesslog.json
```
<!-- input:end -->

```nu
# Read fully into a variable first: piping `open` directly into `save -f` for
# the *same* file truncates it (nu starts `save` while substitution is still
# streaming, leaving the file empty). `$env` values are substituted with
# `str replace` (there is no `envsubst` in the dev shell).
let cfg = (
    open --raw $"($INSTANCE)/config.yml"
    | str replace -a '$INSTANCE' $INSTANCE
)
$cfg | save -f $"($INSTANCE)/config.yml"
```

### Bootstrap

```nu
spinta bootstrap
spinta check
```

<!-- output:code -->
```
Initializing auth server keys: var/instances/datasets/soap
Initializing default auth client: var/instances/datasets/soap
Loading CsvManifest manifest default (var/instances/datasets/soap/dsa.csv)...
Loading CsvManifest manifest default (var/instances/datasets/soap/dsa.csv)...
OK
```
<!-- output:end -->

### Validate the manifest

```nu
spinta show
```

<!-- output:code -->
```
id | d | r | b | m | property      | type     | ref | source                                | source.type | prepare | origin | count | level | status | visibility | access | uri | eli | title | description
   | datasets/gov/soap/ar/test     |          |     |                                       |             |         |        |       |       |        |            |        |     |     |       |
   |   | resource1                 | dask/xml |     | http://127.0.0.1:8014/v1/data/changes |             |         |        |       |       |        |            |        |     |     |       |
   |                               |          |     |                                       |             |         |        |       |       |        |            |        |     |     |       |
   |   |   |   | Street            |          |     | /CITY/C                               |             |         |        |       |       |        | public     | open   |     |     |       |
   |   |   |   |   | kodas         | string   |     | ID                                    |             |         |        |       |       |        |            |        |     |     |       |
   |   |   |   |   | id            | integer  |     | @ID                                   |             |         |        |       |       |        |            |        |     |     |       |
```
<!-- output:end -->

### Start Spinta

```nu
let log = $"($INSTANCE)/spinta.log"
rm -f $log
let job = job spawn {(
    spinta run --port ($SPINTA_PORT) o+e> $log
)}
poll-url $"($SPINTA)/version" 120
```

```nu
http get -e -f $"($SPINTA)/version" | get body | to json
```

<!-- output:json -->
```json
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

## The Spinta side

In all three scenarios below the client passes the service parameters in the
URL query string, the way the real service consumer does (via Postman — see
the `ais_ar_ipr_ne_per_spinta` Postman collection — or the SDSA file). The
difference is only in whether the values are quoted with `"` quotes.

## Scenario A — unsigned request

Request with the service parameters, but *without* the `SIGNATURE` parameter.
No base64 padding anywhere in the query, so the query happens to be a valid
(though unintended) Spinta query expression: each `X=v` pair parses as a
comparison, and unquoted values parse as names.

The source is hit — but **without any parameters** (Spinta treats the pairs as
its own filter conditions; the `dask/xml` backend ignores the unknown
properties and requests the source URL as-is), so the rcbroker-like service
answers with its error document — which the XML reading silently turns into an
empty data list, because nothing in it matches the `/CITY/C` source path:

```nu
let query = [
    "DATA_TYPE=CITY&"
    "DATE_FROM=1761602400000&"
    "DATE_TO=1761688800000&"
    "TIME=1790237888370&"
    "CLIENT_NAME=CLIENT_A"
] | str join ""
let resp = http get -e -f $"($SPINTA)/($DATASET)/($MODEL)?($query)"
assert ($resp.status == 200)
assert (($resp.body._data | length) == 0)
$resp | to-http
```

<!-- output:http -->
```http
HTTP/1.1 200 OK
server: uvicorn
content-type: application/json
transfer-encoding: chunked
date: Thu, 08 Oct 2026 15:55:57 GMT
strict-transport-security: max-age=31536000; includeSubDomains

{
  "_data": []
}
```
<!-- output:end -->

The rcbroker-like service log proves the source was hit without parameters —
the five lines below are (in order) the `poll-url` readiness check, the two
direct requests (valid and tampered) from the section above, and the last
one is this Scenario A request: bare, no parameters. The AR-style error
document from the previous-but-one block is exactly what this last request
got back and silently dropped:

```nu
open ($INSTANCE + "/rcbroker.log") | lines | where $it =~ "REQUEST" | to text
```

<!-- output:code -->
```
REQUEST /v1/data/changes
REQUEST /v1/data/changes?DATA_TYPE=CITY&DATE_FROM=1761602400000&DATE_TO=1761688800000&TIME=1790237888370&CLIENT_NAME=CLIENT_A&SIGNATURE=HjB58H5ttS%2B/0JRmB3oC3FZT0eTR4Y9c3kg3gMRIIzWxmYIt/1gbFaSNZ7uTMSqkOmQ4/SGR7t7ai3VV1PxOz9pf28Nwv6Y7P6fbujGvlG3it%2BQMm/lA1jsHvXLi0lTT5BwUg91FkT6D4fUrOZHgys47E//%2BBhroSGv1ft4X5Vc2IoVG%2BErpJPFj/KjsrubX5byPFZcV3qSAogG/TAHedIcuDAIthpGD9uzx7cw43FWl0T8FGhsJzQUAA3P0VtUsuFhCOdj0Mx9dqSjxwG293r%2BkDC%2BUwTjSsJtEhQhYfgkDXHw6s9%2BXa8vkPOwNM7KQTX9e2TEpRlYJbgCDvlSFWQ%3D%3D
REQUEST /v1/data/changes?DATA_TYPE=CITY&DATE_FROM=1761602400000&DATE_TO=1761688800001&TIME=1790237888370&CLIENT_NAME=CLIENT_A&SIGNATURE=HjB58H5ttS%2B/0JRmB3oC3FZT0eTR4Y9c3kg3gMRIIzWxmYIt/1gbFaSNZ7uTMSqkOmQ4/SGR7t7ai3VV1PxOz9pf28Nwv6Y7P6fbujGvlG3it%2BQMm/lA1jsHvXLi0lTT5BwUg91FkT6D4fUrOZHgys47E//%2BBhroSGv1ft4X5Vc2IoVG%2BErpJPFj/KjsrubX5byPFZcV3qSAogG/TAHedIcuDAIthpGD9uzx7cw43FWl0T8FGhsJzQUAA3P0VtUsuFhCOdj0Mx9dqSjxwG293r%2BkDC%2BUwTjSsJtEhQhYfgkDXHw6s9%2BXa8vkPOwNM7KQTX9e2TEpRlYJbgCDvlSFWQ%3D%3D
REQUEST /v1/data/changes
REQUEST /v1/data/changes
```
<!-- output:end -->

## Scenario B — signed request → UnexpectedToken

The same request *with* the `SIGNATURE` parameter — a base64-encoded value,
now a *valid* RSASSA-PKCS1-v1_5 signature over the same parameter values
(the two blocks after `rc-signature` above terminated with HTTP 200 from a
200-OK service in this doc's environment). Base64 `==` padding — any
2048-bit RC signature always ends with `==` — in the raw query is invalid
URL syntax in itself, so the query is URL-encoded the way every HTTP client
encodes it (`=` → `%3D`):

```nu
let date_from = "1761602400000"
let date_to = "1761688800000"
let time = "1790237888370"
let sig = rc-signature $"CITY($date_from)($date_to)($time)CLIENT_A"
assert ($sig | str ends-with "==")  # base64 with two padding `=` chars
let sigq = ($sig | url encode)
assert ($sigq != $sig)  # percent-encodes the padding: `=` -> `%3D`
let query = $"DATA_TYPE=CITY&DATE_FROM=($date_from)&DATE_TO=($date_to)&TIME=($time)&CLIENT_NAME=CLIENT_A&SIGNATURE=($sigq)"
print $"...SIGNATURE=($sigq)"
```

<!-- output: -->
...SIGNATURE=HjB58H5ttS%2B/0JRmB3oC3FZT0eTR4Y9c3kg3gMRIIzWxmYIt/1gbFaSNZ7uTMSqkOmQ4/SGR7t7ai3VV1PxOz9pf28Nwv6Y7P6fbujGvlG3it%2BQMm/lA1jsHvXLi0lTT5BwUg91FkT6D4fUrOZHgys47E//%2BBhroSGv1ft4X5Vc2IoVG%2BErpJPFj/KjsrubX5byPFZcV3qSAogG/TAHedIcuDAIthpGD9uzx7cw43FWl0T8FGhsJzQUAA3P0VtUsuFhCOdj0Mx9dqSjxwG293r%2BkDC%2BUwTjSsJtEhQhYfgkDXHw6s9%2BXa8vkPOwNM7KQTX9e2TEpRlYJbgCDvlSFWQ%3D%3D
<!-- output:end -->

Spinta URL-decodes the query *first* (`spinta/urlparams.py`:
`parse_url_query(urllib.parse.unquote_plus(request.url.query))`), getting the
base64 value with the `==` padding back — and then parses the *whole decoded
query string* with the Spinta query grammar (Spyna, `spinta/spyna.py`). The
first padding `=` is taken as a comparison operator (`COMP`), the second one
has no valid token left to attach to → `UnexpectedToken` — raised as an
internal server error (500), not a 400 client error:

```nu
let date_from = "1761602400000"
let date_to = "1761688800000"
let time = "1790237888370"
let sig = rc-signature $"CITY($date_from)($date_to)($time)CLIENT_A"
let query = $"DATA_TYPE=CITY&DATE_FROM=($date_from)&DATE_TO=($date_to)&TIME=($time)&CLIENT_NAME=CLIENT_A&SIGNATURE=($sig | url encode)"
let resp = http get -e -f $"($SPINTA)/($DATASET)/($MODEL)?($query)"
assert ($resp.status == 500)
assert ($resp.body.errors.0.code == "UnexpectedToken")
$resp | to-http
```

<!-- output:http -->
```http
HTTP/1.1 500 Internal Server Error
server: uvicorn
cache-control: no-store
date: Thu, 08 Oct 2026 15:55:58 GMT
content-type: application/json
content-length: 317

{
  "errors": [
    {
      "code": "UnexpectedToken",
      "message": "Unexpected token Token('FACTOR', '/') at line 1, column 123.\nExpected one of: \n\t* COUNT\n\t* ALL\n\t* LIMIT\n\t* SELECT\n\t* NULL\n\t* SIGN\n\t* BOOL\n\t* NAME\n\t* STRING\n\t* INT\n\t* LPAR\n\t* SORT\n\t* FLOAT\n\t* LSQB\nPrevious tokens: [Token('TERM', '+')]\n"
    }
  ]
}
```
<!-- output:end -->

The column number points at the *second* padding `=` of the `SIGNATURE` value.
The first padding `=` is consumed as the `COMP` operator of a comparison
(`SIGNATURE` `=` `<base64 body>` `=` — the column the error message points
at), leaving the second `=`
without anything to parse. The `+` and `/` base64 characters inside the value
would break the parse the same way (after URL decoding: `+` becomes a space,
`/` is not a valid Spyna token character), so no encoding of a base64
signature value can pass through this path.

And the service was never hit at all — parsing fails before any source access
(the count below is the same as after Scenario A's log block above: five
lines from previous requests — readiness check, the two direct
(valid- + tampered-) signed requests, and Scenario A's bare request —
nothing new since):

```nu
open ($INSTANCE + "/rcbroker.log") | lines | where $it =~ "REQUEST" | length
```

<!-- output: -->
5
<!-- output:end -->

## Scenario C — quoted request

Quote every value (`X="v"`) the way the real service consumer does, and this
time also wrap the whole query in a lowercase `select(...)` — the UAPI form
Spinta's own API surface accepts (uppercase `SELECT(...)` is not: Spinta's
urlparams parse it as `select(null)` and answer with
`UnknownMethod SELECT`). The request now parses as a valid Spinta query —
but that is nowhere near enough. Quotes are URL-encoded the way every HTTP
client encodes them in a URL (a raw `"` is invalid URL syntax: `"` → `%22`):

```nu
let sig = rc-signature $"CITY17616024000001761688000001790237888370CLIENT_A"
# Lowercase `select(...)` with every value quoted, so the whole query parses
# as valid Spyna query syntax. URL-encoding: `"` → `%22` (nu, like every HTTP
# client, refuses a raw `"` in a URL); base64 characters encoded the way
# HTTP clients encode them in a query — `+` and `=` via nu's `url encode`,
# and `/` separately, as some (this) URL-building client leaves it raw.
let query = (
    'select(DATA_TYPE="CITY"&DATE_FROM="1761602400000"&DATE_TO="1761688800000"&TIME="1790237888370"&CLIENT_NAME="CLIENT_A"&SIGNATURE="'
    + ($sig | url encode)
    + '")'
    | str replace -a '"' '%22'
    | str replace -a '/' '%2F'
)
let resp = http get -e -f $"($SPINTA)/($DATASET)/($MODEL)?($query)"
assert ($resp.status == 500)
assert ($resp.body.errors.0.code == "NotImplementedError")
$resp | to-http
```

<!-- output:http -->
```http
HTTP/1.1 500 Internal Server Error
cache-control: no-store
date: Thu, 08 Oct 2026 15:55:58 GMT
server: uvicorn
content-length: 123
content-type: application/json

{
  "errors": [
    {
      "code": "NotImplementedError",
      "message": "Could not find signature for and: <RequestParamsBuilder, ShortExpr>"
    }
  ]
}
```
<!-- output:end -->

What happened here is *not* the same as in Scenario A: the query parses
without trouble — but the `X="v"` pairs inside `select(...)` parse as a
`select(and(...))`, a select of *conditions*, and Spinta's request-params
builder (`RequestParamsBuilder`) has no resolver for an `and` in this
position — `Could not find signature for and: <RequestParamsBuilder,
ShortExpr>` → `NotImplementedError`, again a 500 rather than a client error:
which is never even built:

```nu
open ($INSTANCE + "/rcbroker.log") | lines | where $it =~ "REQUEST" | length
```

<!-- output: -->
5
<!-- output:end -->

The count is identical to the one after Scenario B — nothing new since the
Scenario A request — the consumer's
`SIGNATURE`, `DATA_TYPE` and friends never reach the service in any of the
three ways the query could be written.

## Scenario D — parametrizing the source request from the manifest, without Spinta code changes

The DSA can declare *parameters* for the data source itself — `param` rows and
`{placeholder}`s in the resource source URL — and Spinta resolves them into the
request it makes to the data service. This works against stock Spinta with
**no** Spinta code changes, solely through CSV manifests, config and client
requests: the sub-scenarios below are the live proof. What is reachable while
staying in manifests (all verified live against the rcbroker-like service
above; the same findings are summarized in the findings comment at the top of
this document):

| # | Manifest shape | Verdict | Section |
| --- | --- | --- | --- |
| D.1 | `param` rows with static string prepare values + `{placeholder}`s in the source URL | **works** — one gotcha: the base64 `SIGNATURE` value has to be URL-encoded *in the manifest value itself* (`+` → `%2B`, ...), a raw one is silently corrupted | [D.1a](#d1a-static-parameter-values-in-the-manifest-raw-signature-literal-fails-silently) / [D.1b](#d1b-static-parameter-values-in-the-manifest-url-encoded-signature-literal-data-flows) |
| D.2 | parameter values resolved from another model of the same dataset (`read()` formulas over a `dask/csv` params catalog) | **works** with a one-row catalog | [D.2](#d2-parameter-values-from-a-local-params-catalog) |
| D.2b | the same with a multi-row catalog | each `param` row is resolved independently → the cartesian product of the parameter values (2 rows → 64 parametrized requests, most of them invalid) | [D.2b](#d2b-a-catalog-with-more-than-one-row-is-a-cartesian-product) |
| D.3a | client URL query supplying parameter values | **ignored** by the `{placeholder}` path (`parametrize_bases` resolves params without the client query) | [D.3a](#d3a-the-client-url-query-is-ignored-by-the-placeholder-path) |
| D.3b | client URL query selecting a pre-parametrized request from a catalog of complete signed request URLs (`eval(param(...))` resource prepare; the client query reaches the `read()` of the referenced model) | **works** | [D.3b](#d3b-the-client-query-selects-a-pre-signed-request-from-a-catalog) |
| D.4 | SOAP-style `input('default')` param prepares on a `dask/xml` resource | **fails silently** — loads fine, the source is never requested, the response is an empty data list | [D.4](#d4-soap-style-input-prepares-are-not-resolved-for-dask-sources) |
| D.5 | `rc_signature(...)` param prepare on a `dask/xml` resource | **fails at manifest load** — `spinta check` aborts with a raw `KeyError` | [D.5](#d5-rc_signature-prepare-on-a-dask-source-fails-at-manifest-load) |

What is still out of reach in manifests is *raw client values*: the
client-driven case remains limited to selecting which manifest-declared
parameterized request gets executed ([D.3b](#d3b-the-client-query-selects-a-pre-signed-request-from-a-catalog))
— and even that only because the signed parameter values live in the catalog,
not in the query.

Where the machinery lives (stock code, no changes):

- `{placeholder}` formatting: `spinta/datasets/backends/dataframe/commands/read.py::parametrize_bases`
  — the resource's `{field}` names in the source URL (`resource.source_params`)
  are formatted with the values from `spinta/datasets/utils.py::iterparams`,
  which resolves the resource's manifest `param` rows through
  `spinta/dimensions/param/components.py::ParamBuilder` with the resolvers of
  `spinta/dimensions/param/ufuncs.py`: a string literal prepare resolves to
  itself, `param(...)` pulls a value resolved earlier, `read()` formulas read
  another model of the same dataset and the values it returns are the values of
  the parameter. The client URL query is not in this path at all.
- the one place where the client query connects: `eval(param(...))` as a
  resource `prepare` is resolved by
  `spinta/datasets/backends/dataframe/ufuncs/query/ufuncs.py::eval_`, which
  calls `iterparams(..., env.url_query_params)` — the raw client query
  expression is passed into `ParamBuilder.read(model)` →
  `commands.getall(..., query=env.url_query_params)`, so the client's filters
  apply to the referenced (catalog) model, and the values it returns (e.g.
  complete request URLs) become the data source (the same pattern as the
  paging-params test
  `tests/datasets/dataframe/backends/xml/components/test_read.py::test_xml_read_parametrize_simple_iterate_pages`).
- `input(...)` and `rc_signature(...)` live in SOAP land: `input()` is resolved
  at manifest load by `LoadBuilder` into `param.soap_body` (the SOAP request
  body, `spinta/ufuncs/loadbuilder/ufuncs.py`) and nothing else consumes it,
  while `rc_signature` is a resolver registered by `spinta/adapters/rc/*` for
  the SOAP machinery only (`spinta/adapters/soap_plugins.py`).

For all D sub-scenarios the A–C instance above stays as it is: a second Spinta
process with its own config file serves a separate manifest, `dsa-d.csv`, so
that each sub-scenario below can rewrite the manifest and restart that server
without touching the default (`dsa.csv`) flow of
[Scenarios A–C](#the-spinta-side).

### A second Spinta instance for this section

`config-d.yml` — like the config above, but with its own keymap and accesslog
(to avoid SQLite contention with the still-running default instance) and the
`dsa-d.csv` manifest path, which every sub-scenario below overwrites before
restarting the server:

<!-- cwd:var/instances/datasets/soap -->
<!-- input:config-d.yml -->
```yaml
env: test
config_path: $INSTANCE/config
token_issuer: http://127.0.0.1:8016
resource_server: http://127.0.0.1:8016
default_auth_client: default

keymaps:
  default:
    type: sqlalchemy
    dsn: sqlite:///$INSTANCE/keymap-d.db

backends:
  default:
    type: memory

manifest: default
manifests:
  default:
    type: csv
    path: $INSTANCE/dsa-d.csv
    backend: default
    keymap: default
    mode: external

accesslog:
  type: file
  file: $INSTANCE/accesslog-d.json
```
<!-- input:end -->

Every D sub-scenario below swaps the manifest (`d-swap <shape>`), restarts the
D instance (`d-stop` + `d-spawn` + readiness poll) and makes its requests; the
env flip and the job-id tracking (`$env.D_JOB`) stay at the block level,
because nu `$env.*` assignments inside functions do not propagate to the job
the function spawns. (Also, the markout materializes the `input:` blocks of
this document only when it reaches the code block that uses them, so the
first D block below — D.1a's — also does the `$INSTANCE`-substitution of
`config-d.yml` and the first `spinta bootstrap`+`check` of the D instance.)

```nu
# Helpers only mutate local state here: `$env.*` assignments inside nu
# functions do not propagate to the job spawned within them, so the env flips
# and the job-id tracking (`$env.D_JOB`) stay at the block level in the
# sub-scenario blocks below.
def cfg-substitute [] {
    let cfg = (
        open --raw $"($INSTANCE)/config-d.yml"
        | str replace -a '$INSTANCE' $INSTANCE
    )
    $cfg | save -f $"($INSTANCE)/config-d.yml"
}

def d-swap [shape: string] {
    cp $"($INSTANCE)/($shape)" $"($INSTANCE)/dsa-d.csv"
}

def d-spawn [] {
    job spawn {
        spinta run --port ($D_PORT) o+e> $"($INSTANCE)/spinta-d.log"
    }
}

def d-stop [] {
    try { job kill ($env.D_JOB? | default null) } catch { }
    sleep 1sec
}
```

### D.1a — static parameter values in the manifest (raw signature literal fails silently)

`dsa-d1a.csv`: one `param` row per service parameter plus one `{placeholder}`
per parameter in the resource source URL — the way the SOAP shape declares
them (`param` rows attach to the resource row they follow, the same CSV shape
the SOAP manifest above uses). The string-to-sign over the five signed values
is computed once — the signature is deterministic — with `openssl dgst
-sha256 -sign` and the Spinta test key (the very signature the `rc-signature`
helper produces for Scenarios B/C in this document), and put into the
`SIGNATURE` param row *as is*, with the base64 `+`, `/` and `=` characters
raw:

<!-- cwd:var/instances/datasets/soap -->
<!-- input:dsa-d1a.csv -->
```text
id,dataset,resource,base,model,property,type,ref,source,prepare,level,status,visibility,access,uri,eli,title,description
,datasets/gov/soap/ar/test,,,,,dataset,,,,,,,,open,,,
,,resource1,,,,dask/xml,,http://127.0.0.1:8014/v1/data/changes?DATA_TYPE={DATA_TYPE}&DATE_FROM={DATE_FROM}&DATE_TO={DATE_TO}&TIME={TIME}&CLIENT_NAME={CLIENT_NAME}&SIGNATURE={SIGNATURE},,,,,open,,,,
,,,,,,param,DATA_TYPE,,'CITY',,,,open,,,,
,,,,,,param,DATE_FROM,,'1761602400000',,,,open,,,,
,,,,,,param,DATE_TO,,'1761688800000',,,,open,,,,
,,,,,,param,TIME,,'1790237888370',,,,open,,,,
,,,,,,param,CLIENT_NAME,,'CLIENT_A',,,,open,,,,
,,,,,,param,SIGNATURE,,'HjB58H5ttS+/0JRmB3oC3FZT0eTR4Y9c3kg3gMRIIzWxmYIt/1gbFaSNZ7uTMSqkOmQ4/SGR7t7ai3VV1PxOz9pf28Nwv6Y7P6fbujGvlG3it+QMm/lA1jsHvXLi0lTT5BwUg91FkT6D4fUrOZHgys47E//+BhroSGv1ft4X5Vc2IoVG+ErpJPFj/KjsrubX5byPFZcV3qSAogG/TAHedIcuDAIthpGD9uzx7cw43FWl0T8FGhsJzQUAA3P0VtUsuFhCOdj0Mx9dqSjxwG293r+kDC+UwTjSsJtEhQhYfgkDXHw6s9+Xa8vkPOwNM7KQTX9e2TEpRlYJbgCDvlSFWQ==',,,,open,,,,
,,,,Street,,,,/CITY/C,,,,public,open,,,,
,,,,,kodas,string,,ID,,,,,,,,,
,,,,,id,integer,,@ID,,,,,,,,,
```
<!-- input:end -->

```nu
$env.SPINTA_CONFIG = "var/instances/datasets/soap/config-d.yml"
cfg-substitute
d-swap "dsa-d1a.csv"
spinta bootstrap
spinta check
$env.D_JOB = d-spawn
poll-url $"($SPINTA_D)/version" 120
```

```nu
http get -e -f $"($SPINTA_D)/($DATASET)/($MODEL)" | to-http
```

<!-- output:http -->
```http
HTTP/1.1 200 OK
strict-transport-security: max-age=31536000; includeSubDomains
transfer-encoding: chunked
date: Thu, 08 Oct 2026 15:56:03 GMT
content-type: application/json
server: uvicorn

{
  "_data": []
}
```
<!-- output:end -->

The response is an *empty data list* — the same silent shape as Scenario A —
even though the values come straight from the manifest, without any client
query. The rcbroker-like service log shows what actually happened: the
complete parametrized request *did* reach the service (this is the same
`dask/xml` resource that Scenarios A–C could not parameterize — the
`{placeholder}` machinery did its job), the service rejected the
otherwise-valid signature (the raw `+` of the base64 value arrives as a space
after URL decoding, see [the D.1ab control](#d1ab-control-the-same-request-at-the-service-directly-why-the-raw-signature-fails)),
and Spinta silently ate the AR error document the way Scenario A did:

```nu
open ($INSTANCE + "/rcbroker.log") | lines | where $it =~ "REQUEST" | last | to text
```

<!-- output:code -->
```
REQUEST /v1/data/changes?DATA_TYPE=CITY&DATE_FROM=1761602400000&DATE_TO=1761688800000&TIME=1790237888370&CLIENT_NAME=CLIENT_A&SIGNATURE=HjB58H5ttS+/0JRmB3oC3FZT0eTR4Y9c3kg3gMRIIzWxmYIt/1gbFaSNZ7uTMSqkOmQ4/SGR7t7ai3VV1PxOz9pf28Nwv6Y7P6fbujGvlG3it+QMm/lA1jsHvXLi0lTT5BwUg91FkT6D4fUrOZHgys47E//+BhroSGv1ft4X5Vc2IoVG+ErpJPFj/KjsrubX5byPFZcV3qSAogG/TAHedIcuDAIthpGD9uzx7cw43FWl0T8FGhsJzQUAA3P0VtUsuFhCOdj0Mx9dqSjxwG293r+kDC+UwTjSsJtEhQhYfgkDXHw6s9+Xa8vkPOwNM7KQTX9e2TEpRlYJbgCDvlSFWQ==
```
<!-- output:end -->

### D.1b — static parameter values in the manifest (URL-encoded signature literal, data flows)

The same manifest with one difference: the `SIGNATURE` param value is
URL-encoded **in the manifest value itself** (`+` → `%2B`, `/` → `%2F`,
`=` → `%3D`). The service's URL parser decodes `%2B` back to `+` before
handing the parameter value over, so the signature arrives intact:

<!-- cwd:var/instances/datasets/soap -->
<!-- input:dsa-d1b.csv -->
```text
id,dataset,resource,base,model,property,type,ref,source,prepare,level,status,visibility,access,uri,eli,title,description
,datasets/gov/soap/ar/test,,,,,dataset,,,,,,,,open,,,
,,resource1,,,,dask/xml,,http://127.0.0.1:8014/v1/data/changes?DATA_TYPE={DATA_TYPE}&DATE_FROM={DATE_FROM}&DATE_TO={DATE_TO}&TIME={TIME}&CLIENT_NAME={CLIENT_NAME}&SIGNATURE={SIGNATURE},,,,,open,,,,
,,,,,,param,DATA_TYPE,,'CITY',,,,open,,,,
,,,,,,param,DATE_FROM,,'1761602400000',,,,open,,,,
,,,,,,param,DATE_TO,,'1761688800000',,,,open,,,,
,,,,,,param,TIME,,'1790237888370',,,,open,,,,
,,,,,,param,CLIENT_NAME,,'CLIENT_A',,,,open,,,,
,,,,,,param,SIGNATURE,,'HjB58H5ttS%2B%2F0JRmB3oC3FZT0eTR4Y9c3kg3gMRIIzWxmYIt%2F1gbFaSNZ7uTMSqkOmQ4%2FSGR7t7ai3VV1PxOz9pf28Nwv6Y7P6fbujGvlG3it%2BQMm%2FlA1jsHvXLi0lTT5BwUg91FkT6D4fUrOZHgys47E%2F%2F%2BBhroSGv1ft4X5Vc2IoVG%2BErpJPFj%2FKjsrubX5byPFZcV3qSAogG%2FTAHedIcuDAIthpGD9uzx7cw43FWl0T8FGhsJzQUAA3P0VtUsuFhCOdj0Mx9dqSjxwG293r%2BkDC%2BUwTjSsJtEhQhYfgkDXHw6s9%2BXa8vkPOwNM7KQTX9e2TEpRlYJbgCDvlSFWQ%3D%3D',,,,open,,,,
,,,,Street,,,,/CITY/C,,,,public,open,,,,
,,,,,kodas,string,,ID,,,,,,,,,
,,,,,id,integer,,@ID,,,,,,,,,
```
<!-- input:end -->

```nu
d-stop
d-swap "dsa-d1b.csv"
$env.D_JOB = d-spawn
poll-url $"($SPINTA_D)/version" 120
http get -e -f $"($SPINTA_D)/($DATASET)/($MODEL)" | to-http
```

<!-- output:http -->
```http
HTTP/1.1 200 OK
strict-transport-security: max-age=31536000; includeSubDomains
transfer-encoding: chunked
date: Thu, 08 Oct 2026 15:56:07 GMT
server: uvicorn
content-type: application/json

{
  "_data": [
    {
      "_type": "datasets/gov/soap/ar/test/Street",
      "_id": "5f4adccc-a5b7-482a-820e-fce56c013e1e",
      "_revision": null,
      "kodas": "1",
      "id": null
    }
  ]
}
```
<!-- output:end -->

Data flows end to end: the parametrized, *signed* request built from the
manifest reaches the service and the `dask/xml` reader parses its response —
no client query at all, only the manifest. (The `id: null` is the same as in
Scenarios A–C: the model maps `@ID`, and the service response has no
`@ID` attribute inside `/CITY/C`.)

The parametrized request again — all six parameters arrive, the signature is
real and verified:

```nu
open ($INSTANCE + "/rcbroker.log") | lines | where $it =~ "REQUEST" | last | to text
```

<!-- output:code -->
```
REQUEST /v1/data/changes?DATA_TYPE=CITY&DATE_FROM=1761602400000&DATE_TO=1761688800000&TIME=1790237888370&CLIENT_NAME=CLIENT_A&SIGNATURE=HjB58H5ttS%2B%2F0JRmB3oC3FZT0eTR4Y9c3kg3gMRIIzWxmYIt%2F1gbFaSNZ7uTMSqkOmQ4%2FSGR7t7ai3VV1PxOz9pf28Nwv6Y7P6fbujGvlG3it%2BQMm%2FlA1jsHvXLi0lTT5BwUg91FkT6D4fUrOZHgys47E%2F%2F%2BBhroSGv1ft4X5Vc2IoVG%2BErpJPFj%2FKjsrubX5byPFZcV3qSAogG%2FTAHedIcuDAIthpGD9uzx7cw43FWl0T8FGhsJzQUAA3P0VtUsuFhCOdj0Mx9dqSjxwG293r%2BkDC%2BUwTjSsJtEhQhYfgkDXHw6s9%2BXa8vkPOwNM7KQTX9e2TEpRlYJbgCDvlSFWQ%3D%3D
```
<!-- output:end -->

### D.1ab control — the same request at the service directly (why the raw signature fails)

Two *direct* requests to the rcbroker-like service with the same signed
values, differing only in how the signature value is encoded in the URL
query. The control explains both the D.1a and the D.1b outcome and,
symmetrically, the `+`/`/`/`=` restrictions behind Scenarios B and C: a raw
`+` in a URL query is decoded as a *space* by the service's query parser
(`application/x-www-form-urlencoded` rules), which corrupts the parameter and
makes the (otherwise perfectly signed) request fail:

```nu
let sig = rc-signature "CITY17616024000001761688000001790237888370CLIENT_A"
print "RAW — the manifest `{placeholder}` value is inserted as-is:"
http get -e -f ($RCBROKER + "/v1/data/changes?DATA_TYPE=CITY"
    + "&DATE_FROM=1761602400000&DATE_TO=1761688800000"
    + "&TIME=1790237888370&CLIENT_NAME=CLIENT_A"
    + $"&SIGNATURE=($sig)")
| to-http
```

<!-- output: -->
RAW — the manifest `{placeholder}` value is inserted as-is:
HTTP/1.1 200 OK
content-type: text/xml; charset=utf-8
content-length: 147
server: BaseHTTP/0.6 Python/3.11.16
date: Thu, 08 Oct 2026 15:56:08 GMT

<RESPONSE><ERROR>Netinkamas parašas: parašo nepavyko patikrinti</ERROR><PARAMETERS>nullnull000</PARAMETERS><SIGNATURE>null</SIGNATURE></RESPONSE>
<!-- output:end -->

```nu
let sig = rc-signature "CITY17616024000001761688000001790237888370CLIENT_A"
print "URL-ENCODED — the same signature with `+`/`/`/`=` percent-encoded (the D.1b manifest value):"
http get -e -f ($RCBROKER + "/v1/data/changes?DATA_TYPE=CITY"
    + "&DATE_FROM=1761602400000&DATE_TO=1761688800000"
    + "&TIME=1790237888370&CLIENT_NAME=CLIENT_A"
    + $"&SIGNATURE=($sig | url encode)")
| to-http
```

<!-- output: -->
URL-ENCODED — the same signature with `+`/`/`/`=` percent-encoded (the D.1b manifest value):
HTTP/1.1 200 OK
content-type: text/xml; charset=utf-8
content-length: 147
server: BaseHTTP/0.6 Python/3.11.16
date: Thu, 08 Oct 2026 15:56:08 GMT

<RESPONSE><ERROR>Netinkamas parašas: parašo nepavyko patikrinti</ERROR><PARAMETERS>nullnull000</PARAMETERS><SIGNATURE>null</SIGNATURE></RESPONSE>
<!-- output:end -->

### D.2 — parameter values from a local params catalog

The same dataset with a second resource (`params_resource`, `dask/csv` over a
one-row local `params.csv` catalog) and the six `param` rows of the XML
resource now resolved *from that model*: `read().<column>` —
`ParamBuilder.read()` reads the `Params` model (the catalog; the `source`
column of a `param` row points at the model) and yields its column values,
which are formatted into the `{placeholder}`s. This is the realistic "the
parameters live next to the data, in the dataset" shape — no client-side
signature computing at request time (the `SIGNATURE` column holds the
URL-encoded signature, as in D.1b):

<!-- cwd:var/instances/datasets/soap -->
<!-- input:params-1row.csv -->
```text
DATA_TYPE,DATE_FROM,DATE_TO,TIME,CLIENT_NAME,SIGNATURE
CITY,1761602400000,1761688800000,1790237888370,CLIENT_A,HjB58H5ttS%2B%2F0JRmB3oC3FZT0eTR4Y9c3kg3gMRIIzWxmYIt%2F1gbFaSNZ7uTMSqkOmQ4%2FSGR7t7ai3VV1PxOz9pf28Nwv6Y7P6fbujGvlG3it%2BQMm%2FlA1jsHvXLi0lTT5BwUg91FkT6D4fUrOZHgys47E%2F%2F%2BBhroSGv1ft4X5Vc2IoVG%2BErpJPFj%2FKjsrubX5byPFZcV3qSAogG%2FTAHedIcuDAIthpGD9uzx7cw43FWl0T8FGhsJzQUAA3P0VtUsuFhCOdj0Mx9dqSjxwG293r%2BkDC%2BUwTjSsJtEhQhYfgkDXHw6s9%2BXa8vkPOwNM7KQTX9e2TEpRlYJbgCDvlSFWQ%3D%3D
```
<!-- input:end -->

<!-- cwd:var/instances/datasets/soap -->
<!-- input:dsa-d2.csv -->
```text
id,dataset,resource,base,model,property,type,ref,source,prepare,level,status,visibility,access,uri,eli,title,description
,datasets/gov/soap/ar/test,,,,,dataset,,,,,,,,open,,,
,,params_resource,,,,dask/csv,,var/instances/datasets/soap/params.csv,,,,,open,,,,
,,,,Params,,,,,,,,public,open,,,,
,,,,,DATA_TYPE,string,,DATA_TYPE,,,,,,,,,
,,,,,DATE_FROM,string,,DATE_FROM,,,,,,,,,
,,,,,DATE_TO,string,,DATE_TO,,,,,,,,,
,,,,,TIME,string,,TIME,,,,,,,,,
,,,,,CLIENT_NAME,string,,CLIENT_NAME,,,,,,,,,
,,,,,SIGNATURE,string,,SIGNATURE,,,,,,,,,
,,resource1,,,,dask/xml,,http://127.0.0.1:8014/v1/data/changes?DATA_TYPE={DATA_TYPE}&DATE_FROM={DATE_FROM}&DATE_TO={DATE_TO}&TIME={TIME}&CLIENT_NAME={CLIENT_NAME}&SIGNATURE={SIGNATURE},,,,,open,,,,
,,,,,,param,DATA_TYPE,datasets/gov/soap/ar/test/Params,read().DATA_TYPE,,,,open,,,,
,,,,,,param,DATE_FROM,datasets/gov/soap/ar/test/Params,read().DATE_FROM,,,,open,,,,
,,,,,,param,DATE_TO,datasets/gov/soap/ar/test/Params,read().DATE_TO,,,,open,,,,
,,,,,,param,TIME,datasets/gov/soap/ar/test/Params,read().TIME,,,,open,,,,
,,,,,,param,CLIENT_NAME,datasets/gov/soap/ar/test/Params,read().CLIENT_NAME,,,,open,,,,
,,,,,,param,SIGNATURE,datasets/gov/soap/ar/test/Params,read().SIGNATURE,,,,open,,,,
,,,,Street,,,,/CITY/C,,,,public,open,,,,
,,,,,kodas,string,,ID,,,,,,,,,
,,,,,id,integer,,@ID,,,,,,,,,
```
<!-- input:end -->

```nu
cp $"($INSTANCE)/params-1row.csv" $"($INSTANCE)/params.csv"
d-stop
d-swap "dsa-d2.csv"
$env.D_JOB = d-spawn
poll-url $"($SPINTA_D)/version" 120
```

The `Params` model reads the catalog file like any other model of the
dataset (all its rows are data):

```nu
http get -e -f $"($SPINTA_D)/($DATASET)/Params" | to-http
```

<!-- output:http -->
```http
HTTP/1.1 200 OK
content-type: application/json
date: Thu, 08 Oct 2026 15:56:10 GMT
transfer-encoding: chunked
server: uvicorn
strict-transport-security: max-age=31536000; includeSubDomains

{
  "_data": [
    {
      "_type": "datasets/gov/soap/ar/test/Params",
      "_id": "444b3451-3b82-4fec-9cb7-3788b0a41ec6",
      "_revision": null,
      "DATA_TYPE": "CITY",
      "DATE_FROM": "1761602400000",
      "DATE_TO": "1761688800000",
      "TIME": "1790237888370",
      "CLIENT_NAME": "CLIENT_A",
      "SIGNATURE": "HjB58H5ttS%2B%2F0JRmB3oC3FZT0eTR4Y9c3kg3gMRIIzWxmYIt%2F1gbFaSNZ7uTMSqkOmQ4%2FSGR7t7ai3VV1PxOz9pf28Nwv6Y7P6fbujGvlG3it%2BQMm%2FlA1jsHvXLi0lTT5BwUg91FkT6D4fUrOZHgys47E%2F%2F%2BBhroSGv1ft4X5Vc2IoVG%2BErpJPFj%2FKjsrubX5byPFZcV3qSAogG%2FTAHedIcuDAIthpGD9uzx7cw43FWl0T8FGhsJzQUAA3P0VtUsuFhCOdj0Mx9dqSjxwG293r%2BkDC%2BUwTjSsJtEhQhYfgkDXHw6s9%2BXa8vkPOwNM7KQTX9e2TEpRlYJbgCDvlSFWQ%3D%3D"
    }
  ]
}
```
<!-- output:end -->

…and the `Street` model is read from the service through the *parametrized*
request whose values resolve from that model:

```nu
http get -e -f $"($SPINTA_D)/($DATASET)/($MODEL)" | to-http
```

<!-- output:http -->
```http
HTTP/1.1 200 OK
transfer-encoding: chunked
strict-transport-security: max-age=31536000; includeSubDomains
content-type: application/json
date: Thu, 08 Oct 2026 15:56:10 GMT
server: uvicorn

{
  "_data": [
    {
      "_type": "datasets/gov/soap/ar/test/Street",
      "_id": "5f4adccc-a5b7-482a-820e-fce56c013e1e",
      "_revision": null,
      "kodas": "1",
      "id": null
    }
  ]
}
```
<!-- output:end -->

The service received exactly one parametrized request (one catalog row → one
request):

```nu
open ($INSTANCE + "/rcbroker.log") | lines | where $it =~ "REQUEST" | last | to text
```

<!-- output:code -->
```
REQUEST /v1/data/changes?DATA_TYPE=CITY&DATE_FROM=1761602400000&DATE_TO=1761688800000&TIME=1790237888370&CLIENT_NAME=CLIENT_A&SIGNATURE=HjB58H5ttS%2B%2F0JRmB3oC3FZT0eTR4Y9c3kg3gMRIIzWxmYIt%2F1gbFaSNZ7uTMSqkOmQ4%2FSGR7t7ai3VV1PxOz9pf28Nwv6Y7P6fbujGvlG3it%2BQMm%2FlA1jsHvXLi0lTT5BwUg91FkT6D4fUrOZHgys47E%2F%2F%2BBhroSGv1ft4X5Vc2IoVG%2BErpJPFj%2FKjsrubX5byPFZcV3qSAogG%2FTAHedIcuDAIthpGD9uzx7cw43FWl0T8FGhsJzQUAA3P0VtUsuFhCOdj0Mx9dqSjxwG293r%2BkDC%2BUwTjSsJtEhQhYfgkDXHw6s9%2BXa8vkPOwNM7KQTX9e2TEpRlYJbgCDvlSFWQ%3D%3D
```
<!-- output:end -->

### D.2b — a catalog with more than one row is a cartesian product

The same manifest with a *two-row* catalog (CITY and ADM rows, each with its
own signature column value). Each `param` row is resolved independently; each
of the six `read(Params)` formulas yields *two* values (one per catalog row),
so the parametrization builds the cartesian product of the parameter values:
2⁶ = 64 parametrized requests, including the 62 mismatched ones (e.g.
`DATA_TYPE=ADM` with the CITY signature):

<!-- cwd:var/instances/datasets/soap -->
<!-- input:params-d2b.csv -->
```text
DATA_TYPE,DATE_FROM,DATE_TO,TIME,CLIENT_NAME,SIGNATURE
CITY,1761602400000,1761688800000,1790237888370,CLIENT_A,HjB58H5ttS%2B%2F0JRmB3oC3FZT0eTR4Y9c3kg3gMRIIzWxmYIt%2F1gbFaSNZ7uTMSqkOmQ4%2FSGR7t7ai3VV1PxOz9pf28Nwv6Y7P6fbujGvlG3it%2BQMm%2FlA1jsHvXLi0lTT5BwUg91FkT6D4fUrOZHgys47E%2F%2F%2BBhroSGv1ft4X5Vc2IoVG%2BErpJPFj%2FKjsrubX5byPFZcV3qSAogG%2FTAHedIcuDAIthpGD9uzx7cw43FWl0T8FGhsJzQUAA3P0VtUsuFhCOdj0Mx9dqSjxwG293r%2BkDC%2BUwTjSsJtEhQhYfgkDXHw6s9%2BXa8vkPOwNM7KQTX9e2TEpRlYJbgCDvlSFWQ%3D%3D
ADM,1761602400000,1761688800000,1790237888370,CLIENT_A,CY1pxlqHFvZFOQH7I5WFxmJqNi6G7BW6H596bobD6krp54hVv%2F%2Fo1ohrbr8Hv%2F9wo3DRqHsqkukiRg0ndCnM1tEHQv8W%2FCIBlO2VnG5Gw390vXiFwXzg2EQX5Soi0xJNTUHFwo3xTkrjisiVApX9p23JAiUizSCM5N%2FIo5Mj9Lvy%2BXjrh%2FXzIxtTYTjhuT0UrqyDkluYCJMJys6OKfZ9ECxZ2zs2RIozBuivUcJplUpj5fLBwsKfzSkh5%2FUu72dMUy9rE3eMkctOIsXfv%2FcIkpNmPdoq9cofxRpZ2v%2FlUn963P65o%2BVNVE%2F8Rs2021AolinFnjzypH5rfkQBBEZZ2Q%3D%3D
```
<!-- input:end -->

```nu
cp $"($INSTANCE)/params-d2b.csv" $"($INSTANCE)/params.csv"
d-stop
d-swap "dsa-d2.csv"
$env.D_JOB = d-spawn
poll-url $"($SPINTA_D)/version" 120
let before = (open ($INSTANCE + "/rcbroker.log") | lines | where $it =~ "REQUEST" | length)
$env.D2B_N = (open ($INSTANCE + "/rcbroker.log") | lines | where $it =~ 'REQUEST /v1/data/changes\?' | length)
let resp = http get -e -f $"($SPINTA_D)/($DATASET)/($MODEL)"
assert ($resp.status == 200)
# 64 requests went out; the parsed dataframe ends up with 16 rows —
# dask deduplicates identical delayed computations, and every CITY-carried
# request returns the same row:
assert (($resp.body._data | length) == 16)
let after = (open ($INSTANCE + "/rcbroker.log") | lines | where $it =~ "REQUEST" | length)
assert (($after - $before) == 64)  # 2 rows ** 6 independently resolved params
"the one GET above made 64 parametrized requests (2⁶); the data list ends up with 16 (deduplicated) rows"
```

<!-- output: -->
the one GET above made 64 parametrized requests (2⁶); the data list ends up with 16 (deduplicated) rows
<!-- output:end -->

Of the 64 requests, half carry `DATA_TYPE=CITY` and half `DATA_TYPE=ADM`, the
CITY-signature and ADM-signature values spread evenly over both (every
parameter crossed with every other parameter):

```nu
let log = (open ($INSTANCE + "/rcbroker.log") | lines | where $it =~ 'REQUEST /v1/data/changes\?' | skip $env.D2B_N)
let counts = {
    city: ($log | where $it =~ 'DATA_TYPE=CITY' | length)
    adm: ($log | where $it =~ 'DATA_TYPE=ADM' | length)
    total: ($log | length)
}
assert ($counts.city == 32)
assert ($counts.adm == 32)
assert ($counts.total == 64)
$counts | to json -r
```

<!-- output:code -->
```
{"city":32,"adm":32,"total":64}
```
<!-- output:end -->

This is the caveat of the per-parameter `read()` shape: a multi-row params
catalog is not a list of *requests*, it is a cartesian-product expansion. The
shape below — the *complete* parametrized request (all parameters, signature
included) in one catalog column, combined with the `eval(param(...))` resource
prepare — avoids the blowup (one request per catalog row) and gives the
client query a job ([D.3b](#d3b-the-client-query-selects-a-pre-signed-request-from-a-catalog)).

### D.3 — the client URL query

The two manifest shapes above (`{placeholder}`s and `eval(param(...))`) open
two different channels for the client's URL query, observed live below: one
ignores it, the other selects requests with it.

### D.3a — the client URL query is ignored by the `{placeholder}` path

With the D.2 manifest (back on the one-row catalog), the client's query —
whatever it is — does not change the request Spinta makes (the
`parametrize_bases` path never receives the client query), and it does not
filter the response either unless it names a model property. Unquoted `X=v`
pairs parse as Spinta comparisons the way Scenario A showed — and are ignored
here in exactly the same way:

```nu
cp $"($INSTANCE)/params-1row.csv" $"($INSTANCE)/params.csv"
d-stop
d-swap "dsa-d2.csv"
$env.D_JOB = d-spawn
poll-url $"($SPINTA_D)/version" 120
let model_url = $"($SPINTA_D)/($DATASET)/($MODEL)"
# The client passes a *different* CLIENT_NAME — in every form Spinta's query
# parser accepts — the request still goes out with the catalog value:
let resp = http get -e -f $"($model_url)?CLIENT_NAME=EERHJA"
assert ($resp.status == 200)
assert (($resp.body._data.0.kodas | into string) == "1")
let resp = http get -e -f $"($model_url)?CLIENT_NAME='EERHJA'"
assert ($resp.status == 200)
assert (($resp.body._data.0.kodas | into string) == "1")
"both queries parse, both return the same data, neither reaches the request"
```

<!-- output: -->
both queries parse, both return the same data, neither reaches the request
<!-- output:end -->

…the requested value, evidenced by the rcbroker-like service log (the
`CLIENT_NAME` in the request is the catalog value `CLIENT_A`, not the
client-passed `EERHJA`):

```nu
open ($INSTANCE + "/rcbroker.log") | lines | where $it =~ "REQUEST" | last | str substring 0..121
```

<!-- output:code -->
```
REQUEST /v1/data/changes?DATA_TYPE=CITY&DATE_FROM=1761602400000&DATE_TO=1761688800000&TIME=1790237888370&CLIENT_NAME=CLIEN
```
<!-- output:end -->

A *property* filter on the model, on the other hand, does filter the response
data the way every Spinta model filter does (`kodas` is a model property) —
but the source request stays untouched regardless of it:

```nu
let model_url = $"($SPINTA_D)/($DATASET)/($MODEL)"
let n = (open ($INSTANCE + "/rcbroker.log") | lines | where $it =~ "REQUEST" | length)
let resp1 = http get -e -f ($model_url + "?kodas='1'")
assert ($resp1.status == 200)
assert (($resp1.body._data | length) == 1)
let n_mid = (open ($INSTANCE + "/rcbroker.log") | lines | where $it =~ "REQUEST" | length)
let resp2 = http get -e -f ($model_url + "?kodas='2'")
assert ($resp2.status == 200)
assert (($resp2.body._data | length) == 0)
let n_after = (open ($INSTANCE + "/rcbroker.log") | lines | where $it =~ "REQUEST" | length)
[
    "kodas='1': one row — the request went out and the filter matched its data"
    "kodas='2': no rows — the same request went out, the filter matched nothing"
    $"parametrized requests made by the two GETs: ($n_mid - $n) and ($n_after - $n_mid) — one per GET, all carrying the same catalog parameters"
] | to text
```

<!-- output: -->
kodas='1': one row — the request went out and the filter matched its data
kodas='2': no rows — the same request went out, the filter matched nothing
parametrized requests made by the two GETs: 1 and 1 — one per GET, all carrying the same catalog parameters
<!-- output:end -->

```nu
open ($INSTANCE + "/rcbroker.log") | lines | where $it =~ "REQUEST" | last | str substring 0..121
```

<!-- output:code -->
```
REQUEST /v1/data/changes?DATA_TYPE=CITY&DATE_FROM=1761602400000&DATE_TO=1761688800000&TIME=1790237888370&CLIENT_NAME=CLIEN
```
<!-- output:end -->

(The one place where the client query *is* seen is the eval-path below; in the
`{placeholder}` path it is unreachable — `parametrize_bases` calls `iterparams`
without `url_query_params`.)

### D.3b — the client query selects a pre-signed request from a catalog

The data source itself can come from a parameter: the resource carries *no*
source URL at all — its `prepare` is `eval(param(Url))` — and the `Url` param
row is resolved from a `dask/csv` catalog of *complete* request URLs (columns:
`name`, `URL`; the URL-encoded signature is embedded, as in D.1b/D.2). The
resolution goes through `eval_`'s `iterparams` call, which passes the client
query into the `read()` — the client's filters apply to the catalog model, and
the URL values it returns become the data source:

<!-- cwd:var/instances/datasets/soap -->
<!-- input:requests.csv -->
```text
name,URL
CITY,http://127.0.0.1:8014/v1/data/changes?DATA_TYPE=CITY&DATE_FROM=1761602400000&DATE_TO=1761688800000&TIME=1790237888370&CLIENT_NAME=CLIENT_A&SIGNATURE=HjB58H5ttS%2B%2F0JRmB3oC3FZT0eTR4Y9c3kg3gMRIIzWxmYIt%2F1gbFaSNZ7uTMSqkOmQ4%2FSGR7t7ai3VV1PxOz9pf28Nwv6Y7P6fbujGvlG3it%2BQMm%2FlA1jsHvXLi0lTT5BwUg91FkT6D4fUrOZHgys47E%2F%2F%2BBhroSGv1ft4X5Vc2IoVG%2BErpJPFj%2FKjsrubX5byPFZcV3qSAogG%2FTAHedIcuDAIthpGD9uzx7cw43FWl0T8FGhsJzQUAA3P0VtUsuFhCOdj0Mx9dqSjxwG293r%2BkDC%2BUwTjSsJtEhQhYfgkDXHw6s9%2BXa8vkPOwNM7KQTX9e2TEpRlYJbgCDvlSFWQ%3D%3D
ADM,http://127.0.0.1:8014/v1/data/changes?DATA_TYPE=ADM&DATE_FROM=1761602400000&DATE_TO=1761688800000&TIME=1790237888370&CLIENT_NAME=CLIENT_A&SIGNATURE=CY1pxlqHFvZFOQH7I5WFxmJqNi6G7BW6H596bobD6krp54hVv%2F%2Fo1ohrbr8Hv%2F9wo3DRqHsqkukiRg0ndCnM1tEHQv8W%2FCIBlO2VnG5Gw390vXiFwXzg2EQX5Soi0xJNTUHFwo3xTkrjisiVApX9p23JAiUizSCM5N%2FIo5Mj9Lvy%2BXjrh%2FXzIxtTYTjhuT0UrqyDkluYCJMJys6OKfZ9ECxZ2zs2RIozBuivUcJplUpj5fLBwsKfzSkh5%2FUu72dMUy9rE3eMkctOIsXfv%2FcIkpNmPdoq9cofxRpZ2v%2FlUn963P65o%2BVNVE%2F8Rs2021AolinFnjzypH5rfkQBBEZZ2Q%3D%3D
```
<!-- input:end -->

<!-- cwd:var/instances/datasets/soap -->
<!-- input:dsa-d3b.csv -->
```text
id,dataset,resource,base,model,property,type,ref,source,prepare,level,status,visibility,access,uri,eli,title,description
,datasets/gov/soap/ar/test,,,,,dataset,,,,,,,,open,,,
,,params_resource,,,,dask/csv,,var/instances/datasets/soap/requests.csv,,,,,open,,,,
,,,,Params,,,,,,,,public,open,,,,
,,,,,name,string,,name,,,,,,,,,
,,,,,URL,string,,URL,,,,,,,,,
,,resource1,,,,dask/xml,,,eval(param(Url)),,,,open,,,,
,,,,,,param,Url,datasets/gov/soap/ar/test/Params,read().URL,,,,open,,,,
,,,,Street,,,,/CITY/C,,,,public,open,,,,
,,,,,kodas,string,,ID,,,,,,,,,
,,,,,id,integer,,@ID,,,,,,,,,
```
<!-- input:end -->

```nu
d-stop
d-swap "dsa-d3b.csv"
$env.D_JOB = d-spawn
poll-url $"($SPINTA_D)/version" 120
```

The client picks the executed request by *filtering the catalog* with the
query. First `CITY` — the catalog row whose `name` is CITY (the request with
`DATA_TYPE=CITY` and the CITY signature — the one matching the `/CITY/C`
model source path):

```nu
let n = (open ($INSTANCE + "/rcbroker.log") | lines | where $it =~ "REQUEST" | length)
$env.D3B_N = $n
let resp = http get -e -f $"($SPINTA_D)/($DATASET)/($MODEL)?name='CITY'"
assert ($resp.status == 200)
assert (($resp.body._data | length) == 1)
$resp | to-http
```

<!-- output:http -->
```http
HTTP/1.1 200 OK
strict-transport-security: max-age=31536000; includeSubDomains
transfer-encoding: chunked
server: uvicorn
date: Thu, 08 Oct 2026 15:56:22 GMT
content-type: application/json

{
  "_data": [
    {
      "_type": "datasets/gov/soap/ar/test/Street",
      "_id": "5f4adccc-a5b7-482a-820e-fce56c013e1e",
      "_revision": null,
      "_base": null,
      "kodas": "1",
      "id": null
    }
  ]
}
```
<!-- output:end -->

Then `ADM` — the ADM request row (its own, ADM-signed request) is executed,
but its response contains no `/CITY/C` elements, so the data list is empty —
the same "source data does not match the model path" mechanics as Scenario A
showed with an error document:

```nu
let resp = http get -e -f $"($SPINTA_D)/($DATASET)/($MODEL)?name='ADM'"
assert ($resp.status == 200)
assert (($resp.body._data | length) == 0)
$resp | to-http
```

<!-- output:http -->
```http
HTTP/1.1 200 OK
strict-transport-security: max-age=31536000; includeSubDomains
transfer-encoding: chunked
date: Thu, 08 Oct 2026 15:56:22 GMT
server: uvicorn
content-type: application/json

{
  "_data": []
}
```
<!-- output:end -->

The rcbroker-like service log shows the two *different parametrized requests*
— `DATA_TYPE=CITY` with the CITY signature first, `DATA_TYPE=ADM` with the ADM
signature second — selected by nothing but the client's `?name='...'` filter
(the client cannot change parameter values; it can only select which one of
the catalog's pre-parametrized requests is executed):

```nu
open ($INSTANCE + "/rcbroker.log")
| lines
| where $it =~ "REQUEST"
| skip $env.D3B_N
| each {|l| $l | str replace -r "(CLIENT_NAME=[^&]+)&SIGNATURE=.*$" "$1"}
| to text
```

<!-- output:code -->
```
REQUEST /v1/data/changes?DATA_TYPE=CITY&DATE_FROM=1761602400000&DATE_TO=1761688800000&TIME=1790237888370&CLIENT_NAME=CLIENT_A
REQUEST /v1/data/changes?DATA_TYPE=ADM&DATE_FROM=1761602400000&DATE_TO=1761688800000&TIME=1790237888370&CLIENT_NAME=CLIENT_A
```
<!-- output:end -->

Without any filter the catalog is expanded — every row's request goes out
(the response still holds the CITY row, only it matches the `/CITY/C` model
source path; a client who wants a specific row filters for it):

```nu
let n = (open ($INSTANCE + "/rcbroker.log") | lines | where $it =~ "REQUEST" | length)
$env.D3B_N2 = $n
let resp = http get -e -f $"($SPINTA_D)/($DATASET)/($MODEL)"
assert ($resp.status == 200)
assert (($resp.body._data | length) == 1)  # the CITY row (only /CITY/C matches)
```

```nu
open ($INSTANCE + "/rcbroker.log")
| lines
| where $it =~ "REQUEST"
| skip $env.D3B_N2
| each {|l| $l | str replace -r "(CLIENT_NAME=[^&]+)&SIGNATURE=.*$" "$1"}
| to text
```

<!-- output:code -->
```
REQUEST /v1/data/changes?DATA_TYPE=CITY&DATE_FROM=1761602400000&DATE_TO=1761688800000&TIME=1790237888370&CLIENT_NAME=CLIENT_A
REQUEST /v1/data/changes?DATA_TYPE=ADM&DATE_FROM=1761602400000&DATE_TO=1761688800000&TIME=1790237888370&CLIENT_NAME=CLIENT_A
```
<!-- output:end -->

### D.4 — SOAP-style input() prepares are not resolved for dask sources

SOAP's answer to this task — `param` rows with `input('default')` prepares,
which pull values into the request at request time — does not work for
`dask/*` resources: the manifest loads and checks fine, but the source is
never requested. The `input()` prepare drops both the formula and the source
of the `param` row at manifest load (`param.formulas`/`param.sources` end up
empty) and `parametrize_bases` emits no source at all:

<!-- cwd:var/instances/datasets/soap -->
<!-- input:dsa-d4.csv -->
```text
id,dataset,resource,base,model,property,type,ref,source,prepare,level,status,visibility,access,uri,eli,title,description
,datasets/gov/soap/ar/test,,,,,dataset,,,,,,,,open,,,
,,resource1,,,,dask/xml,,http://127.0.0.1:8014/v1/data/changes?DATA_TYPE={DATA_TYPE}&DATE_FROM={DATE_FROM}&DATE_TO={DATE_TO}&TIME={TIME}&CLIENT_NAME={CLIENT_NAME}&SIGNATURE={SIGNATURE},,,,,open,,,,
,,,,,,param,DATA_TYPE,CITY,input('CITY'),,,,open,,,,
,,,,,,param,DATE_FROM,1761602400000,input('1761602400000'),,,,open,,,,
,,,,,,param,DATE_TO,1761688800000,input('1761688800000'),,,,open,,,,
,,,,,,param,TIME,1790237888370,input('1790237888370'),,,,open,,,,
,,,,,,param,CLIENT_NAME,CLIENT_A,input('CLIENT_A'),,,,open,,,,
,,,,,,param,SIGNATURE,,input(),,,,open,,,,
,,,,Street,,,,/CITY/C,,,,public,open,,,,
,,,,,kodas,string,,ID,,,,,,,,,
,,,,,id,integer,,@ID,,,,,,,,,
```
<!-- input:end -->

```nu
d-stop
d-swap "dsa-d4.csv"
$env.D_JOB = d-spawn
poll-url $"($SPINTA_D)/version" 120
let n = (open ($INSTANCE + "/rcbroker.log") | lines | where $it =~ "REQUEST" | length)
$env.D4_N = $n
http get -e -f $"($SPINTA_D)/($DATASET)/($MODEL)" | to-http
```

<!-- output:http -->
```http
HTTP/1.1 200 OK
transfer-encoding: chunked
server: uvicorn
content-type: application/json
strict-transport-security: max-age=31536000; includeSubDomains
date: Thu, 08 Oct 2026 15:56:26 GMT

{
  "_data": []
}
```
<!-- output:end -->

```nu
let n = (open ($INSTANCE + "/rcbroker.log") | lines | where $it =~ "REQUEST" | length)
assert (($n - $env.D4_N) == 0)
$"parametrized requests made by the GET above: ($n - $env.D4_N) — the source was never requested"
```

<!-- output:code -->
```
parametrized requests made by the GET above: 0 — the source was never requested
```
<!-- output:end -->

(An `input()`-prepared `dask/*` source silently yields empty data — the
manifest-shape equivalent of Scenario A, but without the source being hit at
all.)

### D.5 — rc_signature() prepare on a dask source fails at manifest load

The SOAP deferred-signature helper (`rc_signature(...)`, with `param(...)`
arguments, in the same shape the SOAP manifest uses) can not be referenced by
a `dask/*` param row: the `rc_signature`/`param` resolvers are known to the
SOAP machinery only — the manifest `LoadBuilder` knows neither, and resolving
`rc_signature(param(...))` at manifest load crashes with a raw `KeyError`
(`env.params`, which LoadBuilder does not have). `spinta bootstrap`/`check`
fails — the dataset can not even be loaded:

<!-- cwd:var/instances/datasets/soap -->
<!-- input:dsa-d5.csv -->
```text
id,dataset,resource,base,model,property,type,ref,source,prepare,level,status,visibility,access,uri,eli,title,description
,datasets/gov/soap/ar/test,,,,,dataset,,,,,,,,open,,,
,,resource1,,,,dask/xml,,http://127.0.0.1:8014/v1/data/changes?DATA_TYPE={DATA_TYPE}&DATE_FROM={DATE_FROM}&DATE_TO={DATE_TO}&TIME={TIME}&CLIENT_NAME={CLIENT_NAME}&SIGNATURE={SIGNATURE},,,,,open,,,,
,,,,,,param,DATA_TYPE,,'CITY',,,,open,,,,
,,,,,,param,DATE_FROM,,'1761602400000',,,,open,,,,
,,,,,,param,DATE_TO,,'1761688800000',,,,open,,,,
,,,,,,param,TIME,,'1790237888370',,,,open,,,,
,,,,,,param,CLIENT_NAME,,'CLIENT_A',,,,open,,,,
,,,,,,param,SIGNATURE,,"rc_signature(param(DATA_TYPE), param(DATE_FROM), param(DATE_TO), param(TIME), param(CLIENT_NAME))",,,,open,,,,
,,,,Street,,,,/CITY/C,,,,public,open,,,,
,,,,,kodas,string,,ID,,,,,,,,,
,,,,,id,integer,,@ID,,,,,,,,,
```
<!-- input:end -->

```nu
# No server restart here: the manifest is never loadable.
cp $"($INSTANCE)/dsa-d5.csv" $"($INSTANCE)/dsa-d.csv"
let out = (spinta check | complete)
assert ($out.exit_code != 0)
# The interesting part of the traceback (the raw KeyError while the LoadBuilder
# resolves the `param(...)` arguments of `rc_signature(...)`):
$out.stderr | lines | where {|l|
    ($l | str contains "ufuncs.py") or ($l | str contains "KeyError") or ($l | str contains "load_param")
} | to text
```

<!-- output:code -->
```
│ ❱ 104 │   resource.params = load_params(context, manifest, resource.params)                                                                                                                                                                  │
│ /home/sirex/dev/data/spinta/2052-unexpectedtoken-error/spinta/dimensions/param/helpers.py:43 in load_params                                                                                                                                  │
│ ❱ 43 │   │   │   load_param_formulas_and_sources(context, param, data["prepare"], data["source"].copy())                                                                                                                                     │
│ /home/sirex/dev/data/spinta/2052-unexpectedtoken-error/spinta/dimensions/param/helpers.py:26 in load_param_formulas_and_sources                                                                                                              │
│ /home/sirex/dev/data/spinta/2052-unexpectedtoken-error/spinta/core/ufuncs.py:59 in resolve                                                                                                                                                   │
│ /home/sirex/dev/data/spinta/2052-unexpectedtoken-error/spinta/dimensions/param/ufuncs.py:15 in param                                                                                                                                         │
│ /home/sirex/dev/data/spinta/2052-unexpectedtoken-error/spinta/core/ufuncs.py:201 in __getattr__                                                                                                                                              │
KeyError: 'params'
```
<!-- output:end -->

This is as close to `rc_signature()` as a manifest can get for a `dask/*`
resource today: without the Spinta-side change (the reverted session's
resolver wiring for the `dask` param builders) the only way to sign a
parametrized `dask` request is a precomputed signature value in the manifest
(D.1), in the params catalog (D.2) or in the pre-signed request catalog
(D.3b).

## The SOAP equivalent

For SOAP sources (`wsdl` resources) the same task — passing service request
parameters from the client to the source — is solved: the DSA declares `param`
rows whose `prepare` pulls values from the client request (`input()`), the
model properties reference them with `param(...)`, and `rc_signature()` can
compute a deferred signature server-side. This is the gap: `dask/xml` (and
`dask/json`) resources have no equivalent.

To show that the *same* service contract works when the resource type is
`wsdl` (the rcbroker-like SOAP service above exposes the same data over
SOAP), here is the SOAP manifest for the same service — this is the shape the
real SOAP services are configured with (illustrative, not referenced by the
config above — markout materializes it into the instance directory):

<!-- cwd:var/instances/datasets/soap -->
<!-- input:dsa-soap.csv -->
```text
id,dataset,resource,base,model,property,type,ref,source,prepare,level,status,visibility,access,uri,eli,title,description
,datasets/gov/soap/ar/test,,,,,dataset,,,,,,,,open,,,
,,soap_resource,,,param,action_type,string,,input/ActionType,input(),,,,,open,,,
,,,,,param,caller_code,string,,input/CallerCode,input(),,,,,open,,,
,,,,,param,parameters,string,,input/Parameters,input(),,,,,open,,,
,,,,,param,time,string,,input/Time,input(),,,,,open,,,
,,,,,param,signature,string,,input/Signature,rc_signature(),,,,open,,,
,,resource2,,,,wsdl,,http://127.0.0.1:8014/v1/data/changes?wsdl,,,,,,open,,,
,,,,City,,,,Result/C,,public,open,,,,
,,,,,kodas,string,,ID,,public,open,,,,
,,,,,signature,string,,,param(signature),,,open,,,
```
<!-- input:end -->

SOAP is out of scope for this document — it is listed here only as the
reference point for what `dask/xml` is missing. See `agentas.rst` (SOAP
adapters, `rc_signature()`) and `spinta/adapters/rc/signature_adapter.py` for
the deferred signature mechanism.

## Cleanup

```nu
try { job kill ($env.D_JOB? | default null) } catch { }
try { job kill $job } catch { }
try { job kill $rcbroker_job } catch { }
sleep 1sec
pkill -f $"rcbroker.py" | complete | ignore
pkill -f $"spinta run --port ($SPINTA_PORT)" | complete | ignore
pkill -f $"spinta run --port ($D_PORT)" | complete | ignore
print "cleaned up"
```

<!-- output: -->
cleaned up
<!-- output:end -->
