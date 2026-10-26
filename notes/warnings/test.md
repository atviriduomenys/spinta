# Manual E2E test — `spinta_*` scope deprecation warning ([#2046])

Verify the server-side behavior, end to end, of the fix in `Token.check_scope()`
(`spinta/auth.py:478`): presenting an access token that carries **old-format
scopes** (the deprecated `spinta_*` prefix) must

1. emit a `ScopeFormatDeprecationWarning` via `warnings.warn` instead of
   `log.warning`, so the notice is **deduplicated** — shown once per running
   server process, not once per `check_scope()` call (i.e. not per request and
   not per written row),
2. become **silent** when the filter is `ignore::DeprecationWarning`,
3. become an **error** when an `error::DeprecationWarning`-style filter is in
   effect.

The test must not touch the shared `spinta` database or a user-global
`~/.config/spinta`: it spins up a fully isolated Spinta instance, in its own
instance directory, with a dedicated Postgres database, its own auth keys and
its own client registry. Nothing is cleaned up afterwards — instead, the
instance directory and the dedicated database are recreated from scratch at
the start of every run, and the server is stopped at the end — no
configuration changes to your machine are needed.

*Tested on: Python 3.11, spinta 1.2.0, branch `2046-ignore-scope-warnings`,
Nushell 0.115.1, psql (PostgreSQL) 18.6, the repo's Citus/PostgreSQL docker
db.*

## Running

The scenario is a single, self-contained document plus a small helper library
of nushell commands, `notes/warnings/test.nu`, sourced by the Helper library
block below: a persistent nushell session is wanted (state like the Spinta
server job and tokens must survive between blocks), which is exactly what
`markout` provides.

All code blocks are executed by `notes/scripts/markout.py`:

```
nix develop -c markout notes/warnings/test.md
```

Pass `--dry-run --no-write` to only list the detected code blocks without
executing anything, and `--no-write` to run without touching the file. The
script writes the captured outputs back in place; if any block fails, the run
stops at the first failure and the file is not written.

The document runs the Spinta server on port **8021** in a background nushell
job. A server left over from an earlier run is stopped in the preflight block;
docker containers (`db`) are left running.

## Preconditions

- Working copy of the spinta repository on branch `2046-ignore-scope-warnings`.
- Dependencies installed: `nix develop -c poetry install`.
- Docker stack up (needs `db` only): `docker compose up -d db`.

> `DeprecationWarning` is hidden by default outside `__main__` (unless
> `PYTHONDEVMODE=1` is set), so the server under test is started with
> `PYTHONWARNINGS="default::DeprecationWarning"` to make the warning visible.
> Making the warning visible in an intentionally-controlled way is exactly one
> of the things this test covers.

## Conventions for this test

- Run `markout` from the repository root; every block below uses
  `$instance`-prefixed absolute paths, nothing depends on the current
  directory (the single exception is the relative `source` path of the
  helper library below, `notes/warnings/test.nu`, resolved against the
  repository root).
- Everything runs in one persistent nushell session: `let` variables, `def`s
  and `$env` entries survive between blocks. `$env` is used only for values
  that external commands must see through the environment: `SPINTA_CONFIG`
  (read by every `spinta` CLI call) and `INSTANCE`/`BASE_URL`/`DB_DSN`
  (substituted into `config.yml` by `envsubst`); everything else lives in
  plain nushell variables.
  `$env.SPINTA_CONFIG` is set
  once, in the Test config block below, and every `spinta` CLI call from
  then on (including the spawned server) picks it up from the environment —
  no per-call `env SPINTA_CONFIG=...` is needed.
- Inline Python snippets are kept in `input` blocks only where SQL can't
  reach (the in-process warning filter checks in Test 4), and the test config
  is kept as a `yaml` `input` block for syntax highlighting; the
  `<!-- cwd:var/instances/warnings -->` marker tells `markout` to
  materialize them into the instance directory before the `nu` blocks run
  (without a `cwd` marker, input files are written relative to the
  current directory).
- Database setup/cleanup uses `psql` directly: `nix develop` provides `psql`
  via the `pkgs.postgresql` package in `flake.nix`, and `markout` runs `nu`
  blocks through `nix develop -c`, so the `psql-query` helper below can call
  it.
- The server base URL is defined once as `$env.BASE_URL` (built from
  `$port`) in the configuration-variables block below; the `--port`
  argument, `pkill`/`ss` checks and the `token_issuer`/`resource_server`
  config values all reference it, so changing the port means changing one
  line.
- The manifest uses the exact layout proven to work on the repo's Citus
  database (dataset in the `d` column, resource row with an empty `d`
  column), so do not "re-indent" the rows.

## Setup

### Configuration variables

<!-- cwd:var/instances/warnings -->

```nu
# Directory of this document, used by the Helper library block below to
# source `test.nu` (helper commands used throughout this document).
const DIR = "notes/warnings"

let instance = ("var/instances/warnings" | path expand)
$env.INSTANCE = $instance
$env.SPINTA_CONFIG = ($instance + "/config.yml")

# `$env` is used only for variables that external commands must reach
# through the environment:
#   $env.SPINTA_CONFIG — read by every `spinta` CLI call (including the
#                        spawned server) from the environment
#   $env.INSTANCE      — substituted into `config.yml` by `envsubst` as
#                        `$INSTANCE`
#   $env.BASE_URL      — substituted into `config.yml` by `envsubst` as
#                        `$BASE_URL`
#   $env.DB_DSN        — substituted into `config.yml` by `envsubst` as
#                        `$DB_DSN`
# Everything else lives in plain nushell variables, which — like `$env`
# entries — survive between blocks in the persistent session.

# Server port and base URL:
#   $port         — port the Spinta server listens on (`--port`, pkill/ss
#                   checks)
#   $env.BASE_URL — server base URL, used by `poll-url`, `http get/post`
#                   calls; substituted into `config.yml` as `$BASE_URL` by
#                   envsubst
let port = "8021"
let base_url = $"http://127.0.0.1:($port)"
$env.BASE_URL = $base_url

# Database connection details:
#   $db_name   — database to connect to, used by the psql DROP/CREATE
#                calls; also the last `config.yml` DSN segment
#   $db_maint  — database `psql-query` connects to for admin commands (DROP
#                DATABASE cannot run against the database being dropped, so
#                psql must connect elsewhere)
#   $psql_conn — the psql connection parts below, bundled into one record
#                and passed to `psql-query` as its first argument
#   $env.DB_DSN — full DSN, substituted into `config.yml` by envsubst
#   `$BASE_URL` — `$env.BASE_URL` above, substituted into `config.yml`
#                 by envsubst along with `$INSTANCE`/`$DB_DSN`
let db_name = "spinta_2046_e2e"
let db_maint = "spinta"
let db_host = "localhost"
let db_port = "54321"
let db_user = "admin"
let db_password = "admin123"
$env.DB_DSN = $"postgresql://($db_user):($db_password)@($db_host):($db_port)/($db_name)"

# Connection details for `psql-query` (see `test.nu`):
let psql_conn = {
    host: $db_host
    port: $db_port
    user: $db_user
    password: $db_password
    maint: $db_maint
}
```

### Helper library

Helper commands used throughout this document live in the helper library
`test.nu`, next to this document: `psql-query` (database setup and the
sanity check below), `poll-url` (the server-startup blocks), `get-token`
(Tests 1–3) and `insert-city` (the write-path test, Test 2). The library is
sourced once per run — its definitions then live in the persistent nushell
session for the rest of the document — and its paths are anchored on
`test.nu` being sourced from the repository root:

```nu
source $"($DIR)/test.nu"
```

### Preflight

Check that we are in the repository root, stop any Spinta server left over
from a previous run and confirm port 8021 is free.


```nu
use std assert
assert ("flake.nix" | path exists)
pkill -f $"spinta run --port ($port)" | complete | ignore
sleep 1sec
assert ((ss -tln | complete | get stdout | lines | where {|l| $l | str contains $":($port)"} | is-empty))
```

### Recreate the instance directory

Everything this scenario generates lives under `$instance`
(ignored by git), so a previous run's state is removed on every run.

```nu
rm -rf $instance
mkdir $"($instance)/config"
```

### Dedicated Postgres database

Drop and re-create the dedicated test database, so the test is repeatable and
the shared `spinta` database (used by the repo's test suite) stays untouched.
The connection details must match the repo's `docker-compose.yml`, as in
`psql_conn` above, and are defined once in the previous block;
`$env.DB_DSN` is reused by `config.yml` via envsubst below.

The `psql-query conn [cmds]` helper — defined in the `test.nu` helper library,
sourced above — wraps `psql` with those connection details (passed as a
record, see `psql_conn` in the configuration-variables block above). It fails
the
whole markout block when psql exits non-zero (`ON_ERROR_STOP=1`), so a bad
SQL statement cannot slip through unnoticed.

Note the one-line calls: in the persistent PTY session a multi-line call
would be parsed as a bare `psql-query` (psql then opens an interactive
session reading from the pty, hanging the block). A list literal, however,
is a single expression even when spread over multiple lines, so a long
list of SQL commands can be split across lines — but the call must start
on the line it ends on.

```nu
psql-query $psql_conn [
    $"DROP DATABASE IF EXISTS ($db_name) WITH \(FORCE\)"
    $"CREATE DATABASE ($db_name)"
]
```

`WITH (FORCE)` kills leftover connections from a previous server run, so
the DROP cannot fail because the test database is still in use.

### Test manifest

A single model `City` under dataset `cities`, resource `default`, with three
integer properties, all access `open`:

<!-- input:manifest.txt -->
```text
d | r | b | m | property | type    | ref | source                   | access
cities                   |         |     |                          |
  | default              |         |     |                          |
  |   |   | City         |         |     | create_distributed_table | open
  |   |   |    | id      | integer |     | id                       | open
  |   |   |    | name    | string  |     | name                     | open
  |   |   |    | pop     | integer |     | pop                      | open
```
<!-- input:end -->

`create_distributed_table` as the model's `source` is only needed on the
repo's Citus db; skip that column value on a plain PostgreSQL, and the file
layout works the same.

### Test config

`config_path` points inside the instance dir, so auth keys, the client registry
and the keymap are created there instead of in the user-global
`~/.config/spinta`. Similarly, `file_log_path` points the rotating file log
(see Test 5) into the instance dir instead of the user-global
`~/.spinta_logs`.

<!-- input:config.yml -->
```yaml
env: test
config_path: $INSTANCE/config
file_log_path: $INSTANCE/logs
token_issuer: $BASE_URL
resource_server: $BASE_URL

default_auth_client: default

keymaps:
  default:
    type: sqlalchemy
    dsn: sqlite:///$INSTANCE/keymap.db

backends:
  default:
    type: postgresql
    dsn: $DB_DSN

manifest: default
manifests:
  default:
    type: csv
    path: $INSTANCE/manifest.csv
    backend: default
    keymap: default
    mode: internal

accesslog:
  type: file
  file: $INSTANCE/accesslog.json
```
<!-- input:end -->

```nu
# Read fully into a variable first: piping `open` directly into
# `save -f` for the *same* file truncates it (nu starts `save` while
# `envsubst` is still streaming, leaving the file empty).
let cfg = (open -r $env.SPINTA_CONFIG | envsubst)
$cfg | save -f $env.SPINTA_CONFIG
```

`token_issuer`/`resource_server` are required config for the `/auth/token`
endpoint (see "Configuration parameter 'token_issuer' is required" if
missing).

The `env: test` environment (see `spinta/config.py`) sets the
`default_distribution_strategy` to `undistributed` (a no-op on plain
PostgreSQL, and the exact value adapted by the model's
`create_distributed_table` source on the repo's Citus db), plus
`default_access_level: open`, which the model rows above rely on.

### Manifest as CSV

Bootstrap (below) refuses to run while the manifest file named in the config
is missing, so the CSV manifest must exist before it — it is converted from
the ASCII manifest above with `spinta copy`, so the manifest is not
duplicated in a second, hand-written format.

`spinta copy`, however, needs the client folder structure
(`clients/helpers/keymap.yml`) that `spinta bootstrap` would normally create,
and bootstrap cannot run yet. `spinta upgrade clients` breaks that
deadlock: it creates the client folder structure (plus auth server keys and
the default client) under the isolated instance `config_path` and does not
need the manifest:

```nu
spinta upgrade clients
spinta copy $"($instance)/manifest.txt" -o $"($instance)/manifest.csv"
```

<!-- output:code -->
```
Initializing auth server keys: var/instances/warnings/config
Initializing default auth client: var/instances/warnings/config
Script 'clients' check. Status: PASSED
Loading InlineManifest manifest default
```
<!-- output:end -->

(`source.type`, `prepare`, etc. columns stay empty — `spinta copy` fills the
`id,dataset,resource,…` tabular columns from the ASCII manifest and leaves
the rest blank.)

### Bootstrap, check, register test clients

```nu
spinta bootstrap
spinta check

# Register three test clients:
#   `old-client`     — legacy `spinta_*` scopes (target of this test)
#   `spinta-insert`  — only a legacy write scope, for the write-path test
#   `new-client`     — new-style `uapi:*` scopes (must NOT trigger a warning)
spinta client add -n old-client -s old-secret --add-secret --scope ([
    "spinta_getall"
    "spinta_getone"
    "spinta_search"
] | str join ' ')
spinta client add -n spinta-insert -s insert-secret --add-secret --scope "spinta_insert"
spinta client add -n new-client -s new-secret --add-secret --scope ([
    "uapi:/:getall"
    "uapi:/:getone"
    "uapi:/:search"
] | str join ' ')
```

## Helpers

Poll `http get` on a URL until it answers, fail after `attempts` seconds.
Two `try { ... } | complete` wrappers: `complete` swallows the exit code of
the (harmlessly failing) pre-up requests, so a failed poll never leaks a
non-zero exit code into the markout failure detector, and `try` keeps the
poll running through `http get` transport errors.

Both helpers — together with `psql-query` (database setup above) and
`insert-city` (the write-path helper in Test 2) — are defined in the helper
library `notes/warnings/test.nu`, sourced by the Helper library block above;
the blocks below only call them.

## Wait for the server port

Waiting on `ss` (the kernel's own port table) instead of probing HTTP avoids
a race: the `GET /auth/token` probe that precedes it would hit the
just-started server with a warning-triggering request, which is exactly what
Test 1 must deliver once and only once, plus it keeps the total block
duration below the markout per-block timeout.

## Start the Spinta server

First variant: make deprecation warnings visible with
`PYTHONWARNINGS="default::DeprecationWarning"`.

```nu
let job = job spawn {(
    env PYTHONWARNINGS="default::DeprecationWarning"
    spinta run --port $port o+e> ($instance + "/server.log")
)}
poll-url $"($base_url)/version" 120
```

The job id is kept in `$job` (variables survive in the persistent session) —
the teardown at the bottom of the document `job kill`s it.

## Test 1 — legacy scopes + `default::DeprecationWarning` → exactly one warning

```nu
let old_scope = {
    Authorization: $"Bearer (get-token "old-client" "old-secret" "spinta_getall")"
}
let new_scope = {
    Authorization: $"Bearer (get-token "new-client" "new-secret" "uapi:/:getall")"
}

# 2 authorized GETs with the legacy-scope token, 1 with the new-style token
# (its check_scope call emits nothing — no legacy scopes trigger no warning):
let r1 = (http get -e -f -H $old_scope $"($base_url)/cities/City")
let r2 = (http get -e -f -H $old_scope $"($base_url)/cities/City")
let r3 = (http get -e -f -H $new_scope $"($base_url)/cities/City")
assert ($r1.status == 200 and $r2.status == 200 and $r3.status == 200)
print $"statuses: ($r1.status), ($r2.status), ($r3.status)"
```

<!-- output: -->
statuses: 200, 200, 200
<!-- output:end -->

`check_scope()` runs again on every request, but the `warnings` module
deduplicates the notice, so the warning appears **exactly once** in the
server log — as logged by `py.warnings`, with the code context that triggered
it:

```nu
open ($instance + "/server.log") | lines | where $it =~ "ScopeFormatDeprecationWarning" | to text
```

<!-- output:code -->
```
WARNING - spinta/auth.py:1039: ScopeFormatDeprecationWarning: using 'spinta_*' scopes is deprecated and will be removed in a future version. Use 'uapi:*' scopes instead, see: https://ivpk.github.io/uapi/#section/Authorization/Scope
```
<!-- output:end -->

```nu
open --raw ($instance + "/server.log") | lines | where $it =~ "ScopeFormatDeprecationWarning" | length
```

<!-- output: -->
1
<!-- output:end -->

Note that `auth.py:1039` is the *call site* of `check_scope()` inside
`has_scope()` — the warning is raised from the `Token.check_scope()`
implementation at `spinta/auth.py:485` (search `stacklevel=2` there), which
Python attributes to the caller so the log shows the registered scope
handler.

## Test 2 — write path (row inserts) with a legacy-scope client

`check_scope()` fires on write actions as well. The separate `spinta-insert`
client authorizes writes with only a legacy write scope. Writes are issued
through the `insert-city` helper from `test.nu`; note the explicit
`Content-Type` passed through `--headers` (see the gotcha below the block):

```nu
let write_tok = (get-token "spinta-insert" "insert-secret" "spinta_insert")
let w1 = (insert-city $write_tok "Vilnius" 1000000)
let w2 = (insert-city $write_tok "Kaunas" 400000)
assert ($w1.status == 201 and $w2.status == 201)
print $"statuses: ($w1.status), ($w2.status)"
```

<!-- output: -->
statuses: 201, 201
<!-- output:end -->

Both inserts return HTTP **201 Created** (a data insert response, not the
HTTP 200 Typical of a query — don't "fix" the status here).

Verify the writes landed (a query with the legacy-scope reader token must
still see both rows):

```nu
let read_tok = {
    Authorization: $"Bearer (get-token "old-client" "old-secret" "spinta_getall")"
}
let resp = (http get -e -f -H $read_tok $"($base_url)/cities/City")
$resp.body._data | length
```

<!-- output: -->
2
<!-- output:end -->

… and the warning count is **still exactly 1** — the write path reaches the
same `check_scope()` location, and the per-process dedup therefore also holds
for written rows:

```nu
open ($instance + "/server.log") | lines | where $it =~ "ScopeFormatDeprecationWarning" | length
```

<!-- output: -->
1
<!-- output:end -->

> **Gotcha:** Nushell's `http post -t application/json` appends
> `; charset=utf-8` to that header, which spinta's write path rejects with
> `UnknownContentType 'application/json; charset=utf-8'`. Passing
> `Content-Type` via `--headers` sends a clean header value.

## Test 3 — silence the warning with `ignore::DeprecationWarning`

Stop the previous server and restart with the ignore filter (a fresh
`$job`, the log goes to a separate file):

```nu
job kill $job
let job = job spawn {(
    env PYTHONWARNINGS="ignore::DeprecationWarning"
    spinta run --port $port o+e> ($instance + "/server-ignore.log")
)}
poll-url $"($base_url)/version" 120
```

Repeat the legacy-scope requests:

```nu
let token = {
    Authorization: $"Bearer (get-token "old-client" "old-secret" "spinta_getall")"
}
let r1 = (http get -e -f -H $token $"($base_url)/cities/City")
assert ($r1.status == 200)

open ($instance + "/server-ignore.log") | lines | where $it =~ "ScopeFormatDeprecationWarning" | length
```

<!-- output: -->
0
<!-- output:end -->

Expected: requests succeed (HTTP 200) and **no** warning line shows up.
Filtered warnings are dropped by the `warnings` module before both the console
output and the `py.warnings` file log (Test 5).

> **Gotcha:** the dotted third-party category
> (`spinta.warnings.SpintaDeprecationWarning`) can't be used with
> `PYTHONWARNINGS` or `-W`: CPython applies those filters at interpreter
> startup, before third-party packages are importable, and ignores them with
> `Invalid -W option ignored: invalid module name: 'spinta.warnings'`. Use the
> stdlib `DeprecationWarning` category in environment variables and `-W`.
> Spinta's own category is usable in pytest
> (`-W "error::spinta.warnings.SpintaDeprecationWarning"` or the
> `filterwarnings` setting), because pytest applies filters *in-process*;
> that variant is covered by the unit tests in `tests/test_auth.py`.

## Test 4 — `error::DeprecationWarning` aborts the server at startup

Restart with the error filter:

```nu
job kill $job
let job = job spawn {(
    env PYTHONWARNINGS="error::DeprecationWarning"
    spinta run --port $port o+e> ($instance + "/server-error.log")
)}
let up = try {
    poll-url $"($env.BASE_URL)/version" 60
    true
} catch { |err|
    print $err.msg
    false
}
assert ($up == false)
```

<!-- output: -->
polling http://127.0.0.1:8021/version failed for 60s
<!-- output:end -->

The process **exits during startup**, before port 8021 can be served. This
filter turns every `DeprecationWarning` into an exception, and third-party
packages imported by spinta at startup emit such warnings too: with the
repo's pinned dependencies, the abort comes from `lark` (importing
`sre_parse`), and the `DeprecationWarning` line is the last line of the
traceback:

```nu
open ($instance + "/server-error.log") | lines | last
```

<!-- output: -->
DeprecationWarning: module 'sre_parse' is deprecated
<!-- output:end -->

The dead job is already gone by the time the next block runs (`job kill` on
it is skipped via `try`/`catch`):

```nu
try { job kill $job } catch { |err| "job already exited" }
```

<!-- output: -->
job already exited
<!-- output:end -->

This is expected: as with pytest's "turn any deprecation into an error" mode,
`error::DeprecationWarning` is a CI-style knob, not a per-request crash test.
To exercise *precisely* this test's target warning as an error — the exact
`Token.check_scope()` code path — apply a scoped error filter in-process, no
server needed. First with a legacy `spinta_*` scope (an exception must be
raised):

<!-- input:check_scope_old.py -->
```python
import warnings

from spinta.auth import Token

warnings.simplefilter('error', DeprecationWarning)
fake = {
    'iss': 'x',
    'sub': 'test',
    'aud': 'x',
    'client_id': 'test',
    'iat': 1,
    'exp': 2,
    'scope': 'spinta_getall',
    'jti': 'x',
}


class FakeValidator:
    def scope_insufficient(self, tok_scope, required_scopes):
        return False


tok = Token.__new__(Token)
tok._token = fake
tok._validator = FakeValidator()
tok.expires_in = 1
try:
    tok.check_scope('uapi:/:getall')
    print('no error!')
except DeprecationWarning as e:
    print(f'caught as error: {e}')
```
<!-- input:end -->

```nu
python ($instance + "/check_scope_old.py")
```

<!-- output: -->
caught as error: using 'spinta_*' scopes is deprecated and will be removed in a future version. Use 'uapi:*' scopes instead, see: https://ivpk.github.io/uapi/#section/Authorization/Scope
<!-- output:end -->

… and with the *same* fake token payload but **new-style** `uapi:*` scopes in
the `scope` claim (`'scope': 'uapi:/:getall uapi:/:getone'` instead of
`'scope': 'spinta_getall'` — the same script with that single value changed):

<!-- input:check_scope_new.py -->
```python
import warnings

from spinta.auth import Token

warnings.simplefilter('error', DeprecationWarning)
fake = {
    'iss': 'x',
    'sub': 'test',
    'aud': 'x',
    'client_id': 'test',
    'iat': 1,
    'exp': 2,
    'scope': 'uapi:/:getall uapi:/:getone',
    'jti': 'x',
}


class FakeValidator:
    def scope_insufficient(self, tok_scope, required_scopes):
        return False


tok = Token.__new__(Token)
tok._token = fake
tok._validator = FakeValidator()
tok.expires_in = 1
try:
    tok.check_scope('uapi:/:getall')
    print('no error!')
except DeprecationWarning as e:
    print(f'caught as error: {e}')
```
<!-- input:end -->

```nu
python ($instance + "/check_scope_new.py")
```

<!-- output: -->
no error!
<!-- output:end -->

— nothing triggers for a standardized-scope token.

## Test 5 — the file log captures the warning too

`setup_logging()` routes *displayed* (i.e. non-filtered) warnings into the
rotating file log via `logging.captureWarnings()`, under the `py.warnings`
logger. One displayed notice shows up in `$INSTANCE/logs/spinta_<date>.log`
(`file_log_path` from the test config above) as one `py.warnings - WARNING`
line, followed by one context line showing the source of the warning.

Because the log file is scoped to this instance (`file_log_path`), it only
receives entries from spinta processes run with this instance's config, and
the instance directory is wiped on every run — so the count is
deterministic: exactly one per server process run with the `default::`
filter (the Test 1/2 server here), none for the `ignore::` server, and one
per *process*, not per request: the log never accumulates hits from other
spinta runs (repo test-suite runs, instances) as `~/.spinta_logs/` would.

```nu
let logfile = ($instance + "/logs/spinta_" + (date now | format date '%Y-%m-%d') + ".log")
open $logfile | lines | where $it =~ "ScopeFormatDeprecationWarning" | length
```

<!-- output: -->
1
<!-- output:end -->

## Cleanup

Only stop the running Spinta server — the instance directory and the
dedicated test database are intentionally **left behind**: they are not
cleaned up here, because both are recreated from scratch at the start of
every run (see "Recreate the instance directory" and "Dedicated Postgres
database" above), which also makes any leftover state — server logs,
`accesslog.json`, keymap, or stale database rows — a record of the last test
run rather than a problem for the next one:

```nu
try { job kill $job } catch { |err| "server already stopped" }
sleep 1sec
pkill -f $"spinta run --port ($port)" | complete | ignore
assert ((ss -tln | complete | get stdout | lines | where {|l| $l | str contains $":($port)"} | is-empty))
print "cleaned up"
```

<!-- output: -->
cleaned up
<!-- output:end -->

Then re-verify the scenario with
`nix develop -c markout notes/warnings/test.md` — its
preflight stops any leftover server, wipes the instance directory and
drop/re-creates the database before the tests run.

### Sanity check

Neither the shared `spinta` database nor `~/.config/spinta` must have been
touched by this scenario — guaranteed by the isolated `config_path` and the
dedicated instance database:

```nu
pkill -f $"spinta run --port ($port)" | complete | ignore
open ("~/.config/spinta/clients/helpers/keymap.yml" | path expand)
```
