#!/usr/bin/env nu
# Spinta `spinta_*` scope deprecation warning E2E test — helper library.
#
# This file is a library of helper commands, sourced by
# `notes/warnings/test.md`, which contains the actual test steps, their
# explanations and captured outputs.
#
# The commands below rely on `$env.BASE_URL` defined by the
# configuration-variables block in test.md (`psql-query` takes the database
# connection details as a record argument — see `psql_conn` there).
#
# Must be run from the repository root (test.md sources this file there),
# inside the `nix develop` shell — the commands rely on tools provided by
# the dev shell: `psql` (via `pkgs.postgresql` in `flake.nix`) and the
# spinta venv.

use std assert

# Wrap `psql` with the given connection details (a record with `host`,
# `port`, `user` and `password` fields, plus the optional `maint` database).
# Fails the whole
# markout block when psql exits non-zero (`ON_ERROR_STOP=1`), so a bad SQL
# statement cannot slip through unnoticed.
def psql-query [
    conn: record,                # connection details (see `psql_conn` in test.md)
    cmds: list<string>,          # SQL commands, executed in order
] {
    let db = ($conn.maint? | default "postgres")
    let r = with-env { PGPASSWORD: $conn.password } {(
        psql
            -h $conn.host
            -p $conn.port
            -U $conn.user
            -d $db
            -v ON_ERROR_STOP=1
            -P pager=off
            -qtA
            ...($cmds | each {|c| [-c $c]} | flatten)
        | complete
    )}
    if $r.exit_code != 0 {
        error make {
            msg: $"psql failed (exit ($r.exit_code)): ($r.stderr | str trim)"
        }
    }
    $r.stdout | str trim
}

# Poll `http get` on a URL until it answers, fail after `attempts` seconds.
# Two `try { ... } | complete` wrappers: `complete` swallows the exit code of
# the (harmlessly failing) pre-up requests, so a failed poll never leaks a
# non-zero exit code into the markout failure detector, and `try` keeps the
# poll running through `http get` transport errors.
def poll-url [url: string, attempts: int = 60] {
    mut ok = false
    for i in (seq 1 $attempts) {
        try {
            let resp = try {
                http get -e -f --max-time 2sec $url
            } catch { |err| {
                status: 0
            }}
            if $resp.status == 200 {
                $ok = true
                break
            }
        } catch { |err| }
        sleep 1sec
    }
    if not $ok {
        error make { msg: $"polling ($url) failed for ($attempts)s" }
    }
}

# Fetch a client-credentials access token from the `/auth/token` endpoint.
def get-token [user: string, pass: string, scope: string] {
    let resp = (http post -e -f --user $user --password $pass -t "application/x-www-form-urlencoded" $"($env.BASE_URL)/auth/token" {grant_type: "client_credentials", scope: $scope})
    assert ($resp.status == 200)
    $resp.body.access_token
}

# Insert one `City` row through the REST API. Note the explicit `Content-Type`
# passed via `--headers` (nushell's `http post -t application/json` appends
# `; charset=utf-8`, which spinta's write path rejects — see the gotcha in
# test.md, Test 2).
def insert-city [tok: string, name: string, pop: int] {
    http post -e -f --headers {
        Content-Type: "application/json",
        Authorization: $"Bearer ($tok)"
    } $"($env.BASE_URL)/cities/City" ({
        _type: "cities/City",
        name: $name,
        pop: $pop
    } | to json --raw)
}
