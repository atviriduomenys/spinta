#!/usr/bin/env nu
# Spinta × rcbroker-like REST service E2E test helpers.
#
# This file is a library of helper commands, used (sourced) by
# `notes/datasets/soap/test.md`, which contains the actual test steps, their
# explanations and outputs.
#
# Must be run from the repository root — the commands assume paths relative to
# the repository root and take URLs and other parameters from constants
# (e.g. `$RC`, `$SPINTA`) defined by test.md as nushell `const` values.

# On failure, the error is reported at the `assert` call site (not here), via
# the span of the condition expression.
def assert [cond: bool] {
    if not $cond {
        error make {
            msg: "assertion failed"
            label: {
                text: "this assertion failed"
                span: (metadata $cond).span
            }
        }
    }
}

# Poll a URL until it answers with HTTP 200, fail after `attempts` seconds.
def poll-url [url: string, attempts: int = 60] {
    mut ok = false
    for i in (seq 1 $attempts) {
        try {
            let r = http get -e -f $url
            if $r.status == 200 {
                $ok = true
                break
            }
        } catch { }
        sleep 1sec
    }
    assert $ok
}

def status-reason [status: int] {
    {
        200: "OK"
        201: "Created"
        204: "No Content"
        400: "Bad Request"
        401: "Unauthorized"
        403: "Forbidden"
        404: "Not Found"
        422: "Unprocessable Content"
        500: "Internal Server Error"
    } | get -o $"($status)" | default ""
}

# Wrap an `http get`/`http post` response record (as returned by
# `http get -e -f`, ...) and print it as a plain HTTP response: status line,
# headers and body. Used to render HTTP responses in `output:http` blocks.
def to-http [] {
    let resp = $in
    let headers = (
        $resp.headers
        | get -o response
        | default []
        | each {|h| $"($h.name): ($h.value)"}
        | str join (char nl)
    )
    let content_type = (
        $resp.headers
        | get -o response
        | default []
        | where {|h| ($h.name | str lowercase) == "content-type"}
        | get -o 0.value
        | default ""
    )
    let body = if ($content_type | str contains "xml") {
        # XML bodies are parsed by nu into a record — render them back as XML
        # (it is the service contract format here).
        match ($resp.body | describe -d).type {
            "record" => ($resp.body | to xml)
            _ => ($resp.body | to json -r)
        }
    } else if ($content_type | str contains "json") {
        # Format JSON bodies for readability.
        $resp.body | to json --indent 2
    } else {
        match ($resp.body | describe -d).type {
            "string" => $resp.body,
            "binary" => ($resp.body | decode utf-8),
            _ => ($resp.body | to json -r)
        }
    }
    # Nushell's http commands don't report the HTTP version, assume HTTP/1.1.
    let reason = status-reason $resp.status
    let status_line = if $reason == "" {
        $"HTTP/1.1 ($resp.status)"
    } else {
        $"HTTP/1.1 ($resp.status) ($reason)"
    }
    # Print via an argument — `print` in a pipeline tail (`| print`) does not
    # add a trailing newline. Trim trailing newlines and add one, so that
    # responses printed one after another are separated with a single blank
    # line.
    let text = [
        $status_line
        $headers
        ""
        $body
    ] | str join (char nl) | str trim -r
    print $"($text)\n"
}
