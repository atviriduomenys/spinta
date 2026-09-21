#!/usr/bin/env nu
# Spinta × KeyCloak local end-to-end test suite.
#
# This file is a library of helper commands, used (sourced) by
# `notes/auth/keycloak/test.md`, which contains the actual test steps, their
# explanations and outputs.
#
# Command parameters are given by test.md, which defines all constants (e.g.
# `$KEYCLOAK`, `$REALM`, `$INSTANCE`, `$REDIRECT_URI`) as nushell `const`
# values in its first code block and passes them explicitly to the commands
# below.
#
# Must be run from the repository root, inside the `nix develop` shell — the
# commands rely on tools provided by the dev shell: spinta, docker, the
# nushell `formats` plugin, etc.
#
# Hint: run `make env` first, so poetry creates the `.venv` used inside the
# nix shell.

plugin use --plugin-config $nu.plugin-path formats

# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

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

# Decode JWT claims (payload part), signature is not verified.
def jwt-claims [token: string] {
    $token | split row "." | get 1 | decode base64 --nopad | decode | from json
}

# Decode JWT token
def decode-jwt [token: string] {
    let parts = $token | split row "."

    if ($parts | length) != 3 {
        error make { msg: "Invalid JWT structure: token must contain 3 parts separated by '.'" }
    }

    let header = $parts.0 | decode base64 --nopad --url | decode utf-8 | from json
    let payload = $parts.1 | decode base64 --nopad --url | decode utf-8 | from json

    {
        header: $header
        payload: $payload
        signature: $parts.2
    }
}

# Textual representation (reason phrase) of common HTTP status codes.
# Nushell's http commands report only the status code, not its reason phrase,
# so phrases are looked up in the table below.
def status-reason [status: int] {
    {
        100: "Continue"
        101: "Switching Protocols"
        200: "OK"
        201: "Created"
        202: "Accepted"
        203: "Non-Authoritative Information"
        204: "No Content"
        205: "Reset Content"
        206: "Partial Content"
        300: "Multiple Choices"
        301: "Moved Permanently"
        302: "Found"
        303: "See Other"
        304: "Not Modified"
        307: "Temporary Redirect"
        308: "Permanent Redirect"
        400: "Bad Request"
        401: "Unauthorized"
        402: "Payment Required"
        403: "Forbidden"
        404: "Not Found"
        405: "Method Not Allowed"
        406: "Not Acceptable"
        408: "Request Timeout"
        409: "Conflict"
        410: "Gone"
        411: "Length Required"
        412: "Precondition Failed"
        413: "Content Too Large"
        414: "URI Too Long"
        415: "Unsupported Media Type"
        416: "Range Not Satisfiable"
        417: "Expectation Failed"
        418: "I'm a teapot"
        422: "Unprocessable Content"
        426: "Upgrade Required"
        428: "Precondition Required"
        429: "Too Many Requests"
        431: "Request Header Fields Too Large"
        451: "Unavailable For Legal Reasons"
        500: "Internal Server Error"
        501: "Not Implemented"
        502: "Bad Gateway"
        503: "Service Unavailable"
        504: "Gateway Timeout"
        505: "HTTP Version Not Supported"
    } | get -o $"($status)" | default ""
}

# Format a full nushell http command response (returned by `http get -e -f`,
# `http post -e -f`, ...) as a plain HTTP response: status line, response
# headers and body. Used by test.md to display HTTP responses in `output:http`
# code blocks. Prints the response (nushell displays only the last value of a
# multi-line entry, so returning a value would discard all but the last
# response), expects a response record on the pipeline input:
#
#   $resp | to-http
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
    let body = if ($content_type | str contains "application/json") {
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

# POST a record as an `application/x-www-form-urlencoded` form. A command
# wrapper around `http post`: the content type is fixed here, everything else
# (flags like `-e`, `-f`, `-m`, `-R`, `-H`, the URL and the form data) is
# passed through to `http post` as-is.
alias "http form" = http post -t "application/x-www-form-urlencoded"

# ---------------------------------------------------------------------------
# KeyCloak Admin API helpers
# ---------------------------------------------------------------------------

# Authorization header for the KeyCloak Admin API, obtained via the password
# grant of the built-in `admin-cli` client.
def admin-auth [url: string] {
    let resp = http form -e -f $"($url)/realms/master/protocol/openid-connect/token" {
        client_id: admin-cli,
        grant_type: password,
        username: admin,
        password: admin
    }
    let token = $resp.body
    assert ($token | get access_token? | is-not-empty)
    {Authorization: $"Bearer ($token.access_token)"}
}


# ---------------------------------------------------------------------------
# Authorization code flow helpers
# ---------------------------------------------------------------------------

# Raise a catchable error with a plain message.
def fail [msg: string] {
    error make {msg: $msg}
}

# Extract KeyCloak login form action URL from the login page HTML.
# KeyCloak login form: <form id="kc-form-login" onsubmit="..." action="...">
def login-form-action [html: string] {
    let action = (
        $html
        | parse -r '(?is)<form[^>]*id="kc-form-login"[^>]*action="([^"]*)"'
        | get -o 0.capture0
    )
    if $action == null {
        fail $"Can't find login form action in:\n($html)"
    }
    $action | str replace -a "&amp;" "&"
}

# Build a Cookie header from full response headers. KeyCloak (26.x) sets
# `Secure` on its cookies even over plain HTTP, while nushell's http commands
# treat such cookies as HTTPS-only. Send the cookies collected from response
# headers manually, so the login form POST is treated as part of the same
# authentication session.
def cookie-header [headers: any] {
    let cookies = (
        $headers.response
        | where name == "set-cookie"
        | get value
        | each {|cookie| $cookie | split row ";" | get 0}
    )
    $cookies | str join "; "
}

# GET authorization endpoint with response_type=code (follow redirects to reach
# the login form), POST user credentials without following redirects and read
# `code` from the Location header of the redirect to `redirect_uri`.
#
# `resource` is the OAuth 2.0 Resource Indicator (RFC 8707) — an identifier of
# the resource server (Spinta) the token is requested for.
def get-auth-code [
    auth_endpoint: string,
    redirect_uri: string,
    client_id: string,
    username: string,
    password: string,
    scope: string,
    resource: string,
] {
    let query = ({
        client_id: $client_id,
        response_type: "code",
        scope: $scope,
        resource: $resource,
        redirect_uri: $redirect_uri,
    } | url build-query)
    let resp = http get -e -f -m 30sec $"($auth_endpoint)?($query)"
    let action = login-form-action ($resp.body | into string)
    let cookie = cookie-header $resp.headers
    let headers = if $cookie == "" {
        {}
    } else {
        {Cookie: $cookie}
    }

    let resp = (
        http form -e -f -m 30sec -R manual -H $headers $action {
            username: $username,
            password: $password,
            credentialId: "",
        }
    )

    # Successful login redirects to the redirect_uri with ?code=...&session_state=...
    if $resp.status not-in [302 303] {
        fail $"Expected redirect after login, got HTTP ($resp.status):\n($resp.body | into string)"
    }

    let location = (
        $resp.headers.response
        | where name == "location"
        | get -o 0.value
        | default ""
    )
    let code = (
        $location
        | url parse
        | get -o query
        | default ""
        | url split-query
        | where key == "code"
        | get -o 0.value
    )
    if $code == null {
        fail $"No code in redirect Location ($location)"
    }
    $code
}
