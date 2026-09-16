import posixpath

from starlette.datastructures import MutableHeaders
from starlette.middleware.gzip import GZipMiddleware
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from spinta.components import Context


def _is_normalized_path(scope: Scope) -> bool:
    raw_path = (scope.get("raw_path") or b"").lower()
    # Percent-encoded slashes, backslashes and dots decode into extra path
    # delimiters and dot segments that shared caches do not see when they
    # build the cache key.
    if b"%2f" in raw_path or b"%5c" in raw_path or b"%2e" in raw_path:
        return False
    path = scope["path"]
    if "\\" in path:
        return False
    normalized = posixpath.normpath(path)
    # posixpath.normpath preserves a leading "//" (POSIX leaves two leading
    # slashes implementation-defined), but shared caches collapse it to "/",
    # so treat such a path as non-normalized.
    if normalized.startswith("//"):
        normalized = normalized[1:]
    if path.endswith("/") and normalized != "/":
        normalized += "/"
    return path == normalized


class PathNormalizationMiddleware:
    """Rejects requests whose path is not in normalized form.

    Shared caches (proxies, CDNs) normalize URLs (resolve `.` and `..`
    segments, decode percent-encoded slashes, collapse duplicate slashes)
    when building the cache key, while the application receives the original
    path and may reflect it in the response. This discrepancy enables web
    cache poisoning: a response generated for a malicious non-normalized path
    gets cached under the key of a legitimate URL. Rejecting such requests
    with a non-cacheable response closes that gap.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http" and not _is_normalized_path(scope):
            response = JSONResponse(
                {
                    "errors": [
                        {
                            "code": "InvalidRequestPath",
                            "message": "Request path is not normalized.",
                        }
                    ]
                },
                status_code=404,
                headers={"Cache-Control": "no-store"},
            )
            await response(scope, receive, send)
            return
        await self.app(scope, receive, send)


class StrictTransportSecurityMiddleware:
    """Adds `Strict-Transport-Security` (HSTS) header to every HTTP response.

    HSTS instructs browsers to only communicate with the server via HTTPS. The
    header value is configurable via the `http_strict_transport_security`
    configuration option and defaults to a `max-age` of one year together with
    the `includeSubDomains` directive.
    """

    def __init__(self, app: ASGIApp, *, value: str) -> None:
        self.app = app
        self.value = value

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or not self.value:
            await self.app(scope, receive, send)
            return

        async def send_with_hsts(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = message.setdefault("headers", [])
                headers.append((b"strict-transport-security", self.value.encode("latin-1")))
            await send(message)

        await self.app(scope, receive, send_with_hsts)


class ContextMiddleware:
    """Adds `request.state.context`.

    There is a global `context`, where all heavy things are preloaded as
    startup. This preloading happens on Starlette's 'startup' event.

    `ContextMiddleware` creates a fork of global preloaded context for each
    request and assigns it to `request.state.context`. Forked context can be
    modified in each request without effecting global context.
    """

    def __init__(self, app: ASGIApp, *, context: Context) -> None:
        self.app = app
        self.context = context

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] in ["http", "websocket"]:
            with self.context.fork("request") as context:
                scope.setdefault("state", {})
                scope["state"]["context"] = context
                await self.app(scope, receive, send)
        else:
            await self.app(scope, receive, send)


def _append_accept_encoding(headers: MutableHeaders) -> None:
    # Gzip negotiation can affect the selected representation.
    # Normalize Vary after Starlette has processed the response.
    vary_values = [value.strip() for value in headers.get("vary", "").split(",") if value.strip()]

    if "accept-encoding" not in {value.lower() for value in vary_values}:
        vary_values.append("Accept-Encoding")

    headers["vary"] = ", ".join(vary_values)


class DebugAwareGZipMiddleware:
    """
    Currently, starlette's GZipMiddleware does not handle debug messages, and it just removes them.

    Since GZipMiddleware does not support injecting custom responders without copying and pasting the entire call logic, we have to
    create a separate ` app ` and ` send ` wrappers that are able to intercept events.

    To achieve this, we have to store GZipMiddleware object as a variable, instead of extending it as a parent,
    this allows us to create a custom ` app ` wrapper, which would skip parsing debug messages and just return them as given.

    To pass the original send function, we have to hijack the scope variable and insert the original function there. (This could be considered
    a hack, but prior to that, the original scope is copied to not create issues later on.)

    During the ` __call__ ` method we also hijack the send function to change etags from strong to weak (this is needed for compression
    if we cannot ensure that compressed data will have unique revisions, this is also done inside nginx compression module.)

    """

    def __init__(self, app: ASGIApp, **gzip_options) -> None:
        self.app = app
        self.gzip = GZipMiddleware(
            self.app_with_debug,
            **gzip_options,
        )

    async def app_with_debug(self, scope: Scope, receive: Receive, gzip_send: Send) -> None:
        original_send: Send = scope["_spinta_gzip_original_send"]

        async def send_with_debug(message: Message) -> None:
            if message["type"] == "http.response.debug":
                await original_send(message)
            else:
                await gzip_send(message)

        await self.app(scope, receive, send_with_debug)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # Copy rather than modify the caller's scope.
        request_scope = dict(scope)
        request_scope["_spinta_gzip_original_send"] = send

        async def send_response(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = MutableHeaders(raw=message["headers"])
                etag = headers.get("etag")

                _append_accept_encoding(headers)

                if headers.get("content-encoding") == "gzip" and etag and not etag.startswith("W/"):
                    headers["etag"] = f"W/{etag}"

            await send(message)

        await self.gzip(request_scope, receive, send_response)
