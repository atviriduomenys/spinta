from starlette.types import ASGIApp, Message, Receive, Scope, Send

from spinta.components import Context


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
                headers.append(
                    (b"strict-transport-security", self.value.encode("latin-1"))
                )
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
