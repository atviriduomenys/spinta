from starlette.datastructures import Headers
from starlette.middleware.gzip import GZipMiddleware, GZipResponder, IdentityResponder
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from spinta.components import Context


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


class DebugAwareGZipResponder(GZipResponder):
    async def send_with_compression(self, message: Message) -> None:
        if message["type"] == "http.response.debug":
            await self.send(message)
            return

        await super().send_with_compression(message)


class DebugAwareIdentityResponder(IdentityResponder):
    async def send_with_compression(self, message: Message) -> None:
        if message["type"] == "http.response.debug":
            await self.send(message)
            return

        await super().send_with_compression(message)


class DebugAwareGZipMiddleware(GZipMiddleware):
    """
    This Middleware wraps GZipMiddleware and IdentityMiddleware to allow
    debug messages to be sent without compression (compression removes debug messages, which causes errors with TestClient
    using TemplateResponse).
    """

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # Copied over from GZipMiddleware 0.52.1 version

        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = Headers(scope=scope)

        if "gzip" in headers.get("Accept-Encoding", ""):
            responder = DebugAwareGZipResponder(
                self.app,
                self.minimum_size,
                compresslevel=self.compresslevel,
            )
        else:
            responder = DebugAwareIdentityResponder(
                self.app,
                self.minimum_size,
            )

        await responder(scope, receive, send)
