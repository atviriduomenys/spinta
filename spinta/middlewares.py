from starlette.types import ASGIApp
from starlette.types import Receive
from starlette.types import Scope
from starlette.types import Send

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
            with self.context.fork('request') as context:
                scope.setdefault('state', {})
                scope['state']['context'] = context
                await self.app(scope, receive, send)
        else:
            await self.app(scope, receive, send)
