import asyncio
import typing

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse
from starlette.types import Receive, Scope, Send


class ContextMiddleware(BaseHTTPMiddleware):
    """Adds `request.state.context`.

    There is a global `context`, where all heavy things are preloaded as
    startup. This preloading happens on Starlette's 'startup' event.

    `ContextMiddleware` creates a fork of global preloaded context for each
    request and assigns it to `request.state.context`. Forked context can be
    modified in each request without effecting global context.
    """

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        self._orig_send = send
        if scope['type'] == 'http':
            with scope['app'].state.context.fork('request') as context:
                scope.setdefault('state', {})
                scope['state']['context'] = context
                await super().__call__(scope, receive, send)
        else:
            await super().__call__(scope, receive, send)

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        return await call_next(request)

    # TODO: Temporary fix for https://github.com/encode/starlette/issues/472
    async def call_next(self, request: Request) -> Response:
        loop = asyncio.get_event_loop()
        queue = asyncio.Queue()  # type: asyncio.Queue

        scope = request.scope
        receive = request.receive
        send = queue.put

        async def coro() -> None:
            try:
                await self.app(scope, receive, send)
            finally:
                await queue.put(None)

        task = loop.create_task(coro())
        message = await queue.get()
        if message is None:
            task.result()
            raise RuntimeError("No response returned.")

        if "http.response.template" in scope.get("extensions", {}):
            if message["type"] == "http.response.template":
                await self._orig_send(message)
                message = await queue.get()

        assert message["type"] == "http.response.start"

        async def body_stream() -> typing.AsyncGenerator[bytes, None]:
            while True:
                message = await queue.get()
                if message is None:
                    break
                assert message["type"] == "http.response.body"
                yield message["body"]
            task.result()

        response = StreamingResponse(
            status_code=message["status"], content=body_stream()
        )
        response.raw_headers = message["headers"]
        return response
