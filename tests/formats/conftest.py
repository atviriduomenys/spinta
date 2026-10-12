from __future__ import annotations

import asyncio
import socket
import threading
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from http.client import HTTPConnection
from typing import TYPE_CHECKING

import pytest
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import Receive, Scope, Send

from spinta import commands
from spinta.auth import AdminToken
from spinta.components import Context, Model
from spinta.core.config import RawConfig
from spinta.testing.manifest import load_manifest_and_context

if TYPE_CHECKING:
    from uvicorn import Server

ResponseRenderer = Callable[[Request], Response]
LiveResponse = Callable[[ResponseRenderer], AbstractContextManager[tuple[HTTPConnection, threading.Event]]]


@pytest.fixture(scope="module")
def streaming_model(rc: RawConfig, tmp_path_factory: pytest.TempPathFactory) -> tuple[Context, Model]:
    context, manifest = load_manifest_and_context(
        rc.fork({"file_log_path": str(tmp_path_factory.mktemp("streaming-logs"))}),
        """
        m     | property | type    | access
        Entry |          |         | open
              | value    | integer | open
        """,
    )
    context.set("auth.token", AdminToken())
    return context, commands.get_model(context, manifest, "Entry")


def _serve(server: Server, listener: socket.socket) -> None:
    loop = asyncio.SelectorEventLoop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(server.serve(sockets=[listener]))
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.run_until_complete(loop.shutdown_default_executor())
        loop.close()


@pytest.fixture
def live_response() -> LiveResponse:
    """Serve a rendered response over HTTP; stop the server when the test exits."""
    uvicorn = pytest.importorskip("uvicorn", reason="Requires the http extra")

    @contextmanager
    def start(render_response: ResponseRenderer) -> Iterator[tuple[HTTPConnection, threading.Event]]:
        response_finished = threading.Event()

        async def app(scope: Scope, receive: Receive, send: Send) -> None:
            try:
                response = render_response(Request(scope, receive))
                await response(scope, receive, send)
            finally:
                response_finished.set()

        with socket.socket() as listener:
            # Port 0 lets the OS choose a free port. Bind before starting the thread.
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            server = uvicorn.Server(uvicorn.Config(app, lifespan="off", http="h11", log_config=None, access_log=False))
            thread = threading.Thread(target=_serve, args=(server, listener), daemon=True)
            client = HTTPConnection(*listener.getsockname(), timeout=5)
            thread.start()
            try:
                yield client, response_finished
            finally:
                client.close()
                server.should_exit = True
                thread.join(5)
                assert not thread.is_alive(), "HTTP server did not stop"

    return start
