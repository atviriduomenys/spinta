import asyncio
import gzip

import pytest
from jinja2 import DictLoader, Environment
from starlette.datastructures import Headers
from starlette.requests import Request
from starlette.responses import Response
from starlette.templating import Jinja2Templates
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from spinta.middlewares import DebugAwareGZipMiddleware


def _http_scope(accept_encoding: str, path: str = "/") -> Scope:
    return {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "root_path": "",
        "query_string": b"",
        "headers": [(b"accept-encoding", accept_encoding.encode())],
        "client": ("testclient", 50000),
        "server": ("testserver", 80),
        "extensions": {"http.response.debug": {}},
    }


async def _collect_messages(app: ASGIApp, scope: Scope) -> list[Message]:
    messages = []

    async def receive() -> Message:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: Message) -> None:
        messages.append(message)

    await app(scope, receive, send)
    return messages


def _response_headers(messages: list[Message]) -> Headers:
    starts = [message for message in messages if message["type"] == "http.response.start"]
    assert len(starts) == 1
    assert starts[0]["status"] == 200
    return Headers(raw=starts[0]["headers"])


def _response_body(messages: list[Message]) -> bytes:
    bodies = [message for message in messages if message["type"] == "http.response.body"]
    assert bodies
    assert not bodies[-1].get("more_body", False)
    return b"".join(message.get("body", b"") for message in bodies)


def _response_app(body: bytes, etag: str | None = None) -> ASGIApp:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        headers = {"ETag": etag} if etag is not None else {}
        await Response(body, media_type="text/plain", headers=headers)(scope, receive, send)

    return app


@pytest.mark.asyncio
@pytest.mark.parametrize("accept_encoding", ["gzip", "identity"])
async def test_gzip_preserves_template_debug_metadata(accept_encoding: str):
    value = "hello" * 200
    templates = Jinja2Templates(env=Environment(loader=DictLoader({"page.html": "{{ value }}"})))

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        response = templates.TemplateResponse(request=Request(scope), name="page.html", context={"value": value})
        await response(scope, receive, send)

    middleware = DebugAwareGZipMiddleware(app, minimum_size=100)
    messages = await _collect_messages(middleware, _http_scope(accept_encoding))

    debug = [message for message in messages if message["type"] == "http.response.debug"]
    assert len(debug) == 1
    assert messages[0]["type"] == "http.response.debug"
    assert debug[0]["info"]["template"].name == "page.html"
    assert debug[0]["info"]["context"]["value"] == value

    headers = _response_headers(messages)
    body = _response_body(messages)
    if accept_encoding == "gzip":
        assert headers["content-encoding"] == "gzip"
        assert gzip.decompress(body) == value.encode()
    else:
        assert "content-encoding" not in headers
        assert body == value.encode()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "accept_encoding,body_size,etag,expected_encoding,expected_etag",
    [
        ("gzip", 100, '"revision"', "gzip", 'W/"revision"'),
        ("gzip", 99, '"revision"', None, '"revision"'),
        ("identity", 100, '"revision"', None, '"revision"'),
        ("gzip", 100, 'W/"revision"', "gzip", 'W/"revision"'),
        ("gzip", 100, None, "gzip", None),
    ],
)
async def test_gzip_response_etag(
    accept_encoding: str,
    body_size: int,
    etag: str | None,
    expected_encoding: str | None,
    expected_etag: str | None,
):
    body = b"x" * body_size
    middleware = DebugAwareGZipMiddleware(_response_app(body, etag), minimum_size=100)
    messages = await _collect_messages(middleware, _http_scope(accept_encoding))

    headers = _response_headers(messages)
    assert headers.get("content-encoding") == expected_encoding
    assert headers.get("etag") == expected_etag
    received_body = _response_body(messages)
    if expected_encoding == "gzip":
        assert gzip.decompress(received_body) == body
        assert "accept-encoding" in {field.strip().lower() for field in headers["vary"].split(",")}
    else:
        assert received_body == body


@pytest.mark.asyncio
async def test_gzip_preserves_existing_content_encoding():
    body = b"already encoded"

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        response = Response(body, headers={"Content-Encoding": "br", "ETag": '"revision"'})
        await response(scope, receive, send)

    middleware = DebugAwareGZipMiddleware(app, minimum_size=1)
    messages = await _collect_messages(middleware, _http_scope("gzip"))

    headers = _response_headers(messages)
    assert headers["content-encoding"] == "br"
    assert headers["etag"] == '"revision"'
    assert _response_body(messages) == body


@pytest.mark.asyncio
async def test_gzip_streaming_response():
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.debug", "info": {"value": "stream"}})
        await send({"type": "http.response.start", "status": 200, "headers": [(b"etag", b'"revision"')]})
        await send({"type": "http.response.body", "body": b"hello ", "more_body": True})
        await send({"type": "http.response.body", "body": b"world", "more_body": False})

    middleware = DebugAwareGZipMiddleware(app, minimum_size=100)
    messages = await _collect_messages(middleware, _http_scope("gzip"))

    assert messages[0] == {"type": "http.response.debug", "info": {"value": "stream"}}
    assert sum(message["type"] == "http.response.debug" for message in messages) == 1
    headers = _response_headers(messages)
    assert headers["content-encoding"] == "gzip"
    assert headers["etag"] == 'W/"revision"'
    assert "content-length" not in headers
    assert gzip.decompress(_response_body(messages)) == b"hello world"


@pytest.mark.asyncio
async def test_gzip_does_not_modify_original_scope():
    scope = _http_scope("gzip")
    original_scope = scope.copy()
    middleware = DebugAwareGZipMiddleware(_response_app(b"hello"), minimum_size=100)

    messages = await _collect_messages(middleware, scope)

    assert scope == original_scope
    assert _response_body(messages) == b"hello"


@pytest.mark.asyncio
async def test_gzip_concurrent_requests_keep_debug_and_responses_separate():
    both_started = asyncio.Event()
    started = 0

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        nonlocal started
        started += 1
        if started == 2:
            both_started.set()
        await both_started.wait()

        path = scope["path"]
        await send({"type": "http.response.debug", "info": {"path": path}})
        await asyncio.sleep(0)
        await Response((path * 100).encode(), headers={"ETag": f'"{path}"'})(scope, receive, send)

    middleware = DebugAwareGZipMiddleware(app, minimum_size=100)
    first, second = await asyncio.wait_for(
        asyncio.gather(
            _collect_messages(middleware, _http_scope("gzip", "/first")),
            _collect_messages(middleware, _http_scope("identity", "/second")),
        ),
        timeout=5,
    )

    assert [message for message in first if message["type"] == "http.response.debug"] == [
        {"type": "http.response.debug", "info": {"path": "/first"}}
    ]
    assert [message for message in second if message["type"] == "http.response.debug"] == [
        {"type": "http.response.debug", "info": {"path": "/second"}}
    ]
    first_headers = _response_headers(first)
    second_headers = _response_headers(second)
    assert first_headers["content-encoding"] == "gzip"
    assert first_headers["etag"] == 'W/"/first"'
    assert gzip.decompress(_response_body(first)) == b"/first" * 100
    assert "content-encoding" not in second_headers
    assert second_headers["etag"] == '"/second"'
    assert _response_body(second) == b"/second" * 100


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scope_type,scope_message",
    [
        ("lifespan", "lifespan.startup.complete"),
        ("websocket", "websocket.accept"),
    ],
)
async def test_gzip_passes_through_non_http_events(scope_type: str, scope_message: str):
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        assert scope["type"] == scope_type
        await send({"type": scope_message})

    middleware = DebugAwareGZipMiddleware(app)
    assert await _collect_messages(middleware, {"type": scope_type}) == [{"type": scope_message}]
