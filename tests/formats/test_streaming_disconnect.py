from __future__ import annotations

import logging
import socket
import struct
import sys
import threading
from collections.abc import Generator, Iterable, Iterator
from http.client import HTTPConnection
from typing import TYPE_CHECKING, Generic, Literal, TypeVar

import pytest
from starlette.requests import Request
from starlette.responses import StreamingResponse

from spinta import commands
from spinta.commands.read import prepare_data_for_response
from spinta.components import Context, Model, UrlParams
from spinta.core.enums import Action
from spinta.formats.ascii.components import Ascii
from spinta.formats.components import Format
from spinta.formats.csv.components import Csv
from spinta.formats.json.components import Json
from spinta.formats.jsonlines.components import JsonLines
from spinta.formats.rdf.components import Rdf
from spinta.formats.xlsx.components import Xlsx
from spinta.utils.response import async_response_iterator

if TYPE_CHECKING:
    from tests.formats.conftest import LiveResponse


T = TypeVar("T")


class PausedStream(Generic[T]):
    """Count consumed items and pause so the client can disconnect mid-response."""

    def __init__(self, items: Iterable[T], pause_after: int = 8) -> None:
        self.items = items
        self.pause_after = pause_after
        self.read_count = 0
        self.paused = threading.Event()
        self.resume = threading.Event()

    def __iter__(self) -> Iterator[T]:
        for index, item in enumerate(self.items):
            if index == self.pause_after:
                self.paused.set()
                assert self.resume.wait(5), "Client did not release the remaining items"
            self.read_count += 1
            yield item


def _disconnect_after_pause(
    client: HTTPConnection,
    response_finished: threading.Event,
    stream: PausedStream,
    *,
    read_body: bool = True,
) -> None:
    try:
        client.request("GET", "/")
        response = client.getresponse()
        assert response.status == 200
        if read_body:
            assert response.read(1)
        assert stream.paused.wait(5), "Stream did not reach the pause"

        # Close with TCP reset, without waiting for the rest of the body.
        linger_format = "hh" if sys.platform == "win32" else "ii"
        client.sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack(linger_format, 1, 0))
        client.close()
        stream.resume.set()
        assert response_finished.wait(5), "Response did not finish after disconnect"
    finally:
        stream.resume.set()


def _format_params(fmt: Format) -> UrlParams:
    params = UrlParams()
    params.fmt = fmt
    params.select_props = {"_type": None, "value": None}
    params.formatparams = {"colwidth": 42}
    return params


@pytest.mark.parametrize(
    "fmt, target",
    [
        (Json(), "model"),
        (JsonLines(), "model"),
        (Csv(), "model"),
        (Ascii(), "model"),
        (Rdf(), "model"),
        (Rdf(), "namespace"),
    ],
    ids=["json", "jsonlines", "csv", "ascii", "rdf", "rdf-namespace"],
)
def test_stream_stops_after_client_disconnect(
    fmt: Format,
    target: Literal["model", "namespace"],
    streaming_model: tuple[Context, Model],
    live_response: LiveResponse,
    caplog: pytest.LogCaptureFixture,
) -> None:
    context, model = streaming_model
    params = _format_params(fmt)
    # ASCII reads about 200 rows to calculate column widths before streaming.
    pause_after = 256 if isinstance(fmt, Ascii) else 8

    data = PausedStream(
        ({"_type": model.model_type(), "value": value} for value in range(1000)),
        pause_after=pause_after,
    )

    def render_response(request: Request) -> StreamingResponse:
        rows = prepare_data_for_response(context, model, Action.GETALL, params, data)
        return commands.render(
            context,
            request,
            model.ns if target == "namespace" else model,
            fmt,
            action=Action.GETALL,
            params=params,
            data=rows,
        )

    with caplog.at_level(logging.WARNING):
        with live_response(render_response) as (client, response_finished):
            _disconnect_after_pause(client, response_finished, data)

    assert 0 < data.read_count < 1000
    assert caplog.messages == []


def test_response_iterator_closes_generator_after_client_disconnect(
    live_response: LiveResponse,
    caplog: pytest.LogCaptureFixture,
) -> None:
    chunks = PausedStream(f"{value}\n".encode() for value in range(1000))
    source_closed = threading.Event()

    def data() -> Generator[bytes, None, None]:
        try:
            yield from chunks
        finally:
            source_closed.set()

    # Keep a reference so garbage collection cannot close the source for us.
    source = data()

    def render_response(request: Request) -> StreamingResponse:
        return StreamingResponse(async_response_iterator(source))

    try:
        with caplog.at_level(logging.WARNING):
            with live_response(render_response) as (client, response_finished):
                _disconnect_after_pause(client, response_finished, chunks)
                assert 0 < chunks.read_count < 1000
                assert source_closed.is_set(), "Response finished without closing the source generator"

        assert caplog.messages == []
    finally:
        # Clean up even when the regression assertion fails.
        source.close()


def test_xlsx_stops_after_client_disconnect(
    streaming_model: tuple[Context, Model],
    live_response: LiveResponse,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # xlsx builds entire workbook before sending, so it consumes all rows, for that reason we cannot put it under generic test

    context, model = streaming_model
    fmt = Xlsx()
    params = _format_params(fmt)
    data = PausedStream({"_type": model.model_type(), "value": value} for value in range(1000))

    def render_response(request: Request) -> StreamingResponse:
        rows = prepare_data_for_response(context, model, Action.GETALL, params, data)
        return commands.render(context, request, model, fmt, action=Action.GETALL, params=params, data=rows)

    with caplog.at_level(logging.WARNING):
        with live_response(render_response) as (client, response_finished):
            # XLSX builds the whole workbook before sending any body bytes.
            _disconnect_after_pause(client, response_finished, data, read_body=False)

    # Cancellation cannot interrupt synchronous workbook construction.
    assert data.read_count == 1000
    assert caplog.messages == []


@pytest.mark.parametrize("use_aiter", [False, True], ids=["sync", "aiter"])
def test_aiter_does_not_reproduce_socket_warnings(
    use_aiter: bool,
    live_response: LiveResponse,
    caplog: pytest.LogCaptureFixture,
) -> None:
    data = PausedStream(f"{value}\n".encode() for value in range(1000))

    def render_response(request: Request) -> StreamingResponse:
        stream = data
        if use_aiter:
            stream = async_response_iterator(stream)
        return StreamingResponse(stream)

    with caplog.at_level(logging.WARNING):
        with live_response(render_response) as (client, response_finished):
            _disconnect_after_pause(client, response_finished, data)

    assert 0 < data.read_count < 1000
    assert caplog.messages == []
