from __future__ import annotations

import logging
import socket
import struct
import sys
import threading
from collections.abc import Iterator
from http.client import HTTPConnection
from typing import TYPE_CHECKING, Literal

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
from spinta.utils.aiotools import aiter

if TYPE_CHECKING:
    from tests.formats.conftest import LiveResponse


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
    rows_read = []
    waiting_for_rows = threading.Event()
    continue_reading = threading.Event()
    # ASCII reads about 200 rows to calculate column widths before streaming.
    pause_after = 256 if isinstance(fmt, Ascii) else 8

    def data() -> Iterator[dict[str, str | int]]:
        for value in range(1000):
            # Pause after the first few rows so the client can disconnect mid-response.
            if value == pause_after:
                waiting_for_rows.set()
                assert continue_reading.wait(5), "Client did not release the remaining rows"
            rows_read.append(value)
            yield {"_type": model.model_type(), "value": value}

    def render_response(request: Request) -> StreamingResponse:
        rows = prepare_data_for_response(context, model, Action.GETALL, params, data())
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
            try:
                client.request("GET", "/")
                response = client.getresponse()
                assert response.status == 200
                assert response.read(1)
                assert waiting_for_rows.wait(5), "Stream did not reach the pause"

                _disconnect(client)
                continue_reading.set()
                assert response_finished.wait(5), "Response did not stop after disconnect"
            finally:
                continue_reading.set()

    assert 0 < len(rows_read) < 1000
    assert caplog.messages == []


def test_xlsx_stops_after_client_disconnect(
    streaming_model: tuple[Context, Model],
    live_response: LiveResponse,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # xlsx builds entire workbook before sending, so it consumes all rows, for that reason we cannot put it under generic test

    context, model = streaming_model
    fmt = Xlsx()
    params = _format_params(fmt)
    rows_read = []
    waiting_for_rows = threading.Event()
    continue_reading = threading.Event()

    def data() -> Iterator[dict[str, str | int]]:
        for value in range(1000):
            if value == 8:
                waiting_for_rows.set()
                assert continue_reading.wait(5), "Client did not release the remaining rows"
            rows_read.append(value)
            yield {"_type": model.model_type(), "value": value}

    def render_response(request: Request) -> StreamingResponse:
        rows = prepare_data_for_response(context, model, Action.GETALL, params, data())
        return commands.render(context, request, model, fmt, action=Action.GETALL, params=params, data=rows)

    with caplog.at_level(logging.WARNING):
        with live_response(render_response) as (client, response_finished):
            try:
                client.request("GET", "/")
                response = client.getresponse()
                assert response.status == 200
                # XLSX builds the whole workbook before sending any body bytes.
                # Disconnect after the headers, while the workbook is being built.
                assert waiting_for_rows.wait(5), "Workbook did not reach the pause"

                _disconnect(client)
                continue_reading.set()
                assert response_finished.wait(5), "Response did not stop after disconnect"
            finally:
                continue_reading.set()

    # Cancellation cannot interrupt a next() already running in a worker thread.
    assert len(rows_read) == 1000
    assert caplog.messages == []


@pytest.mark.parametrize("use_aiter", [True, True], ids=["sync", "aiter"])
def test_aiter_does_not_reproduce_socket_warnings(
    use_aiter: bool,
    live_response: LiveResponse,
    caplog: pytest.LogCaptureFixture,
) -> None:
    chunks_read = []
    waiting_for_chunks = threading.Event()
    continue_reading = threading.Event()

    def data() -> Iterator[bytes]:
        for value in range(1000):
            if value == 8:
                waiting_for_chunks.set()
                assert continue_reading.wait(5), "Client did not release the remaining chunks"
            chunks_read.append(value)
            yield f"{value}\n".encode()

    def render_response(request: Request) -> StreamingResponse:
        stream = data()
        if use_aiter:
            stream = aiter(stream)
        return StreamingResponse(stream)

    with caplog.at_level(logging.WARNING):
        with live_response(render_response) as (client, response_finished):
            try:
                client.request("GET", "/")
                response = client.getresponse()
                assert response.status == 200
                assert response.read(1)
                assert waiting_for_chunks.wait(5), "Stream did not reach the pause"

                _disconnect(client)
                continue_reading.set()
                assert response_finished.wait(5), "Response did not finish after disconnect"
            finally:
                continue_reading.set()

    assert 0 < len(chunks_read) < 1000
    assert caplog.messages == []


def _format_params(fmt: Format) -> UrlParams:
    params = UrlParams()
    params.fmt = fmt
    params.select_props = {"_type": None, "value": None}
    params.formatparams = {"colwidth": 42}
    return params


def _disconnect(client: HTTPConnection) -> None:
    """Close immediately with TCP reset, without waiting for the rest of the body."""
    # The OS-specific structure contains: linger enabled = 1, timeout = 0.
    linger_format = "hh" if sys.platform == "win32" else "ii"
    client.sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack(linger_format, 1, 0))
    client.close()
