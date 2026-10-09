from collections.abc import Iterator

import pytest

from spinta.utils.response import async_response_iterator


@pytest.mark.asyncio
@pytest.mark.parametrize("values", [[], [1, 2]])
async def test_response_iterator_does_not_close_exhausted_source(values: list[int]) -> None:
    class Stream(Iterator[int]):
        def __init__(self) -> None:
            self.items = iter(values)

        def __next__(self) -> int:
            return next(self.items)

        def close(self) -> None:
            raise AssertionError("An exhausted source should not be closed again")

    assert [item async for item in async_response_iterator(Stream())] == values


@pytest.mark.asyncio
@pytest.mark.parametrize("use_iterable", [False, True], ids=["iterator", "iterable"])
async def test_response_iterator_closes_actual_iterator_when_stopped_early(use_iterable: bool) -> None:
    closed = False

    def data() -> Iterator[int]:
        nonlocal closed
        try:
            yield 1
            yield 2
        finally:
            closed = True

    # Keep a reference so garbage collection cannot perform the cleanup.
    source = data()

    class IterableStream:
        def __iter__(self) -> Iterator[int]:
            return source

    try:
        response = async_response_iterator(IterableStream() if use_iterable else source)
        assert await response.__anext__() == 1
        assert not closed
        await response.aclose()
        assert closed
    finally:
        source.close()
