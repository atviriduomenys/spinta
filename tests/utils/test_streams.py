import pytest

from spinta.utils.streams import splitlines


async def _chunks(stream):
    for chunk in stream.encode().split(b'|'):
        yield chunk


async def _splitlines(stream):
    return [line async for line in splitlines(_chunks(stream))]


@pytest.mark.asyncio
async def test_slitlines():
    assert await _splitlines('a|b|\n|c') == ['ab', 'c']
    assert await _splitlines('a|b|\n') == ['ab']
    assert await _splitlines('a|\n|\n|b') == ['a', '', 'b']
