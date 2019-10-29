import pytest

from spinta.utils.aiotools import agroupby, aiter, alist, aslice


@pytest.mark.asyncio
async def test_agroupby():
    it = aiter([1, 1, 1, 2, 2, 3])
    assert [(k, await alist(g)) async for k, g in agroupby(it)] == [
        (1, [1, 1, 1]),
        (2, [2, 2]),
        (3, [3]),
    ]


@pytest.mark.asyncio
async def test_aslice():
    it = aiter([1, 2, 3, 4, 5])
    assert await alist(aslice(it, 2)) == [1, 2]

    it = aiter([1, 2, 3, 4, 5])
    assert await alist(aslice(it, 0, 100, 2)) == [1, 3, 5]

    it = aiter([1, 2, 3, 4, 5])
    assert await alist(aslice(it, 2, 100,)) == [3, 4, 5]
