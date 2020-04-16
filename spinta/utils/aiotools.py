from typing import TypeVar, AsyncIterator, List, Callable, Optional, Tuple, Awaitable, Iterable

T = TypeVar('T')
R = TypeVar('R')


class agroupby:
    # Adapted to async from Python docs:
    # https://docs.python.org/3/library/itertools.html#itertools.groupby

    keyfunc: Callable[[T], R]
    it: AsyncIterator[T]
    tgtkey: R
    currkey: R
    currvalue: T

    def __init__(
        self,
        iterable: AsyncIterator[T],
        key: Optional[Callable[[T], R]] = None,
    ) -> None:
        if key is None:
            key = lambda x: x  # noqa
        self.keyfunc = key
        self.it = iterable
        self.tgtkey = self.currkey = self.currvalue = object()

    def __aiter__(self) -> AsyncIterator[Tuple[R, AsyncIterator[T]]]:
        return self

    async def __anext__(self) -> Awaitable[Tuple[R, AsyncIterator[T]]]:
        self.id = object()
        while self.currkey == self.tgtkey:
            self.currvalue = await self.it.__anext__()    # Exit on AsyncStopIteration
            self.currkey = self.keyfunc(self.currvalue)
        self.tgtkey = self.currkey
        return self.currkey, self._grouper(self.tgtkey, self.id)

    async def _grouper(self, tgtkey: R, id) -> AsyncIterator[T]:
        while self.id is id and self.currkey == tgtkey:
            yield self.currvalue
            try:
                self.currvalue = await self.it.__anext__()
            except StopAsyncIteration:
                return
            self.currkey = self.keyfunc(self.currvalue)


async def alist(it: AsyncIterator[T]) -> List[T]:
    return [x async for x in it]


async def aiter(it: Iterable[T]) -> AsyncIterator[T]:
    for x in it:
        yield x


async def aslice(it: AsyncIterator[T], *args) -> AsyncIterator[T]:
    i = 0
    r = range(*args)
    async for x in it:
        if i in r:
            yield x
        i += 1


async def adrain(it: AsyncIterator[T]) -> None:
    async for _ in it:
        pass
