from typing import AsyncIterable
from typing import AsyncIterator

import io
import codecs


async def splitlines(
    stream: AsyncIterable[bytes],
    encoding: str = 'utf-8',
    errors: str = 'strict',
) -> AsyncIterator[str]:
    """Read chunks of bytes from stream and yield lines."""
    decoder = codecs.getincrementaldecoder(encoding)(errors)
    decoder = io.IncrementalNewlineDecoder(decoder, translate=True, errors=errors)
    buffer = ''
    async for chunk in stream:
        buffer = buffer + decoder.decode(chunk)
        if '\n' in buffer:
            lines = buffer.split('\n')
            for line in lines[:-1]:
                yield line
            buffer = lines[-1]
    buffer = buffer + decoder.decode(b'', final=True)
    if buffer:
        for line in buffer.split('\n'):
            yield line
