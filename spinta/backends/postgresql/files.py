from typing import Optional, List

import io
import uuid

import sqlalchemy as sa


DEFAULT_BLOCK_SIZE = 8 << 20  # 8MB


class DatabaseFile(io.RawIOBase):

    def __init__(
        self,
        conn,
        table: sa.Table,
        size: int = 0,
        blocks: Optional[List[str]] = None,
        bsize: int = DEFAULT_BLOCK_SIZE,
        *,
        mode: str = 'r',
    ):
        self.size = size
        self.blocks = [] if blocks is None else blocks[:]
        self.bsize = bsize
        self.mode = mode
        self.dirty = False

        self._conn = conn
        self._table = table
        self._pos = 0
        self._block = None  # Currenly loaded block.
        self._block_id = None  # Currently loaded block id.

        if self.mode not in ('r', 'w', 'a'):
            raise Exception(f"Unsupported file mode {self.mode!r}.")

        if self.mode == 'a':
            self._pos = self.size

    def seekable(self):
        return True

    def readable(self):
        return self.mode == 'r'

    def writable(self):
        return self.mode in ('w', 'a')

    def tell(self):
        return self._pos

    def seek(self, pos, whence=0):
        offset = self._pos // self.bsize

        if whence == 0:
            self._pos = pos
        elif whence == 1:
            self._pos += pos
        elif whence == 2:
            self.size += pos
        else:
            raise Exception(f"Unsupported whence ({whence}).")

        self._check_position(self._pos)

        if offset != self._pos // self.bsize:
            self._block = None

        return self._pos

    def flush(self):
        self._checkClosed()

    def read(self, size=-1):

        if size is None or size < 0 or self._pos + size > self.size:
            size = self.size - self._pos

        # Read current block.
        #
        #   +--------|-------|-------+
        #   |012345670123456701234567|
        #   +--------|-------|-------+
        #      |   `--  size == 4
        #      `-- self._pos == 2
        #
        self._read_block()
        pos = self._pos % self.bsize
        reminder = self.bsize - pos
        if reminder >= size:
            self._pos += size
            return self._block[pos:pos + size]

        # Read full blocks.
        buffer = self._block[pos:]
        self._pos += reminder
        blocks, reminder = divmod(size - reminder, self.bsize)

        for i in range(blocks):
            self._read_block()
            buffer += self._block
            self._pos += self.bsize

        # Read reminder block.
        if reminder:
            self._read_block()
            buffer += self._block[:reminder]
            self._pos += reminder

        return buffer

    def write(self, b):
        self._read_block()

        size = len(b)
        pos = self._pos % self.bsize
        reminder = self.bsize - pos

        if reminder >= size:
            self._block = self._block[:pos] + b + self._block[pos + size:]
            self._write_block()
            self._pos += size
            if self._pos > self.size:
                self.size = self._pos
            return size

        self._block = self._block[:pos] + b[:reminder]
        self._write_block()
        self._pos += reminder
        pos = reminder
        blocks, reminder = divmod(size - reminder, self.bsize)

        for i in range(blocks):
            self._block = b[pos:pos + self.bsize]
            self._write_block()
            self._pos += self.bsize
            pos += self.bsize

        if reminder:
            self._read_block()
            self._block = b[pos:] + self._block[len(b[pos:]):]
            self._write_block()
            self._pos += reminder

        if self._pos > self.size:
            self.size = self._pos

        return size

    def _read_block(self):
        block_ix = self._pos // self.bsize
        block_count = len(self.blocks)

        if block_ix >= block_count:
            self._block = b''
            self._block_id = None
            return

        if self._block is not None and self.blocks[block_ix] == self._block_id:
            # This block is already loaded.
            return

        # Load block from database.
        query = (
            sa.select([self._table.c._block]).
            where(self._table.c._id == self.blocks[block_ix])
        )

        self._block = self._conn.execute(query).scalar()
        self._block_id = self.blocks[block_ix]

        if self._block is None:
            raise Exception(
                f"Tried to access block {self._block_id} on "
                f"{self._table.name!r}, but it does not exist."
            )

        block_size = len(self._block)
        last_block = block_ix == block_count - 1
        if not last_block and block_size != self.bsize:
            raise Exception(
                f"Expected block size {self.bsize}, received {block_size}. "
                f"Block id: {self._block_id} on {self._table.name!r}."
            )

    def _write_block(self):
        block_ix = self._pos // self.bsize
        block_count = len(self.blocks)

        if block_ix >= block_count + 1:
            raise Exception(
                f"Tried to write block {block_ix}, but number of available "
                f"blocks is {block_count}."
            )

        if len(self._block) > self.bsize:
            block_size = len(self._block)
            raise Exception(
                f"Expected block size {self.bsize}, received {block_size}. "
                f"Block id: {self._block_id} on {self._table.name!r}."
            )

        query = self._table.insert().values(
            _id=str(uuid.uuid4()),
            _block=self._block,
        )
        result = self._conn.execute(query)
        block_id = result.inserted_primary_key[0]

        if block_ix >= block_count:
            self.blocks.append(block_id)
        else:
            self.blocks[block_ix] = block_id

        self.dirty = True
        self._block_id = block_id

    def _check_position(self, pos):
        if pos < 0:
            raise Exception(
                f"Attempt to seek to a negative position ({self._pos})."
            )

        if pos > self.size:
            raise Exception(
                f"Attempt to seek outside of file boundaries, attempted "
                f"position ({self._pos}), file size ({self.size})."
            )

    def __enter__(self):
        self._checkClosed()
        return self

    def __exit__(self, *args):
        self.close()
