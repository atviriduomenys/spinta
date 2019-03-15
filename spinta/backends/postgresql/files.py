import io
import typing

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import BIGINT, ARRAY


DEFAULT_BLOCK_SIZE = 1 << 20  # 1MB


class Tables(typing.NamedTuple):
    files: sa.Table
    blocks: sa.Table = None


def get_database_file_tables(name, metadata):
    return Tables(
        files=sa.Table(
            f'{name}F', metadata,
            sa.Column('id', BIGINT, primary_key=True),
            sa.Column('file_size', BIGINT),
            sa.Column('block_size', sa.Integer),
            sa.Column('blocks', ARRAY(BIGINT)),
        ),
        blocks=sa.Table(
            f'{name}B', metadata,
            sa.Column('id', BIGINT, primary_key=True),
            sa.Column('block', sa.LargeBinary),
        ),
    )


class DatabaseFile(io.RawIOBase):

    def __init__(self, connection, tables, id=None, mode='r', block_size=DEFAULT_BLOCK_SIZE):
        self.id = id
        self.size = 0
        self.mode = mode
        self._connection = connection
        self._tables = tables
        self._pos = 0

        self._block_size = block_size
        self._block = None  # Currenly loaded block.
        self._block_id = None  # Currently loaded block id.
        self._blocks = []  # List of ids to Tables.blocks.c.id.
        self._dirty = False

        if self.mode not in ('r', 'w', 'a'):
            raise Exception(f"Unsupported file mode {self.mode!r}.")

        if self.mode == 'r' and self.id is None:
            raise Exception("File id is required in read mode.")

        if self.id is None:
            result = self._connection.execute(
                self._tables.files.insert().values(
                    file_size=self.size,
                    block_size=self._block_size,
                    blocks=self._blocks,
                )
            )
            self.id = result.inserted_primary_key[0]
        else:
            qry = (
                sa.select([self._tables.files]).
                where(self._tables.files.c.id == self.id)
            )
            row = self._connection.execute(qry).first()

            if row is None:
                raise Exception(f"Given file id {self.id} does not exist in database.")

            self.size = row[self._tables.files.c.file_size]
            self._block_size = row[self._tables.files.c.block_size]
            self._block = None
            self._blocks = row[self._tables.files.c.blocks]

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
        offset = self._pos // self._block_size

        if whence == 0:
            self._pos = pos
        elif whence == 1:
            self._pos += pos
        elif whence == 2:
            self.size += pos
        else:
            raise Exception(f"Unsupported whence ({whence}).")

        self._check_position(self._pos)

        if offset != self._pos // self._block_size:
            self._block = None

        return self._pos

    def flush(self):
        self._checkClosed()

    def close(self):
        super().close()
        self._update_file_metadata()

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
        pos = self._pos % self._block_size
        reminder = self._block_size - pos
        if reminder >= size:
            self._pos += size
            return self._block[pos:pos + size]

        # Read full blocks.
        buffer = self._block[pos:]
        self._pos += reminder
        blocks, reminder = divmod(size - reminder, self._block_size)

        for i in range(blocks):
            self._read_block()
            buffer += self._block
            self._pos += self._block_size

        # Read reminder block.
        if reminder:
            self._read_block()
            buffer += self._block[:reminder]
            self._pos += reminder

        return buffer

    def write(self, b):
        self._read_block()

        size = len(b)
        pos = self._pos % self._block_size
        reminder = self._block_size - pos

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
        blocks, reminder = divmod(size - reminder, self._block_size)

        for i in range(blocks):
            self._block = b[pos:pos + self._block_size]
            self._write_block()
            self._pos += self._block_size
            pos += self._block_size

        if reminder:
            self._read_block()
            self._block = b[pos:] + self._block[len(b[pos:]):]
            self._write_block()
            self._pos += reminder

        if self._pos > self.size:
            self.size = self._pos

        return size

    def _read_block(self):
        block_ix = self._pos // self._block_size
        block_count = len(self._blocks)

        if block_ix >= block_count:
            self._block = b''
            self._block_id = None
            return

        if self._block is not None and self._blocks[block_ix] == self._block_id:
            # This block is already loaded.
            return

        # Load block from database.
        query = (
            sa.select([self._tables.blocks.c.block]).
            where(self._tables.blocks.c.id == self._blocks[block_ix])
        )

        self._block = self._connection.execute(query).scalar()
        self._block_id = self._blocks[block_ix]

        if self._block is None:
            raise Exception(f"Tried to access block {self._block_id} on {self._tables.blocks.name!r}, but it does not exist.")

        block_size = len(self._block)
        last_block = block_ix == block_count - 1
        if not last_block and block_size != self._block_size:
            raise Exception(
                f"Expected block size {self._block_size}, received {block_size}. "
                f"Block id: {self._block_id} on {self._tables.blocks.name!r}."
            )

    def _write_block(self):
        block_ix = self._pos // self._block_size
        block_count = len(self._blocks)

        if block_ix >= block_count + 1:
            raise Exception(f"Tried to write block {block_ix}, but number of available blocks is {block_count}.")

        if len(self._block) > self._block_size:
            block_size = len(self._block)
            raise Exception(
                f"Expected block size {self._block_size}, received {block_size}. "
                f"Block id: {self._block_id} on {self._tables.blocks.name!r}."
            )

        query = self._tables.blocks.insert().values(block=self._block)
        result = self._connection.execute(query)
        block_id = result.inserted_primary_key[0]

        if block_ix >= block_count:
            self._blocks.append(block_id)
        else:
            self._blocks[block_ix] = block_id

        self._dirty = True
        self._block_id = block_id

    def _check_position(self, pos):
        if pos < 0:
            raise Exception(f"Attempt to seek to a negative position ({self._pos}).")

        if pos > self.size:
            raise Exception(f"Attempt to seek outside of file boundaries, attempted position ({self._pos}), file size ({self.size}).")

    def _update_file_metadata(self):
        if self._dirty:
            self._connection.execute(
                self._tables.files.update().values(
                    file_size=self.size,
                    blocks=self._blocks,
                ).
                where(self._tables.files.c.id == self.id)
            )

    def __enter__(self):
        self._checkClosed()
        return self

    def __exit__(self, *args):
        self.close()
