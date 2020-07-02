import enum
import contextlib


class BackendFeatures(enum.Enum):
    # Files are stored in blocks and file metadata must include _bsize and
    # _blocks properties.
    FILE_BLOCKS = 'FILE_BLOCKS'


class Backend:
    metadata = {
        'name': 'backend',
    }

    name: str
    features = set()

    def __repr__(self):
        return (
            f'<{self.__class__.__module__}.{self.__class__.__name__}'
            f'(name={self.name!r}) at 0x{id(self):02x}>'
        )

    @contextlib.contextmanager
    def transaction(self):
        raise NotImplementedError
