import enum
import contextlib
from typing import Any
from typing import Dict
from typing import Optional
from typing import Set


class BackendOrigin(enum.Enum):
    """Origin where backend was defined.

    Backend can be defined in multiple places, for example backend can be
    defined in a configuration file or inline in manifest.
    """

    config = 'config'
    manifest = 'manifest'
    resource = 'resource'


class BackendFeatures(enum.Enum):
    # Files are stored in blocks and file metadata must include _bsize and
    # _blocks properties.
    FILE_BLOCKS = 'FILE_BLOCKS'

    # Backend supports write operations.
    WRITE = 'WRITE'


class Backend:
    metadata = {
        'name': 'backend',
    }

    type: str
    name: str
    origin: BackendOrigin
    features: Set[BackendFeatures] = set()

    # Original configuration values given in manifest, this is used to restore
    # manifest back to its original form.
    config: Dict[str, Any]

    def __repr__(self):
        return (
            f'<{self.__class__.__module__}.{self.__class__.__name__}'
            f'(name={self.name!r}) at 0x{id(self):02x}>'
        )

    @contextlib.contextmanager
    def transaction(self):
        raise NotImplementedError

    def bootstrapped(self):
        raise NotImplementedError


SelectTree = Optional[Dict[str, 'SelectTree']]
