import enum


class TableType(enum.Enum):
    MAIN = ''
    LIST = '/:list'
    CHANGELOG = '/:changelog'
    CACHE = '/:cache'
    FILE = '/:file'


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

    # Backend supports pagination when reading data
    PAGINATION = 'PAGINATION'

    # Backend supports
    EXPAND = 'EXPAND'
