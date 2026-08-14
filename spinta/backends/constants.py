import enum

# Context name holding how long, in seconds, a backend availability check
# (`commands.wait`) may wait for the driver to connect. Waiting for backends to
# come up does not set it, so drivers keep their own defaults and a host that is
# not answering yet gets as long as the caller is willing to wait. Callers that
# need an answer in bounded time, such as the `/health` probe, set it.
WAIT_CONNECT_TIMEOUT = "wait.connect_timeout"


class TableType(enum.Enum):
    MAIN = ""
    LIST = "/:list"
    CHANGELOG = "/:changelog"
    CACHE = "/:cache"
    FILE = "/:file"
    REDIRECT = "/:redirect"


class BackendOrigin(enum.Enum):
    """Origin where backend was defined.

    Backend can be defined in multiple places, for example backend can be
    defined in a configuration file or inline in manifest.
    """

    config = "config"
    manifest = "manifest"
    resource = "resource"


class BackendFeatures(enum.Enum):
    # Files are stored in blocks and file metadata must include _bsize and
    # _blocks properties.
    FILE_BLOCKS = "FILE_BLOCKS"

    # Backend supports write operations.
    WRITE = "WRITE"

    # Backend supports pagination when reading data
    PAGINATION = "PAGINATION"

    # Backend supports
    EXPAND = "EXPAND"

    # Backend supports sharding
    DISTRIBUTE = "DISTRIBUTE"


class DistributionType(enum.Enum):
    SCHEMA = "schema"
    TABLE = "table"
    COPY = "copy"
    UNDISTRIBUTED = "undistributed"
