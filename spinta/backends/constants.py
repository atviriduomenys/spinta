import enum


class TableType(enum.Enum):
    MAIN = ''
    LIST = '/:list'
    CHANGELOG = '/:changelog'
    CACHE = '/:cache'
    FILE = '/:file'
