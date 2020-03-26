import enum


class Action(enum.Enum):
    INSERT = 'insert'
    UPSERT = 'upsert'
    UPDATE = 'update'
    PATCH = 'patch'
    DELETE = 'delete'

    WIPE = 'wipe'

    GETONE = 'getone'
    GETALL = 'getall'
    SEARCH = 'search'

    CHANGES = 'changes'

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_

    @classmethod
    def by_value(cls, value):
        return cls._value2member_map_[value]

    @classmethod
    def values(cls):
        return list(cls._value2member_map_.keys())
