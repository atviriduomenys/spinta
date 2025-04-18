import enum
from typing import Union, Any

from spinta import exceptions
from spinta.utils.errors import report_error


class Access(enum.IntEnum):
    # Private properties can be accesses only if an explicit property scope is
    # given. I client does not have required scope, then private properties
    # can't bet selected, but can be used in query conditions, in sorting.
    private = 0

    # Property is exposed only to authorized user, who has access to model.
    # Authorization token is given manually to each user.
    protected = 1

    # Property can be accessed by anyone, but only after accepting terms and
    # conditions, that means, authorization is still needed and data can only be
    # used as specified in provided terms and conditions. Authorization token
    # can be obtained via WebUI.
    public = 2

    # Open data, anyone can access this data, no authorization is required.
    open = 3


class Level(enum.IntEnum):
    # Data do not exist.
    absent = 0

    # Data exists in any form, for example txt, pdf, etc...
    available = 1

    # Data is structured, for example xls, xml, etc...
    structured = 2

    # Data provided using an open format, csv, tsv, sql, etc...
    open = 3

    # Individual data objects have unique identifiers.
    identifiable = 4

    # Data is linked with a known vocabulary.
    linked = 5


class Action(enum.Enum):
    INSERT = 'insert'
    UPSERT = 'upsert'
    UPDATE = 'update'
    PATCH = 'patch'
    DELETE = 'delete'

    WIPE = 'wipe'

    MOVE = 'move'

    GETONE = 'getone'
    GETALL = 'getall'
    SEARCH = 'search'

    CHANGES = 'changes'

    CHECK = 'check'
    INSPECT = 'inspect'
    SCHEMA = 'schema'

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_

    @classmethod
    def by_value(cls, value):
        return cls._value2member_map_[value]

    @classmethod
    def values(cls):
        return list(cls._value2member_map_.keys())


class Mode(enum.Enum):
    # Internal mode always use internal backend set on manifest, namespace or
    # model.
    internal = 'internal'

    # External model always sue external backend set on dataset or model's
    # source entity.
    external = 'external'


def action_from_op(
    scope: Any,
    payload: dict,
    stop_on_error: bool = True,
) -> Union[Action, exceptions.UserError]:
    action = payload.get('_op')
    if not Action.has_value(action):
        error = exceptions.UnknownAction(
            scope,
            action=action,
            supported_actions=Action.values(),
        )
        report_error(error, stop_on_error=stop_on_error)
        return error
    return Action.by_value(action)
