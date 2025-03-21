from __future__ import annotations

import enum

from spinta.exceptions import InvalidLevel
from spinta.utils.enums import enum_by_name, enum_by_value


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


class Status(enum.Enum):
    """Status of the data record"""

    # Status isn't given
    absent = None

    # The data of this element is being updated. This field can be changed without any warning
    develop = 1

    # Data in this element is completed. The data of this row can be changed with a warning issued
    # before time X, where X is not less than 12 months
    completed = 2

    # Data isn't being updated, but the element isn't planned for removing
    discont = 3

    # For elements which are planned for removal.
    deprecated = 4

    # Withdrawn and isn't used anymore. Can be set after the field has been in the `deprecated` status for X time.
    withdrawn = 5


class Visibility(enum.IntEnum):
    """Visibility of the data, which means access to this metadata."""

    # No visibility is given
    absent = 0

    # Metadata isn't published.
    private = 1

    # Application on the level of the informational system.
    protected = 2

    # Application on the level of the country.
    package = 3

    # Application on the level of the EU.
    public = 4


def load_level(
    component: 'spinta.components.Component',
    given_level: Level | int | str
) -> None:
    if given_level:
        if isinstance(given_level, Level):
            level = given_level
        else:
            if isinstance(given_level, str) and given_level.isdigit():
                given_level = int(given_level)
            if not isinstance(given_level, int):
                raise InvalidLevel(component, level=given_level)
            level = enum_by_value(component, 'level', Level, given_level)
    else:
        level = None
    component.level = level


#  enum loaders

def load_status(
    component: 'spinta.components.Component',
    given_status: Status | str
) -> None:
    if isinstance(given_status, Status):
        component.status = given_status
    else:
        component.status = enum_by_name(component, 'status', Status, given_status or component.status)
    component.given.status = given_status


def load_visibility(
    component: 'spinta.components.Component',
    given_visibility: Visibility | str
) -> None:
    if isinstance(given_visibility, Visibility):
        component.visibility = given_visibility
    else:
        component.visibility = enum_by_name(component, 'visibility', Visibility, given_visibility or component.visibility)
    component.given.visibility = given_visibility
