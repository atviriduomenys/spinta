import enum


class Access(enum.Enum):
    # Property is not exposed to end used in any way. Private properties can
    # still be used internally for example when resolving references.
    private = 0

    # Property is exposed only to authorized user, who has access to model.
    # Authorization token is given manually to each user.
    protected = 1

    # Property can be accessed by anyone, but only after accepting terms and
    # conditions, that means, authorization is still needed and data can only be
    # used as specified in provided terms and conditions. Authorization token
    # can be obtained automatically.
    public = 2

    # Open data, anyone can access this data, no authorization is required.
    open = 3
