import enum


class Access(enum.Enum):
    # Property is not exposed to end used in any way. Private properties can
    # still be used internally for example when resolving references.
    private = 0

    # Property is exposed only to authorized user, who has access to model.
    protected = 1

    # Property is public and can be accessed by anyone without any
    # authorization.
    public = 2
