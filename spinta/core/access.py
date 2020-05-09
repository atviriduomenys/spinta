from spinta import exceptions
from spinta.core.enums import Access


def load_access_param(component, access, parents=()):
    if not access:
        for parent in parents:
            if parent.access:
                access = parent.access
                break
        else:
            access = Access.protected
    elif access == 'private' or access == Access.private:
        access = Access.private
    elif access == 'protected' or access == Access.protected:
        access = Access.protected
    elif access == 'public' or access == Access.public:
        access = Access.public
    elif access == 'open' or access == Access.open:
        access = Access.open
    else:
        raise exceptions.InvalidValue(component, param='access', given=access)

    # If child has higher access than parent, increase parent access.
    for parent in parents:
        if not parent.access or access > parent.access:
            parent.access = access

    return access
