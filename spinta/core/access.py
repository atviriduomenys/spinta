from spinta import exceptions
from spinta.core.enums import Access


def load_access_param(component, access):
    if access == 'private' or access == Access.private:
        return Access.private
    elif not access or access == 'protected' or access == Access.protected:
        return Access.protected
    elif access == 'public' or access == Access.public:
        return Access.public
    elif access == 'open' or access == Access.open:
        return Access.open
    else:
        raise exceptions.InvalidValue(component, param='access', given=access)
