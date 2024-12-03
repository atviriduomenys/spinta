from spinta.components import PageInfo, Page
from multipledispatch import dispatch


@dispatch(PageInfo)
def page_contains_unsupported_keys(page: PageInfo):
    allowed_types = get_allowed_page_property_types()
    for prop in page.keys.values():
        if not isinstance(prop.dtype, allowed_types):
            return True
    return False


@dispatch(Page)
def page_contains_unsupported_keys(page: Page):
    allowed_types = get_allowed_page_property_types()
    for page_by in page.by.values():
        if not isinstance(page_by.prop.dtype, allowed_types):
            return True
    return False


def get_allowed_page_property_types():
    from spinta.types.datatype import Integer, Number, String, Date, Time, DateTime, PrimaryKey, UUID
    return Integer, Number, String, Date, DateTime, Time, PrimaryKey, UUID
