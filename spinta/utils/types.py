from uuid import UUID


def is_str_uuid(value: str) -> bool:
    try:
        uuid_obj = UUID(value, version=4)
    except Exception:
        return False
    return str(uuid_obj) == value
