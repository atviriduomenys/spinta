from spinta.utils.types import is_str_uuid


def test_uuid_correct():
    assert is_str_uuid("9621c46a-3c1e-4e98-9d2f-7a06889a8762")


def test_uuid_incorrect():
    assert not is_str_uuid("9621c46a-3c1e-4e98-9d2f-7a06889a876")
    assert not is_str_uuid("")
    assert not is_str_uuid("96")
    assert not is_str_uuid("test")
    assert not is_str_uuid("9621c46a-3c1e-4e98-9d2f_7a06889a8762")
    assert not is_str_uuid("test sentence")
