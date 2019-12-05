from spinta.utils.schema import NA


def test_na():
    assert bool(NA) is False
    assert (NA is False) is False
    assert (NA == False) is False  # noqa
    assert (1 if NA else 0) == 0
