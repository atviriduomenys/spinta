import pytest

from spinta.utils.units import tobytes
from spinta.utils.units import toseconds


@pytest.mark.parametrize('s,b', [
    ('1', 1),
    ('1b', 1),
    ('1k', 1_000),
    ('8M', 8_000_000),
])
def test_tobytes(s, b):
    assert tobytes(s) == b


@pytest.mark.parametrize('t,s', [
    ('1', 1),
    ('5s', 5),
    ('10m', 600),
    ('1h', 3600),
])
def test_toseconds(t, s):
    assert toseconds(t) == s
