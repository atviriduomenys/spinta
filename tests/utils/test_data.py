from spinta.utils.schema import NA
from spinta.utils.data import take


def test_strip_reserved():
    data = {
        '_a': 1,
        'b_': 2,
    }
    assert take(data) == {
        'b_': 2,
    }


def test_include_reserved():
    data = {
        '_a': 1,
        'b_': 2,
    }
    assert take(all, data) == data


def test_strip_na():
    data = {
        'a': 1,
        'b': NA,
    }
    assert take(data) == {
        'a': 1,
    }


def test_first():
    d1 = NA
    d2 = {}
    d3 = {'a': NA, 'b': 3}
    d4 = {'a': 1}
    d5 = {'a': 2}
    assert take(d1, d2, d3, d4, d5) == {
        'a': 1,
        'b': 3,
    }


def test_data_as_first_arg():
    d1 = {'a': NA, 'b': 3}
    d2 = {'a': 1}
    assert take(d1, d2) == {
        'a': 1,
        'b': 3,
    }


def test_keys():
    data = {
        '_a': 1,
        '_b': 2,
        'c_': 3,
        'd_': 4,
    }
    assert take(['_a', 'c_'], data) == {
        '_a': 1,
        'c_': 3,
    }


def test_keys_na():
    data = {
        '_a': 1,
        '_b': 2,
        'c_': NA,
        'd_': 4,
    }
    assert take(['_a', 'c_'], data) == {
        '_a': 1,
    }


def test_keys_none():
    data = {
        '_a': 1,
        '_b': 2,
        'c_': None,
        'd_': 4,
    }
    assert take(['_a', 'c_'], data) == {
        '_a': 1,
        'c_': None,
    }


def test_keys_data_na():
    data = NA
    assert take(['_a', 'c_'], data) == {}


def test_keys_first():
    d1 = NA
    d2 = {}
    d3 = {'a': NA, 'b': 3}
    d4 = {'a': 1}
    d5 = {'a': 2}
    assert take(['a', 'b'], d1, d2, d3, d4, d5) == {
        'a': 1,
        'b': 3,
    }


def test_key():
    data = {
        '_a': 1,
        '_b': 2,
        'c_': 3,
        'd_': 4,
    }
    assert take('c_', data) == 3


def test_key_reserved():
    data = {
        '_a': 1,
        '_b': 2,
        'c_': 3,
        'd_': 4,
    }
    assert take('_b', data) == 2


def test_key_na():
    data = {
        '_a': 1,
        '_b': 2,
        'c_': NA,
        'd_': 4,
    }
    assert take('c_', data) is NA


def test_key_data_na():
    data = NA
    assert take('c_', data) == NA


def test_key_first():
    d1 = NA
    d2 = {}
    d3 = {'a': NA}
    d4 = {'a': 1}
    d5 = {'a': 2}
    assert take('a', d1, d2, d3, d4, d5) == 1


def test_nested():
    data = {'a': {'b': {'c': 1}, 'd': 2}, 'e': 3}
    assert take('a.b.c', data) == 1


def test_nested_na():
    data = {'a': NA, 'e': 3}
    assert take('a.b.c', data) == NA


def test_nested_data_na():
    data = NA
    assert take('a.b.c', data) == NA


def test_nested_keys():
    data = {'a': {'b': {'c': 1}, 'd': 2}, 'e': 3}
    assert take(['a.b.c', 'a.d'], data) == {
        'a.b.c': 1,
        'a.d': 2,
    }


def test_nested_keys_NA():
    data = {'a': {'b': NA, 'd': 2}, 'e': 3}
    assert take(['a.b.c', 'a.d'], data) == {
        'a.d': 2,
    }


def test_no_keys():
    data = {'a': 1}
    assert take(data) == data


def test_no_keys_dotted():
    data = {'a.b': '3'}
    assert take(data) == data


def test_all_in_keys():
    data = {
        '_a': 1,
        '_b': 2,
        'c_': 3,
        'd_': 4,
    }
    assert take(['_a', all], data) == {
        '_a': 1,
        'c_': 3,
        'd_': 4,
    }
