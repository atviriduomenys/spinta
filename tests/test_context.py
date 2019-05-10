import pytest

from spinta.components import Context


def test_set_overwrite():
    context = Context()
    context.set('a', 1)
    with pytest.raises(Exception) as e:
        context.set('a', 2)
    assert str(e.value) == "Context variable 'a' has been already set."
    assert context.get('a') == 1


def test_bind_overwrite():
    context = Context()
    context.bind('a', lambda: 1)
    with pytest.raises(Exception) as e:
        context.bind('a', lambda: 2)
    assert str(e.value) == "Context variable 'a' has been already set."
    assert context.get('a') == 1
