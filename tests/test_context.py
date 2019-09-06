import concurrent.futures
import contextlib
import unittest.mock

import pytest
import sqlalchemy as sa

from spinta.components import Context


def test_set_overwrite():
    context = Context('test')
    context.set('a', 1)
    with pytest.raises(Exception) as e:
        context.set('a', 2)
    assert str(e.value) == "Context variable 'a' has been already set."
    assert context.get('a') == 1


def test_bind_overwrite():
    context = Context('test')
    context.bind('a', lambda: 1)
    with pytest.raises(Exception) as e:
        context.bind('a', lambda: 2)
    assert str(e.value) == "Context variable 'a' has been already set."
    assert context.get('a') == 1


def test_attach_in_fork():
    context = Context('test')

    @contextlib.contextmanager
    def cmgr(state):
        state['active'] = True
        yield
        state['active'] = False

    state = {'active': None}
    with context.fork('sub') as fork:
        fork.attach('cmgr', cmgr, state)
        assert state['active'] is None

        fork.get('cmgr')
        assert state['active'] is True

    assert state['active'] is False


def test_attach_in_state():
    context = Context('test')

    @contextlib.contextmanager
    def cmgr(state):
        state['active'] = True
        yield
        state['active'] = False

    state = {'active': None}
    with context:
        context.attach('cmgr', cmgr, state)
        assert state['active'] is None

        context.get('cmgr')
        assert state['active'] is True

    assert state['active'] is False


def test_attach_in_nested_state():
    context = Context('test')

    @contextlib.contextmanager
    def cmgr(state):
        state['active'] = True
        yield
        state['active'] = False

    state = {'active': None}
    context.attach('cmgr', cmgr, state)
    with context:
        assert state['active'] is None
        context.get('cmgr')
        assert state['active'] is True
    assert state['active'] is True


def test_attach_in_nested_fork():
    base = Context('base')

    @contextlib.contextmanager
    def cmgr(state):
        state['active'] = True
        yield
        state['active'] = False

    state = {'active': None}
    base.attach('cmgr', cmgr, state)
    with base.fork('fork') as fork:
        with fork:
            assert state['active'] is None
            fork.get('cmgr')
            assert state['active'] is True
        assert state['active'] is True
    assert state['active'] is False


def test_fork():
    def func(context, value):
        with context.fork('fork') as fork:
            fork.set('a', value)
        return context.get('a')

    base = Context('base')
    base.set('a', 42)

    max_threads = 4
    with concurrent.futures.ThreadPoolExecutor(max_threads) as executor:
        futures = [
            executor.submit(func, base, i)
            for i in range(max_threads)
        ]

    futures = concurrent.futures.as_completed(futures)
    results = [f.result() for f in futures]
    assert results == [42, 42, 42, 42]


def test_bind_and_resolve_in_prev_state():
    f = unittest.mock.Mock(return_value=42)
    base = Context('base')
    base.bind('a', f)
    with base:
        with base:
            assert base.get('a') == 42
        assert base.get('a') == 42
    assert base.get('a') == 42
    assert len(f.mock_calls) == 1


def test_bind_and_resolve_in_current_fork():
    f = unittest.mock.Mock(return_value=42)
    base = Context('base')
    base.bind('a', f)
    with base.fork('fork') as fork:
        assert fork.get('a') == 42
    assert base.get('a') == 42
    assert len(f.mock_calls) == 2


def test_bind_and_resolve_in_prev_fork():
    f = unittest.mock.Mock(return_value=42)
    base = Context('base')
    base.bind('a', f)
    assert base.get('a') == 42
    with base.fork('fork') as fork:
        assert fork.get('a') == 42
    assert len(f.mock_calls) == 2


def test_repr():
    base = Context('base')
    assert repr(base) == f'<spinta.components.Context(base:0) at 0x{id(base):02x}>'
    with base.fork('fork') as fork:
        assert repr(fork) == f'<spinta.components.Context(base:0 < fork:0) at 0x{id(fork):02x}>'
        with fork:
            assert repr(fork) == f'<spinta.components.Context(base:0 < fork:1) at 0x{id(fork):02x}>'
            with fork:
                assert repr(fork) == f'<spinta.components.Context(base:0 < fork:2) at 0x{id(fork):02x}>'
