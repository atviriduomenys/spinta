import pathlib

from spinta.utils.path import is_ignored


def test_ignore():
    ignore = [
        '/test.yml',
        'relative.yml',
        '*.txt',
        '/*.rst',
    ]
    base = pathlib.Path('/here')
    assert is_ignored(ignore, base, base / 'test.yml') is True
    assert is_ignored(ignore, base, base / 'a/test.yml') is False
    assert is_ignored(ignore, base, base / 'relative.yml') is True
    assert is_ignored(ignore, base, base / 'test.txt') is True
    assert is_ignored(ignore, base, base / 'a/test.txt') is True
    assert is_ignored(ignore, base, base / 'a/test.rst') is False
    assert is_ignored(ignore, base, base / 'test.rst') is True


def test_ignore_whole_directory():
    base = pathlib.Path('/here')
    assert is_ignored(['/env/'], base, base / 'env/a/b/c/d') is True
    assert is_ignored(['env/'], base, base / 'env/a/b/c/d') is True
    assert is_ignored(['env/'], base, base / 'a/b/env/c/d') is True
    assert is_ignored(['/env/'], base, base / 'a') is False
