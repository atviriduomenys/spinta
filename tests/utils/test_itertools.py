from spinta.utils.itertools import schunks


def test_schunks_smaller():
    stream = ['abc', 'def', 'efg']
    assert list(schunks(stream, 1)) == [
        ['abc'],
        ['def'],
        ['efg'],
    ]


def test_schunks_exact():
    stream = ['abc', 'def', 'efg']
    assert list(schunks(stream, 3)) == [
        ['abc'],
        ['def'],
        ['efg'],
    ]


def test_schunks_bigger():
    stream = ['abc', 'def', 'efg']
    assert list(schunks(stream, 4)) == [
        ['abc'],
        ['def'],
        ['efg'],
    ]


def test_schunks_2x_bigger():
    stream = ['abc', 'def', 'efg']
    assert list(schunks(stream, 6)) == [
        ['abc', 'def'],
        ['efg'],
    ]


def test_schunks_3x_bigger():
    stream = ['abc', 'def', 'efg']
    assert list(schunks(stream, 9)) == [
        ['abc', 'def', 'efg'],
    ]
