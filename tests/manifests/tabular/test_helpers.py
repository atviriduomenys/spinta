import pytest

from spinta.exceptions import PropertyNotFound
from spinta.manifests.tabular.helpers import normalizes_columns


def test_normalizes_columns_short_names():
    assert normalizes_columns(['d', 'r', 'b', 'm']) == [
        'dataset',
        'resource',
        'base',
        'model',
    ]


def test_normalizes_columns_strip_unknown_columns():
    assert normalizes_columns(['m', 'property', 'unknown']) == [
        'model',
        'property',
    ]


def test_normalizes_columns_strip_unknown_columns_2():
    assert normalizes_columns(['unknown']) == []


def test_normalizes_columns_strip_unknown_columns_3():
    assert normalizes_columns(['id', None, None]) == ['id']


def test_normalizes_columns_check_unknown_columns():
    with pytest.raises(PropertyNotFound):
        normalizes_columns(['unknown', 'model'])

