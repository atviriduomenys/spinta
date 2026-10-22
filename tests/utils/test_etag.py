import pytest

from spinta.utils.http.etag import ETag


@pytest.mark.parametrize(
    "header,value,weak,expected",
    [
        ('"revision"', "revision", False, '"revision"'),
        ('W/"revision"', "revision", True, 'W/"revision"'),
        ('""', "", False, '""'),
        ("revision", "revision", False, '"revision"'),
        ('W/"unterminated', "unterminated", True, 'W/"unterminated"'),
        ('"a b"', "a b", False, '"a b"'),
    ],
)
def test_etag_parses_and_serializes(header: str, value: str, weak: bool, expected: str):
    etag = ETag.from_header(header)
    assert str(etag) == expected
    assert etag.is_weak() is weak
    assert etag.value() == value


def test_etag_strength_conversion_preserves_value():
    strong = ETag("revision")
    weak = strong.to_weak()
    assert str(weak) == 'W/"revision"'
    assert str(weak.to_strong()) == '"revision"'
    assert str(strong) == '"revision"'


@pytest.mark.parametrize("etag", ['"revision"', 'W/"revision"'])
@pytest.mark.parametrize(
    "header",
    ['"revision"', 'W/"revision"', '"other", W/"revision"', " * ", '"revision", invalid'],
)
def test_etag_matches_if_none_match_using_weak_comparison(etag: str, header: str):
    assert ETag.from_header(etag).matches(header)


@pytest.mark.parametrize("header", ['"different"', 'W/"different"', "revision", 'w/"revision"'])
def test_etag_rejects_nonmatching_if_none_match(header: str):
    assert not ETag("revision").matches(header)
