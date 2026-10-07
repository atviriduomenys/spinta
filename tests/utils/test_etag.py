import pytest

from spinta.utils.http.etag import ETag


@pytest.mark.parametrize(
    "header,value,weak",
    [
        ('"revision"', "revision", False),
        ('W/"revision"', "revision", True),
        ('""', "", False),
    ],
)
def test_etag_parses_and_serializes(header: str, value: str, weak: bool):
    etag = ETag.from_header(header)
    assert str(etag) == header
    assert etag.is_weak() is weak
    assert etag.value() == value


def test_etag_strength_conversion_preserves_value():
    strong = ETag("revision")
    weak = strong.to_weak()
    assert str(weak) == 'W/"revision"'
    assert str(weak.to_strong()) == '"revision"'
    assert str(strong) == '"revision"'


@pytest.mark.parametrize("header", ["revision", 'w/"revision"', '"a"b"', '"a\nb"'])
def test_etag_rejects_invalid_headers(header: str):
    with pytest.raises(ValueError):
        ETag.from_header(header)


@pytest.mark.parametrize("value", ['a"b', "a\nb", "a b"])
def test_etag_rejects_invalid_values(value: str):
    with pytest.raises(ValueError):
        ETag(value)


@pytest.mark.parametrize("weak", [False, True])
@pytest.mark.parametrize("header", ['"revision"', 'W/"revision"', '"other", W/"revision"', " * "])
def test_etag_matches_if_none_match_using_weak_comparison(weak: bool, header: str):
    assert ETag("revision", weak=weak).matches(header)


@pytest.mark.parametrize("header", ['"different"', 'W/"different"', "revision", 'w/"revision"'])
def test_etag_rejects_nonmatching_if_none_match(header: str):
    assert not ETag("revision").matches(header)


def test_etag_matches_commas_inside_opaque_values():
    assert ETag("a,b").matches('"other", W/"a,b"')
    assert not ETag("a").matches('"a,b"')


@pytest.mark.parametrize("header", ['"revision", invalid', '"revision", "unterminated', '"revision" "other"'])
def test_etag_rejects_malformed_lists_even_when_a_tag_matches(header: str):
    assert not ETag("revision").matches(header)


def test_etag_accepts_whitespace_and_empty_list_elements():
    assert ETag("revision").matches(' ,\t W/"revision" , , ')


def test_etag_does_not_treat_backslashes_as_quote_escapes():
    assert ETag("revision\\").matches('"revision\\", "other"')


@pytest.mark.parametrize("value", ["\x00", "\t", "\x7f", "\u0100"])
def test_etag_rejects_characters_outside_http_tag_syntax(value: str):
    with pytest.raises(ValueError):
        ETag(value)


def test_etag_preserves_non_ascii_http_characters():
    assert str(ETag.from_header('"\x80\xff"')) == '"\x80\xff"'
