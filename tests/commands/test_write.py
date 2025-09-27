from unittest.mock import Mock

import pytest

from spinta.commands.write import is_streaming_request


@pytest.mark.parametrize(
    "content_type,result",
    [
        ("application/x-ndjson", True),
        ("application/x-ndjson; charset=UTF-8", True),
        ("application/json", False),
    ],
)
def test_is_streaming_request(content_type: str, result: bool):
    request = Mock()
    request.headers = {
        "content-type": content_type,
    }
    assert is_streaming_request(request) is result
