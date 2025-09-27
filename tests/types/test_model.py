from unittest.mock import Mock

import pytest

from spinta.core.enums import Level
from spinta.testing.context import create_test_context
from spinta.types.model import load_level


@pytest.mark.parametrize("level", [Level.open, 3, "3"])
def test_load_level(level, rc):
    context = create_test_context(rc)
    node = Mock()
    load_level(context, node, level)
    assert node.level is Level.open
