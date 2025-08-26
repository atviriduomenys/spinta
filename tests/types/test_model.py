from unittest.mock import Mock

import pytest

from spinta.core.enums import Level
from spinta.types.model import load_level


@pytest.mark.parametrize("level", [Level.open, 3, "3"])
def test_load_level(level):
    node = Mock()
    load_level(node, level)
    assert node.level is Level.open
