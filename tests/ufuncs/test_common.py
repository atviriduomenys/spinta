import binascii

import pytest

from spinta.components import Context
from spinta.core.ufuncs import Env


class TestBase64Decode:
    def test_base64_decode(self, context: Context):
        env = Env(context)
        assert env.call("base64_decode", "dGVzdA==") == "test"

    def test_raise_binascii_error_if_base64_is_invalid(self, context: Context):
        env = Env(context)
        with pytest.raises(binascii.Error):
            env.call("base64_decode", "lalald")

    def test_raise_value_error_if_string_has_non_ascii_characters(self, context: Context):
        env = Env(context)
        with pytest.raises(ValueError):
            env.call("base64_decode", "ąčę")
