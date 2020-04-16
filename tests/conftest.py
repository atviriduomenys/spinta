import sys

import snoop
import pprint
import pprintpp

# See: https://github.com/alexmojaki/snoop
snoop.install(
    # Force colors, since pytest captures all output by default.
    color=True,
    # Some tests mock sys.stderr, to we need to pass it directly.
    out=sys.stderr,
)

# User pprintpp for nicer and more readable output.
# https://github.com/alexmojaki/snoop/issues/13
pprint.pformat = pprintpp.pformat

pytest_plugins = ['spinta.testing.pytest']
