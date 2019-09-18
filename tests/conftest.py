import snoop
import pprint
import pprintpp

# See: https://github.com/alexmojaki/snoop
snoop.install(
    # Force colors, since pytest captures all output by default.
    color=True,
)

# User pprintpp for nicer and more readable output.
# https://github.com/alexmojaki/snoop/issues/13
pprint.pformat = pprintpp.pformat

pytest_plugins = [
    'spinta.testing.pytest',
]
