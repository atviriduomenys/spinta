import snoop

# See: https://github.com/alexmojaki/snoop
snoop.install(
    # Force colors, since pytest captures all output by default.
    color=True,
)

pytest_plugins = [
    'spinta.testing.pytest',
]

