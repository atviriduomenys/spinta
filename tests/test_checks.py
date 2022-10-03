import pytest

from spinta import commands
from spinta.testing.manifest import load_manifest_and_context
from spinta.manifests.tabular.helpers import TabularManifestError
from spinta.exceptions import InvalidValue


def test_enum_level(tmpdir, rc):
    with pytest.raises(TabularManifestError) as e:
        context, manifest = load_manifest_and_context(rc, '''
        d | r | b | m | property | type    | prepare | level | title
        datasets/gov/example     |         |         |       |
                                 |         |         |       |
          |   |   | Data         |         |         |       |
          |   |   |   | value    | integer |         |       |
                                 | enum    | 1       | 3     | Positive
                                 |         | 2       | 3     | Negative
        ''')
    assert str(e.value) == (
        "None:6: Enum's do not have a level, but level '3' is given."
    )


def test_enum_type_integer(tmpdir, rc):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b | m | property | type    | prepare
    datasets/gov/example     |         |
                             |         |
      |   |   | Data         |         |
      |   |   |   | value    | integer |
                             | enum    | "1"
                             |         | "2"
    ''')
    with pytest.raises(InvalidValue) as e:
        commands.check(context, manifest)
    assert str(e.value.context['error']) == (
        "Given enum value 1 of <class 'str'> type does not match property "
        "type, which is 'integer'."
    )


def test_enum_type_string(tmpdir, rc):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b | m | property | type    | prepare
    datasets/gov/example     |         |
                             |         |
      |   |   | Data         |         |
      |   |   |   | value    | string  |
                             | enum    | 1
                             |         | 2
    ''')
    with pytest.raises(InvalidValue) as e:
        commands.check(context, manifest)
    assert str(e.value.context['error']) == (
        "Given enum value 1 of <class 'int'> type does not match property "
        "type, which is 'string'."
    )


def test_enum_type_none(tmpdir, rc):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b | m | property | type    | source
    datasets/gov/example     |         |
                             |         |
      |   |   | Data         |         |
      |   |   |   | value    | string  |
                             | enum    | 1
                             |         | 2
    ''')
    commands.check(context, manifest)


def test_enum_type_integer_negative(tmpdir, rc):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b | m | property | type    | prepare
    datasets/gov/example     |         |
      |   |   | Data         |         |
      |   |   |   | value    | integer |
                             | enum    | 1
                             |         | -2
    ''')
    commands.check(context, manifest)


def test_enum_type_boolean(tmpdir, rc):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b | m | property | type    | prepare
    datasets/gov/example     |         |
      |   |   | Data         |         |
      |   |   |   | value    | boolean |
                             | enum    | true
                             |         | false
    ''')
    commands.check(context, manifest)
