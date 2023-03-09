import pytest

from spinta import commands
from spinta.core.config import configure_rc
from spinta.testing.manifest import load_manifest_and_context
from spinta.manifests.tabular.helpers import TabularManifestError
from spinta.exceptions import InvalidValue


def test_enum_level(tmp_path, rc):
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


def test_enum_type_integer(tmp_path, rc):
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


def test_enum_type_string(tmp_path, rc):
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


def test_enum_type_none(tmp_path, rc):
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


def test_enum_type_integer_negative(tmp_path, rc):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b | m | property | type    | prepare
    datasets/gov/example     |         |
      |   |   | Data         |         |
      |   |   |   | value    | integer |
                             | enum    | 1
                             |         | -2
    ''')
    commands.check(context, manifest)


def test_enum_type_boolean(tmp_path, rc):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b | m | property | type    | prepare
    datasets/gov/example     |         |
      |   |   | Data         |         |
      |   |   |   | value    | boolean |
                             | enum    | true
                             |         | false
    ''')
    commands.check(context, manifest)


def test_filename(tmpdir, rc):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b | m | property | type    | prepare
    datasets/gov/example     |         |
      |   |   | Data         |         |
      |   |   |   | value    | string  | 
    ''')

    rc = context.get('rc')
    context = context.fork('configure')
    context.set('rc', configure_rc(rc, ['hidrologija.csv'], filename=True))

    with pytest.raises(Exception) as e:
        commands.check(context, manifest)
    assert str(e.value) == (
        "Dataset namespace datasets/gov/example not match the csv filename hidrologija."
    )
