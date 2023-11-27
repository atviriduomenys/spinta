import pytest

from pathlib import Path

from spinta import commands
from spinta.core.config import RawConfig
from spinta.testing.manifest import load_manifest_and_context
from spinta.testing.manifest import load_manifest_get_context
from spinta.manifests.tabular.helpers import TabularManifestError
from spinta.exceptions import InvalidValue
from spinta.exceptions import InvalidName
from spinta.testing.tabular import create_tabular_manifest


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


@pytest.mark.skip("SKIP FOR NOW, SINCE CHECK SHOULD ALSO BE ON LOAD")
def test_check_names_model(tmp_path: Path, rc: RawConfig):
    create_tabular_manifest(tmp_path / 'hidrologija.csv', '''
    d | r | b | m | property | type    | source
    datasets/gov/example     |         |
                             |         |
      |   |   | data         |         |
      |   |   |   | value    | string  |
    ''')

    context = load_manifest_get_context(rc, tmp_path / 'hidrologija.csv', check_names=True)

    store = context.get('store')
    manifest = store.manifest

    with pytest.raises(InvalidName) as e:
        commands.check(context, manifest)

    assert e.value.message == "Invalid 'data' model code name."


def test_check_names_property(tmp_path: Path, rc: RawConfig):
    create_tabular_manifest(tmp_path / 'hidrologija.csv', '''
    d | r | b | m | property    | type    | source
    datasets/gov/example        |         |
                                |         |
      |   |   | Data            |         |
      |   |   |   | value_Value | string  |
    ''')

    context = load_manifest_get_context(rc, tmp_path / 'hidrologija.csv', check_names=True)

    store = context.get('store')
    manifest = store.manifest

    with pytest.raises(InvalidName) as e:
        commands.check(context, manifest)

    assert e.value.message == "Invalid 'value_Value' property code name."


def test_check_names_dataset(tmp_path: Path, rc: RawConfig):
    create_tabular_manifest(tmp_path / 'hidrologija.csv', '''
    d | r | b | m | property    | type    | source
    datasets/gov/Example        |         |
                                |         |
      |   |   | Data            |         |
      |   |   |   | value       | string  |
    ''')

    context = load_manifest_get_context(rc, tmp_path / 'hidrologija.csv', check_names=True)

    store = context.get('store')
    manifest = store.manifest

    with pytest.raises(InvalidName) as e:
        commands.check(context, manifest)

    assert e.value.message == "Invalid 'datasets/gov/Example' namespace code name."
