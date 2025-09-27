from pathlib import Path

import pytest

from spinta import commands
from spinta.core.config import RawConfig
from spinta.exceptions import InvalidName, InvalidValue
from spinta.testing.manifest import load_manifest_and_context, load_manifest, load_manifest_get_context
from spinta.testing.tabular import create_tabular_manifest


@pytest.mark.manifests("internal_sql", "csv")
def test_enum_type_integer(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
):
    with pytest.raises(InvalidValue) as e:
        load_manifest(
            rc,
            """
        d | r | b | m | property | type    | prepare
        datasets/gov/example     |         |
                                 |         |
          |   |   | Data         |         |
          |   |   |   | value    | integer |
                                 | enum    | "1"
                                 |         | "2"
        """,
            manifest_type=manifest_type,
            tmp_path=tmp_path,
        )
    assert str(e.value.context["error"]) == (
        "Given enum value 1 of <class 'str'> type does not match property type, which is 'integer'."
    )


@pytest.mark.manifests("internal_sql", "csv")
def test_enum_type_string(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
):
    with pytest.raises(InvalidValue) as e:
        load_manifest(
            rc,
            """
        d | r | b | m | property | type    | prepare
        datasets/gov/example     |         |
                                 |         |
          |   |   | Data         |         |
          |   |   |   | value    | string  |
                                 | enum    | 1
                                 |         | 2
        """,
            manifest_type=manifest_type,
            tmp_path=tmp_path,
        )
    assert str(e.value.context["error"]) == (
        "Given enum value 1 of <class 'int'> type does not match property type, which is 'string'."
    )


@pytest.mark.manifests("internal_sql", "csv")
def test_enum_type_none(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property | type    | source
    datasets/gov/example     |         |
                             |         |
      |   |   | Data         |         |
      |   |   |   | value    | string  |
                             | enum    | 1
                             |         | 2
    """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )
    commands.check(context, manifest)


@pytest.mark.manifests("internal_sql", "csv")
def test_enum_type_integer_negative(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property | type    | prepare
    datasets/gov/example     |         |
      |   |   | Data         |         |
      |   |   |   | value    | integer |
                             | enum    | 1
                             |         | -2
    """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )
    commands.check(context, manifest)


@pytest.mark.manifests("internal_sql", "csv")
def test_enum_type_boolean(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property | type    | prepare
    datasets/gov/example     |         |
      |   |   | Data         |         |
      |   |   |   | value    | boolean |
                             | enum    | true
                             |         | false
    """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )
    commands.check(context, manifest)


@pytest.mark.skip("SKIP FOR NOW, SINCE CHECK SHOULD ALSO BE ON LOAD")
def test_check_names_model(context, tmp_path: Path, rc: RawConfig):
    create_tabular_manifest(
        context,
        tmp_path / "hidrologija.csv",
        """
    d | r | b | m | property | type    | source
    datasets/gov/example     |         |
                             |         |
      |   |   | data         |         |
      |   |   |   | value    | string  |
    """,
    )

    context = load_manifest_get_context(rc, tmp_path / "hidrologija.csv", check_names=True)

    store = context.get("store")
    manifest = store.manifest

    with pytest.raises(InvalidName) as e:
        commands.check(context, manifest)

    assert e.value.message == "Invalid 'data' model code name."


@pytest.mark.manifests("internal_sql", "csv")
def test_check_names_property(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
):
    context = load_manifest_get_context(
        rc,
        """
    d | r | b | m | property    | type    | source
    datasets/gov/example        |         |
                                |         |
      |   |   | Data            |         |
      |   |   |   | value_Value | string  |
    """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
        check_names=True,
    )

    store = context.get("store")
    manifest = store.manifest

    with pytest.raises(InvalidName) as e:
        commands.check(context, manifest)

    assert e.value.message == "Invalid 'value_Value' property code name."


@pytest.mark.manifests("internal_sql", "csv")
def test_check_names_dataset(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
):
    context = load_manifest_get_context(
        rc,
        """
    d | r | b | m | property    | type    | source
    datasets/gov/Example        |         |
                                |         |
      |   |   | Data            |         |
      |   |   |   | value       | string  |
    """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
        check_names=True,
    )

    store = context.get("store")
    manifest = store.manifest

    with pytest.raises(InvalidName) as e:
        commands.check(context, manifest)

    assert e.value.message == "Invalid 'datasets/gov/Example' namespace code name."


@pytest.mark.manifests("internal_sql", "csv")
def test_check_enum_values_under_source(manifest_type, tmp_path, rc):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property   | type    | ref | source     | prepare
    test_dataset               |         |     |            |        
      | resource1              | xml     |     |            |         
      |   |   | Country        |         |     | Country    |        
      |   |   |   | driving    | string  |     |            |        
      |   |   |   |            | enum    |     | 'left'     |        
      |   |   |   |            |         |     | 'right'    |        
        """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )
    commands.check(context, manifest)


@pytest.mark.manifests("internal_sql", "csv")
def test_check_enum_values_under_prepare(manifest_type, tmp_path, rc):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property   | type    | ref | source     | prepare
    test_dataset               |         |     |            |        
      | resource1              | xml     |     |            |         
      |   |   | Country        |         |     | Country    |        
      |   |   |   | driving    | string  |     |            |        
      |   |   |   |            | enum    |     |            | 'left'  
      |   |   |   |            |         |     |            | 'right' 
        """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )
    commands.check(context, manifest)


@pytest.mark.manifests("internal_sql", "csv")
def test_check_enum_values_under_source_transforms_into_prepare_value(manifest_type, tmp_path, rc):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property   | type    | ref | source     | prepare
    test_dataset               |         |     |            |        
      | resource1              | xml     |     |            |         
      |   |   | Country        |         |     | Country    |        
      |   |   |   | driving    | string  |     |            |        
      |   |   |   |            | enum    |     | "1"        | "2"    
        """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )
    commands.check(context, manifest)


@pytest.mark.manifests("internal_sql", "csv")
def test_check_enum_null_under_prepare(manifest_type, tmp_path, rc):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property   | type    | ref | source     | prepare
    test_dataset               |         |     |            |        
      | resource1              | xml     |     |            |         
      |   |   | Country        |         |     | Country    |        
      |   |   |   | driving    | string  |     |            |           
      |   |   |   |            | enum    |     |            | null
        """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )
    commands.check(context, manifest)


@pytest.mark.manifests("internal_sql", "csv")
def test_check_enum_swap_null_under_prepare(manifest_type, tmp_path, rc):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property   | type    | ref | source     | prepare
    test_dataset               |         |     |            |        
      | resource1              | xml     |     |            |         
      |   |   | Country        |         |     | Country    |        
      |   |   |   | driving    | string  |     |            |             
      |   |   |   |            | enum    |     |            | swap(null, '-')
        """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )
    commands.check(context, manifest)


@pytest.mark.manifests("internal_sql", "csv")
def test_check_enum_swap_param_validation_under_prepare(manifest_type, tmp_path, rc):
    with pytest.raises(InvalidValue) as e:
        context, manifest = load_manifest_and_context(
            rc,
            """
        d | r | b | m | property   | type    | ref | source     | prepare
        test_dataset               |         |     |            |        
          | resource1              | xml     |     |            |         
          |   |   | Country        |         |     | Country    |        
          |   |   |   | driving    | integer |     |            |             
          |   |   |   |            | enum    |     |            | swap(null, '-')
          |   |   |   |            |         |     |            | '-'
            """,
            manifest_type=manifest_type,
            tmp_path=tmp_path,
        )
        commands.check(context, manifest)
        assert e.value.message == "Invalid value."


@pytest.mark.manifests("internal_sql", "csv")
def test_check_enum_no_operation_prepare_string(manifest_type, tmp_path, rc):
    context, manifest = load_manifest_and_context(
        rc,
        """
        d | r | b | m | property | type    | ref | source       | prepare
        dataset1                 |         |     |              |
          | resource1            | sql     |     |              |
          |   |   | City         |         | id  | CITIES       |
          |   |   |   | id       | integer |     | ID           |
          |   |   |   | country  | string  |     | COUNTRY_CODE |
          |   |   |   |          | enum    |     | lt           |
          |   |   |   |          |         |     | ee           |
          |   |   |   |          |         |     | lv           |
          |   |   |   |          |         |     |              | noop()
        """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )
    commands.check(context, manifest)


@pytest.mark.manifests("internal_sql", "csv")
def test_check_enum_no_operation_prepare_integer(manifest_type, tmp_path, rc):
    context, manifest = load_manifest_and_context(
        rc,
        """
        d | r | b | m | property | type    | ref | source       | prepare
        dataset1                 |         |     |              |
          | resource1            | sql     |     |              |
          |   |   | City         |         | id  | CITIES       |
          |   |   |   | id       | integer |     | ID           |
          |   |   |   | country  | integer |     | CODE         |
          |   |   |   |          | enum    |     | 1            | 1
          |   |   |   |          |         |     | 2            | 2
          |   |   |   |          |         |     | 3            | 3
          |   |   |   |          |         |     |              | noop()
        """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )
    commands.check(context, manifest)
