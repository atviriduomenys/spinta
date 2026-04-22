import pytest

from spinta.core.config import RawConfig, Path
from spinta.core.enums import Mode
from spinta.exceptions import ReservedPropertyTypeShouldMatchPrimaryKey, ReservedPropertySourceOrModelRefShouldBeSet
from spinta.testing.client import create_test_client
from spinta.testing.manifest import prepare_manifest


def test_for_id_uuid(rc: RawConfig, tmp_path: Path):
    xml = """
    <root>
        <order>
            <id>ed76eda3-7922-4a7d-9ba8-62828ca0ae98</id>
            <code>ORD001</code>
        </order>
        <order>
            <id>1590ab44-6463-4da7-8862-3598f6e83924</id>
            <code>ORD002</code>
        </order>
    </root>
    """
    path = tmp_path / "test.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property                      | type       | ref      | source        | level      | access
    example                                       |            |          |               |            |
      | data                                      | dask/xml   |          | {path}        |            |
      |   |   | Region                            |            | id       | /root/order   |            |
      |   |   |   | id                            | uuid       |          | id            |            | open
      |   |   |   | _id                           | uuid       |          |               |            | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Region", ["getall", "getone"])

    resp = app.get("/example/Region")
    assert resp.status_code == 200
    data = resp.json()["_data"]
    assert data == [
        {
            "_type": "example/Region",
            "_id": "ed76eda3-7922-4a7d-9ba8-62828ca0ae98",
            "_revision": None,
            "id": "ed76eda3-7922-4a7d-9ba8-62828ca0ae98",
        },
        {
            "_type": "example/Region",
            "_id": "1590ab44-6463-4da7-8862-3598f6e83924",
            "_revision": None,
            "id": "1590ab44-6463-4da7-8862-3598f6e83924",
        },
    ]
    with pytest.raises(NotImplementedError):
        app.get("/example/Region/ed76eda3-7922-4a7d-9ba8-62828ca0ae98")
        # Expected, XML does not support getone operations


def test_for_id_uuid_errors_with_string(rc: RawConfig, tmp_path: Path):
    with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
        prepare_manifest(
            rc,
            """
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      | dask/xml   |          |               |            |
          |   |   | Region                            |            | id       | /root/order   |            |
          |   |   |   | id                            | string     |          | id            |            | open
          |   |   |   | _id                           | uuid       |          |               |            | open
        """,
            mode=Mode.external,
        )


def test_for_id_uuid_errors_with_integer(rc: RawConfig, tmp_path: Path):
    with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
        prepare_manifest(
            rc,
            """
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      | dask/xml   |          |               |            |
          |   |   | Region                            |            | id       | /root/order   |            |
          |   |   |   | id                            | integer    |          | id            |            | open
          |   |   |   | _id                           | uuid       |          |               |            | open
        """,
            mode=Mode.external,
        )


def test_for_id_integer(rc: RawConfig, tmp_path: Path):
    xml = """
    <root>
        <order>
            <id>123</id>
            <code>ORD001</code>
        </order>
        <order>
            <id>1234</id>
            <code>ORD002</code>
        </order>
    </root>
    """
    path = tmp_path / "test.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property                      | type       | ref      | source        | level      | access
    example                                       |            |          |               |            |
      | data                                      | dask/xml   |          | {path}        |            |
      |   |   | Region                            |            | id       | /root/order   |            |
      |   |   |   | id                            | integer    |          | id            |            | open
      |   |   |   | _id                           | integer    |          |               |            | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Region", ["getall", "getone"])

    resp = app.get("/example/Region")
    assert resp.status_code == 200
    data = resp.json()["_data"]
    assert data == [
        {"_type": "example/Region", "_id": 123, "_revision": None, "id": 123},
        {"_type": "example/Region", "_id": 1234, "_revision": None, "id": 1234},
    ]
    with pytest.raises(NotImplementedError):
        app.get("/example/Region/123")
        # Expected, XML does not support getone operations


def test_for_id_integer_errors_with_string(rc: RawConfig, tmp_path: Path):
    with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
        prepare_manifest(
            rc,
            """
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      | dask/xml   |          |               |            |
          |   |   | Region                            |            | id       | /root/order   |            |
          |   |   |   | id                            | string     |          | id            |            | open
          |   |   |   | _id                           | integer    |          |               |            | open
        """,
            mode=Mode.external,
        )


def test_for_id_integer_errors_with_uuid(rc: RawConfig, tmp_path: Path):
    with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
        prepare_manifest(
            rc,
            """
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      | dask/xml   |          |               |            |
          |   |   | Region                            |            | id       | /root/order   |            |
          |   |   |   | id                            | uuid       |          | id            |            | open
          |   |   |   | _id                           | integer    |          |               |            | open
        """,
            mode=Mode.external,
        )


def test_for_id_string_with_string(rc: RawConfig, tmp_path: Path):
    xml = """
    <root>
        <order>
            <code>ORD001</code>
        </order>
        <order>
            <code>ORD002</code>
        </order>
    </root>
    """
    path = tmp_path / "test.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property                      | type       | ref      | source        | level      | access
    example                                       |            |          |               |            |
      | data                                      | dask/xml   |          | {path}        |            |
      |   |   | Region                            |            | code     | /root/order   |            |
      |   |   |   | code                          | string     |          | code          |            | open
      |   |   |   | _id                           | string     |          |               |            | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Region", ["getall", "getone"])

    resp = app.get("/example/Region")
    assert resp.status_code == 200
    data = resp.json()["_data"]
    assert data == [
        {"_type": "example/Region", "_id": "ORD001", "_revision": None, "code": "ORD001"},
        {"_type": "example/Region", "_id": "ORD002", "_revision": None, "code": "ORD002"},
    ]
    with pytest.raises(NotImplementedError):
        app.get("/example/Region/=ORD001")
        # Expected, XML does not support getone operations


def test_for_id_string_with_integer(rc: RawConfig, tmp_path: Path):
    xml = """
    <root>
        <order>
            <id>123</id>
        </order>
        <order>
            <id>1234</id>
        </order>
    </root>
    """
    path = tmp_path / "test.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property                      | type       | ref      | source        | level      | access
    example                                       |            |          |               |            |
      | data                                      | dask/xml   |          | {path}        |            |
      |   |   | Region                            |            | id       | /root/order   |            |
      |   |   |   | id                            | integer    |          | id            |            | open
      |   |   |   | _id                           | string     |          |               |            | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Region", ["getall", "getone"])

    resp = app.get("/example/Region")
    assert resp.status_code == 200
    data = resp.json()["_data"]
    assert data == [
        {"_type": "example/Region", "_id": "123", "_revision": None, "id": 123},
        {"_type": "example/Region", "_id": "1234", "_revision": None, "id": 1234},
    ]
    with pytest.raises(NotImplementedError):
        app.get("/example/Region/=123")
        # Expected, XML does not support getone operations


def test_for_id_string_with_uuid(rc: RawConfig, tmp_path: Path):
    xml = """
    <root>
        <order>
            <uuid_id>d6420786-082f-4ee4-9624-7a559f31d032</uuid_id>
        </order>
        <order>
            <uuid_id>8f5773d6-a5eb-4409-8f88-aca874e27200</uuid_id>
        </order>
    </root>
    """
    path = tmp_path / "test.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property                      | type       | ref      | source        | level      | access
    example                                       |            |          |               |            |
      | data                                      | dask/xml   |          | {path}        |            |
      |   |   | Region                            |            | uuid_id  | /root/order   |            |
      |   |   |   | uuid_id                       | uuid       |          | uuid_id       |            | open
      |   |   |   | _id                           | string     |          |               |            | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Region", ["getall", "getone"])

    resp = app.get("/example/Region")
    assert resp.status_code == 200
    data = resp.json()["_data"]
    assert data == [
        {
            "_type": "example/Region",
            "_id": "d6420786-082f-4ee4-9624-7a559f31d032",
            "_revision": None,
            "uuid_id": "d6420786-082f-4ee4-9624-7a559f31d032",
        },
        {
            "_type": "example/Region",
            "_id": "8f5773d6-a5eb-4409-8f88-aca874e27200",
            "_revision": None,
            "uuid_id": "8f5773d6-a5eb-4409-8f88-aca874e27200",
        },
    ]
    with pytest.raises(NotImplementedError):
        app.get("/example/Region/=d6420786-082f-4ee4-9624-7a559f31d032")
        # Expected, XML does not support getone operations


def test_for_id_comp(rc: RawConfig, tmp_path: Path):
    xml = """
    <root>
        <order>
            <id>123</id>
            <code>ORD001</code>
        </order>
        <order>
            <id>1234</id>
            <code>ORD002</code>
        </order>
    </root>
    """
    path = tmp_path / "test.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property                      | type       | ref       | source        | level      | access
    example                                       |            |           |               |            |       
      | data                                      | dask/xml   |           | {path}        |            |       
      |   |   | Region                            |            | id, code  | /root/order   |            |       
      |   |   |   | code                          | string     |           | code          |            | open  
      |   |   |   | _id                           | string     |           |               |            | open  
      |   |   |   | id                            | integer    |           | id            |            | open  
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Region", ["getall", "getone"])

    resp = app.get("/example/Region")
    assert resp.status_code == 200
    data = resp.json()["_data"]
    assert data == [
        {"_type": "example/Region", "_id": "123,ORD001", "_revision": None, "code": "ORD001", "id": 123},
        {"_type": "example/Region", "_id": "1234,ORD002", "_revision": None, "code": "ORD002", "id": 1234},
    ]
    with pytest.raises(NotImplementedError):
        app.get("/example/Region/123,ORD001")
        # Expected, XML does not support getone operations


def test_for_id_comp_error_with_integer(rc: RawConfig, tmp_path: Path):
    with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
        prepare_manifest(
            rc,
            """
        d | r | b | m | property                      | type       | ref       | source        | level      | access
        example                                       |            |           |               |            |       
          | data                                      | dask/xml   |           |               |            |       
          |   |   | Region                            |            | id, code  | /root/order   |            |       
          |   |   |   | code                          | string     |           | code          |            | open  
          |   |   |   | _id                           | integer    |           |               |            | open  
          |   |   |   | id                            | integer    |           | id            |            | open  
        """,
            mode=Mode.external,
        )


def test_for_id_comp_error_with_uuid(rc: RawConfig, tmp_path: Path):
    with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
        prepare_manifest(
            rc,
            """
        d | r | b | m | property                      | type       | ref       | source        | level      | access
        example                                       |            |           |               |            |       
          | data                                      | dask/xml   |           |               |            |       
          |   |   | Region                            |            | id, code  | /root/order   |            |       
          |   |   |   | code                          | string     |           | code          |            | open  
          |   |   |   | _id                           | uuid       |           |               |            | open  
          |   |   |   | id                            | integer    |           | id            |            | open  
        """,
            mode=Mode.external,
        )


def test_for_id_comp_error_with_base32(rc: RawConfig, tmp_path: Path):
    with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
        prepare_manifest(
            rc,
            """
        d | r | b | m | property                      | type       | ref       | source        | level      | access
        example                                       |            |           |               |            |       
          | data                                      | dask/xml   |           |               |            |       
          |   |   | Region                            |            | id, code  | /root/order   |            |       
          |   |   |   | code                          | string     |           | code          |            | open  
          |   |   |   | _id                           | base32     |           |               |            | open  
          |   |   |   | id                            | integer    |           | id            |            | open  
        """,
            mode=Mode.external,
        )


def test_for_id_base32_with_string(rc: RawConfig, tmp_path: Path):
    xml = """
    <root>
        <order>
            <code>ORD001</code>
        </order>
        <order>
            <code>ORD002</code>
        </order>
    </root>
    """
    path = tmp_path / "test.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property                      | type       | ref       | source        | level      | access
    example                                       |            |           |               |            |        
      | data                                      | dask/xml   |           | {path}        |            |        
      |   |   | Region                            |            | code      | /root/order   |            |        
      |   |   |   | code                          | string     |           | code          |            | open   
      |   |   |   | _id                           | base32     |           |               |            | open   
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Region", ["getall", "getone"])

    resp = app.get("/example/Region")
    assert resp.status_code == 200
    data = resp.json()["_data"]
    assert data == [
        {"_type": "example/Region", "_id": "J5JEIMBQGE======", "_revision": None, "code": "ORD001"},
        {"_type": "example/Region", "_id": "J5JEIMBQGI======", "_revision": None, "code": "ORD002"},
    ]
    with pytest.raises(NotImplementedError):
        app.get("/example/Region/=J5JEIMBQGE======")
        # Expected, XML does not support getone operations


def test_for_id_base32_with_integer(rc: RawConfig, tmp_path: Path):
    xml = """
    <root>
        <order>
            <id>123</id>
        </order>
        <order>
            <id>1234</id>
        </order>
    </root>
    """
    path = tmp_path / "test.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property                      | type       | ref       | source        | level      | access
    example                                       |            |           |               |            |        
      | data                                      | dask/xml   |           | {path}        |            |        
      |   |   | Region                            |            | id        | /root/order   |            |        
      |   |   |   | _id                           | base32     |           |               |            | open   
      |   |   |   | id                            | integer    |           | id            |            | open   
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Region", ["getall", "getone"])

    resp = app.get("/example/Region")
    assert resp.status_code == 200
    data = resp.json()["_data"]
    assert data == [
        {"_type": "example/Region", "_id": "GEZDG===", "_revision": None, "id": 123},
        {"_type": "example/Region", "_id": "GEZDGNA=", "_revision": None, "id": 1234},
    ]
    with pytest.raises(NotImplementedError):
        app.get("/example/Region/=GEZDG===")
        # Expected, XML does not support getone operations


def test_for_id_base32_with_uuid(rc: RawConfig, tmp_path: Path):
    xml = """
    <root>
        <order>
            <uuid_id>d6420786-082f-4ee4-9624-7a559f31d032</uuid_id>
        </order>
        <order>
            <uuid_id>8f5773d6-a5eb-4409-8f88-aca874e27200</uuid_id>
        </order>
    </root>
    """
    path = tmp_path / "test.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property                      | type       | ref       | source        | level      | access
    example                                       |            |           |               |            |        
      | data                                      | dask/xml   |           | {path}        |            |        
      |   |   | Region                            |            | uuid_id   | /root/order   |            |        
      |   |   |   | _id                           | base32     |           |               |            | open   
      |   |   |   | uuid_id                       | uuid       |           | uuid_id       |            | open   
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Region", ["getall", "getone"])

    resp = app.get("/example/Region")
    assert resp.status_code == 200
    data = resp.json()["_data"]
    assert data == [
        {
            "_type": "example/Region",
            "_id": "MQ3DIMRQG44DMLJQHAZGMLJUMVSTILJZGYZDILJXME2TKOLGGMYWIMBTGI======",
            "_revision": None,
            "uuid_id": "d6420786-082f-4ee4-9624-7a559f31d032",
        },
        {
            "_type": "example/Region",
            "_id": "HBTDKNZXGNSDMLLBGVSWELJUGQYDSLJYMY4DQLLBMNQTQNZUMUZDOMRQGA======",
            "_revision": None,
            "uuid_id": "8f5773d6-a5eb-4409-8f88-aca874e27200",
        },
    ]
    with pytest.raises(NotImplementedError):
        app.get("/example/Region/=MQ3DIMRQG44DMLJQHAZGMLJUMVSTILJZGYZDILJXME2TKOLGGMYWIMBTGI======")
        # Expected, XML does not support getone operations


def test_error_if_model_ref_and_source_empty(rc: RawConfig, tmp_path: Path):
    with pytest.raises(ReservedPropertySourceOrModelRefShouldBeSet):
        prepare_manifest(
            rc,
            """
        d | r | b | m | property                      | type       | ref       | source        | level      | access
        example                                       |            |           |               |            |       
          | data                                      | dask/xml   |           |               |            |       
          |   |   | Region                            |            |           | /root/order   |            |       
          |   |   |   | _id                           | base32     |           |               |            | open  
        """,
            mode=Mode.external,
        )


def test_error_if_model_ref_and_source_populated(rc: RawConfig, tmp_path: Path):
    with pytest.raises(ReservedPropertySourceOrModelRefShouldBeSet):
        prepare_manifest(
            rc,
            """
        d | r | b | m | property                      | type       | ref       | source        | level      | access
        example                                       |            |           |               |            |       
          | data                                      | dask/xml   |           |               |            |       
          |   |   | Region                            |            | id        | /root/order   |            |       
          |   |   |   | _id                           | base32     |           | id            |            | open  
          |   |   |   | id                            | integer    |           |               |            | open  
          
        """,
            mode=Mode.external,
        )


def test_for_id_uuid_works_with_string_if_source_set(rc: RawConfig, tmp_path: Path):
    xml = """
    <root>
        <order>
            <id>ed76eda3-7922-4a7d-9ba8-62828ca0ae98</id>
            <code>ORD001</code>
        </order>
        <order>
            <id>1590ab44-6463-4da7-8862-3598f6e83924</id>
            <code>ORD002</code>
        </order>
    </root>
    """
    path = tmp_path / "test.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property                      | type       | ref      | source        | level      | access
    example                                       |            |          |               |            |
      | data                                      | dask/xml   |          | {path}        |            |
      |   |   | Region                            |            |          | /root/order   |            |
      |   |   |   | id                            | uuid       |          | id            |            | open
      |   |   |   | _id                           | uuid       |          | id            |            | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Region", ["getall", "getone"])

    resp = app.get("/example/Region")
    assert resp.status_code == 200
    data = resp.json()["_data"]
    assert data == [
        {
            "_type": "example/Region",
            "_id": "ed76eda3-7922-4a7d-9ba8-62828ca0ae98",
            "_revision": None,
            "id": "ed76eda3-7922-4a7d-9ba8-62828ca0ae98",
        },
        {
            "_type": "example/Region",
            "_id": "1590ab44-6463-4da7-8862-3598f6e83924",
            "_revision": None,
            "id": "1590ab44-6463-4da7-8862-3598f6e83924",
        },
    ]
    with pytest.raises(NotImplementedError):
        app.get("/example/Region/ed76eda3-7922-4a7d-9ba8-62828ca0ae98")
        # Expected, XML does not support getone operations
