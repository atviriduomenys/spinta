from pathlib import Path

import pytest
from zeep import Client

from spinta import commands
from spinta.core.config import RawConfig
from spinta.core.enums import Mode
from spinta.exceptions import WsdlClientError
from spinta.testing.manifest import load_manifest_and_context


def test_backend_client_does_not_exist_without_backend_begin(rc: RawConfig) -> None:
    table = """
    d | r | b | m | property | type   | source                             
    example                  |        |                                    
      | wsdl_resource        | wsdl   | tests/datasets/backends/wsdl/data/wsdl.xml   
    """
    context, manifest = load_manifest_and_context(rc, table, mode=Mode.external)
    dataset = commands.get_dataset(context, manifest, "example")
    backend = dataset.resources["wsdl_resource"].backend

    assert not hasattr(backend, "client")


def test_raise_error_if_wsdl_source_incorrect(rc: RawConfig, tmp_path: Path) -> None:
    incorrect_wsdl = Path(tmp_path / "incorrect_wsdl.xml")
    incorrect_wsdl.write_text("")
    table = f"""
    d | r | b | m | property | type   | source                             
    example                  |        |                                    
      | wsdl_resource        | wsdl   | {incorrect_wsdl}   
    """
    context, manifest = load_manifest_and_context(rc, table, mode=Mode.external)
    dataset = commands.get_dataset(context, manifest, "example")
    backend = dataset.resources["wsdl_resource"].backend

    with pytest.raises(WsdlClientError):
        with backend.begin():
            assert backend.client is None


def test_backend_client_initiated_if_wsdl_source_is_correct(rc: RawConfig) -> None:
    table = """
    d | r | b | m | property | type   | source                               
    example                  |        |                                      
      | wsdl_resource        | wsdl   | tests/datasets/backends/wsdl/data/wsdl.xml
    """
    context, manifest = load_manifest_and_context(rc, table, mode=Mode.external)
    dataset = commands.get_dataset(context, manifest, "example")
    backend = dataset.resources["wsdl_resource"].backend

    with backend.begin():
        assert type(backend.client) is Client
