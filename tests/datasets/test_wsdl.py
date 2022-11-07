from spinta.core.config import RawConfig
from spinta.testing.manifest import load_manifest


def test_wsdl(rc: RawConfig):
    manifest = load_manifest(rc, 'tests/datasets/soap/service.wsdl')
    assert manifest == '''
    m | property       | type   | ref | source
    GetLastTradePrice  |        |     | StockQuoteService.StockQuotePort.GetLastTradePrice
      | price          | number |     | price
    '''
