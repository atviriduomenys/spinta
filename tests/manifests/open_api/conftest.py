import pytest

from spinta.manifests.tabular.helpers import striptable
from spinta.manifests.components import ManifestPath
from spinta.testing.context import create_test_context
from spinta.testing.tabular import create_tabular_manifest


MANIFEST_WITH_SOAP_PREPARE = striptable("""
id | d | r | b | m | property           | type     | ref              | source                                                 | source.type | prepare                 | origin | count | level | status | visibility | access | uri | eli | title | description
   | datasets/gov/vssa/demo/rctest      |          |                  |                                                        |             |                         |        |       |       |        |            |        |     |     |       | D
   |   | rc_wsdl                        | wsdl     |                  | https://test-data.data.gov.lt/api/v1/rc/get-data/?wsdl |             |                         |        |       |       |        |            |        |     |     |       |
   |   | get_data                       | soap     |                  | Get.GetPort.GetPort.GetData                            |             | wsdl(rc_wsdl)           |        |       |       |        |            |        |     |     |       |
   |                                    | param    | action_type      | input/ActionType                                       |             | input()                 |        |       |       |        |            |        |     |     |       |
   |                                    | param    | caller_code      | input/CallerCode                                       |             | input()                 |        |       |       |        |            |        |     |     |       |
   |                                    | param    | end_user_info    | input/EndUserInfo                                      |             | input()                 |        |       |       |        |            |        |     |     |       |
   |                                    | param    | parameters       | input/Parameters                                       |             | input()                 |        |       |       |        |            |        |     |     |       |
   |                                    | param    | time             | input/Time                                             |             | input()                 |        |       |       |        |            |        |     |     |       |
   |                                    | param    | signature        | input/Signature                                        |             | input()                 |        |       |       |        |            |        |     |     |       |
   |                                    | param    | caller_signature | input/CallerSignature                                  |             | input()                 |        |       |       |        |            |        |     |     |       |
   |                                    |          |                  |                                                        |             |                         |        |       |       |        |            |        |     |     |       |
   |   |   |   | GetData                |          |                  | /                                                      |             |                         |        |       |       |        |            | open   |     |     |       |
   |   |   |   |   | response_code      | string   |                  | ResponseCode                                           |             |                         |        |       |       |        |            |        |     |     |       |
   |   |   |   |   | response_data      | string   |                  | ResponseData                                           |             | base64()                |        |       |       |        |            |        |     |     |       |
   |   |   |   |   | decoded_parameters | string   |                  | DecodedParameters                                      |             |                         |        |       |       |        |            |        |     |     |       |
   |   |   |   |   | action_type        | string   |                  |                                                        |             | param(action_type)      |        |       |       |        |            |        |     |     |       |
   |   |   |   |   | end_user_info      | string   |                  |                                                        |             | param(end_user_info)    |        |       |       |        |            |        |     |     |       |
   |   |   |   |   | caller_code        | string   |                  |                                                        |             | param(caller_code)      |        |       |       |        |            |        |     |     |       |
   |   |   |   |   | parameters         | string   |                  |                                                        |             | param(parameters)       |        |       |       |        |            |        |     |     |       |
   |   |   |   |   | time               | string   |                  |                                                        |             | param(time)             |        |       |       |        |            |        |     |     |       |
   |   |   |   |   | signature          | string   |                  |                                                        |             | param(signature)        |        |       |       |        |            |        |     |     |       |
   |   |   |   |   | caller_signature   | string   |                  |                                                        |             | param(caller_signature) |        |       |       |        |            |        |     |     |       |

""")

MANIFEST = striptable("""
id | d | r | b | m | property             | type                  | ref | source | prepare | level | access | title                                                      | description
   | datasets/demo/system_data            |                       |     |        |         |       |        | Test title                                                 | Test description
   |   | test                             | memory                |     |        |         |       |        |                                                            |
   |   | datasets/demo/demo/test_resource |                       |     | test   |         |       |        |                                                            |
   |                                      |                       |     |        |         |       |        |                                                            |
   |   |   |   | Organization             |                       |     |        |         | 2     |        | Reporting Organizations                                    |
   |   |   |   |   | org_name             | string                |     |        |         | 2     | open   | Organization name                                          |
   |   |   |   |   | annual_revenue       | number                |     |        |         | 3     | open   | Annual revenue amount                                      |
   |   |   |   |   | coordinates          | geometry(point, 3346) |     |        |         | 2     | open   | Organization coordinates                                   |
   |   |   |   |   | established_date     | date                  | D   |        |         | 4     | open   | Organization establishment date                            |
   |   |   |   |   | org_logo             | image                 |     |        |         | 2     | open   | Organization logo image                                    |
   |                                      |                       |     |        |         |       |        |                                                            |
   |   |   |   | ProcessingUnit           |                       |     |        |         | 2     |        | Processing unit data with treatment methods and capacities |
   |   |   |   |   | unit_name            | string                |     |        |         | 3     | open   | Processing unit name                                       |
   |   |   |   |   | unit_type            | string                |     |        |         | 4     | open   | Processing unit type                                       |
   |                                      | enum                  |     |        | 'FAC'   |       |        | Processing Facility                                        |
   |                                      |                       |     |        | 'TRT'   |       |        | Treatment Plant                                            |
   |                                      |                       |     |        | 'OUT'   |       |        | Outlet Point                                               |
   |                                      |                       |     |        | 'OTH'   |       |        | Other Equipment                                            |
   |   |   |   |   | unit_version         | integer               |     |        |         | 4     | open   | Processing unit version                                    |
   |                                      | enum                  |     |        | 1       |       |        | Version v1                                                 |
   |                                      |                       |     |        | 2       |       |        | Version v2                                                 |
   |   |   |   |   | efficiency_rate      | number                |     |        |         | 3     | open   | Processing efficiency rate percentage                      |
   |   |   |   |   | capacity             | integer               |     |        |         | 3     | open   | Processing capacity, units per day                         |
   |   |   |   |   | technical_specs      | file                  |     |        |         | 3     | open   | Technical specifications document                          |
    """)


@pytest.fixture
def manifest():
    return MANIFEST


@pytest.fixture
def manifest_with_soap_prepare():
    return MANIFEST_WITH_SOAP_PREPARE


@pytest.fixture
def open_manifest_path(tmp_path, rc):
    path = f"{tmp_path}/manifest.csv"
    context = create_test_context(rc)
    create_tabular_manifest(
        context,
        path,
        MANIFEST,
    )
    file_handle = open(path, "r")
    yield ManifestPath(type="tabular", name="test_manifest", path=None, file=file_handle, prepare=None)
    file_handle.close()


@pytest.fixture
def open_manifest_path_factory(tmp_path, rc):
    """Factory fixture that creates manifest paths with custom MANIFEST data"""
    opened_files = []

    def _create_manifest(manifest_data):
        path = f"{tmp_path}/manifest_{len(opened_files)}.csv"
        context = create_test_context(rc)
        create_tabular_manifest(
            context,
            path,
            manifest_data,
        )
        file_handle = open(path, "r")
        opened_files.append(file_handle)
        return ManifestPath(type="tabular", name="test_manifest", path=None, file=file_handle, prepare=None)

    yield _create_manifest

    for file_handle in opened_files:
        file_handle.close()
