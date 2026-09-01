import pytest

from spinta.manifests.components import ManifestPath
from spinta.manifests.tabular.helpers import striptable
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
   |   |   |   |   | unit_kind            | string                |     |        |         | 4     | open   | Processing unit kind                                       |
   |                                      | enum                  |     | A      |         |       |        | Kind A                                                     |
   |                                      |                       |     | B      |         |       |        | Kind B                                                     |
   |   |   |   |   | efficiency_rate      | number                |     |        |         | 3     | open   | Processing efficiency rate percentage                      |
   |   |   |   |   | capacity             | integer               |     |        |         | 3     | open   | Processing capacity, units per day                         |
   |   |   |   |   | technical_specs      | file                  |     |        |         | 3     | open   | Technical specifications document                          |
    """)


MANIFEST_WITH_REFS = striptable("""
id | d | r | b | m | property         | type     | ref                                        | level | access | title          | description
   | datasets/gov/giscenter/grpk      |          |                                            |       |        | GRPK           | GRPK dataset
   |   | test                         | memory   |                                            |       |        |                |
   |   |   |   | Area                 |          | top_id, info                               |       |        |                |
   |   |   |   |   | top_id           | string   |                                            | 4     | open   |                |
   |   |   |   |   | info             | ref      | datasets/gov/cemetery/Territory            | 4     | open   |                |
   |   |   |   |   | area             | number   |                                            | 4     | open   |                |
   |                                  |          |                                            |       |        |                |
   | datasets/gov/vssa/demo           |          |                                            |       |        | Demo           | Demo dataset
   |   | test                         | memory   |                                            |       |        |                |
   |   |   |   | Municipality         |          | id, area                                   |       |        |                |
   |   |   |   |   | id               | integer  |                                            | 4     | open   |                |
   |   |   |   |   | area             | ref      |  datasets/gov/giscenter/grpk/Area          | 4     | open   |                |
   |   |   |   |   | name             | string   |                                            | 4     | open   |                |
   |                                  |          |                                            |       |        |                |
   |   |   |   | County               |          | id, title                                  |       |        |                |
   |   |   |   |   | id               | integer  |                                            | 4     | open   |                |
   |   |   |   |   | title            | string   |                                            | 4     | open   |                |
   |   |   |   |   | population       | integer  |                                            | 4     | open   |                |
   |                                  |          |                                            |       |        |                |
   | datasets/gov/cemetery            |          |                                            |       |        | Cemetery        | Cemetery dataset
   |   | test                         | memory   |                                            |       |        |                |
   |   |   |   | Territory            |          | vda_id                                     |       |        |                |
   |   |   |   |   | vda_id           | string   |                                            | 4     | open   |                |
   |   |   |   |   | cemetery         | string   |                                            | 4     | open   |                |
   |   |   |   |   | city             | ref      | datasets/gov/vssa/demo/Municipality        | 4     | open   |                |
   |   |   |   |   | region           | ref      | datasets/gov/vssa/demo/County              | 4     | open   |                |
   |   |   |   |   | geometry         | ref      | datasets/gov/giscenter/grpk/Area           | 4     | open   |                |
    """)


#: One agent serving several information systems, as in the Registrų centras
#: case: two datasets under one data service, models of the same name in both,
#: an adjacent service version, a second information system and a `ref` to a
#: dataset that is not in the manifest.
MANIFEST_WITH_SERVICES = striptable("""
id | d | r | b | m | property   | type            | ref                                                 | level | access | title           | description
   | datasets/gov/rc/jadis/at280/1/at280_israsas |  |                                                   |       |        | AT280 išrašas   | Išrašo duomenys
   |   | test                   | memory          |                                                     |       |        |                 |
   |   |   |   | DalyvioAsmensIsrasas |           | kodas                                               |       |        |                 |
   |   |   |   |   | kodas      | string required |                                                     | 4     | open   |                 |
   |   |   |   |   | adresas    | ref             | datasets/gov/rc/jadis/at280/1/at280_adresai/Adresas | 4     | open   |                 |
   |   |   |   | Adresas        |                 | kodas                                               |       |        |                 |
   |   |   |   |   | kodas      | string required |                                                     | 4     | open   |                 |
   |                            |                 |                                                     |       |        |                 |
   | datasets/gov/rc/jadis/at280/1/at280_adresai |  |                                                   |       |        | AT280 adresai   | Adresų duomenys
   |   | test                   | memory          |                                                     |       |        |                 |
   |   |   |   | Adresas        |                 | id                                                  |       |        |                 |
   |   |   |   |   | id         | string required |                                                     | 4     | open   |                 |
   |   |   |   |   | gatve      | string          |                                                     | 4     | open   |                 |
   |                            |                 |                                                     |       |        |                 |
   | datasets/gov/rc/jadis/at280/10/at280_kitas |   |                                                   |       |        | AT280 v10       |
   |   | test                   | memory          |                                                     |       |        |                 |
   |   |   |   | Adresas        |                 | id                                                  |       |        |                 |
   |   |   |   |   | id         | string required |                                                     | 4     | open   |                 |
   |                            |                 |                                                     |       |        |                 |
   | datasets/gov/rc/ntr/n249/1/n249_israsas |     |                                                     |       |        | N249 išrašas    |
   |   | test                   | memory          |                                                     |       |        |                 |
   |   |   |   | Israsas        |                 | nr                                                  |       |        |                 |
   |   |   |   |   | nr         | string required |                                                     | 4     | open   |                 |
   |   |   |   |   | vieta      | ref             | datasets/gov/rc/ar/nesantis/Vieta                   | 4     | open   |                 |
   |   |   |   |   | adresas    | ref             | datasets/gov/rc/jadis/at280/1/at280_adresai/Adresas | 4     | open   |                 |
   |   |   |   |   | adresas2   | ref             | datasets/gov/rc/jadis/at280/1/at280_adresai/Adresas | 4     | open   |                 |
   |   |   |   |   | zemelapis  | image           |                                                     | 4     | open   |                 |
""")


#: Two dataset paths of one service that map to one schema name when path
#: separators are replaced with underscores.
MANIFEST_WITH_COLLIDING_DATASETS = striptable("""
id | d | r | b | m | property | type            | ref | level | access
   | datasets/gov/rc/jadis/at280/1/a_b |        |     |       |
   |   | test                 | memory          |     |       |
   |   |   |   | C            |                 | x   |       |
   |   |   |   |   | x        | string required |     | 4     | open
   |                          |                 |     |       |
   | datasets/gov/rc/jadis/at280/1/a/b |        |     |       |
   |   | test                 | memory          |     |       |
   |   |   |   | C            |                 | y   |       |
   |   |   |   |   | y        | string required |     | 4     | open
""")


#: A model named `XCollection` next to a model named `X`, whose collection
#: schema takes that same name.
MANIFEST_WITH_COLLIDING_MODELS = striptable("""
id | d | r | b | m | property | type            | ref | level | access
   | datasets/gov/rc/jadis/at280/1/ds |        |     |       |
   |   | test                 | memory          |     |       |
   |   |   |   | Data         |                 | x   |       |
   |   |   |   |   | x        | string required |     | 4     | open
   |   |   |   | DataCollection |               | y   |       |
   |   |   |   |   | y        | string required |     | 4     | open
""")


#: Two datasets outside the exported service, whose paths map to one schema
#: name, referenced from a model of the service.
MANIFEST_WITH_COLLIDING_EXTERNAL_REFS = striptable("""
id | d | r | b | m | property | type            | ref                           | level | access
   | datasets/gov/rc/x/ext/1/a_b |            |                               |       |
   |   | test                 | memory          |                               |       |
   |   |   |   | C            |                 | p                             |       |
   |   |   |   |   | p        | string required |                               | 4     | open
   |                          |                 |                               |       |
   | datasets/gov/rc/x/ext/1/a/b |            |                               |       |
   |   | test                 | memory          |                               |       |
   |   |   |   | C            |                 | q                             |       |
   |   |   |   |   | q        | string required |                               | 4     | open
   |                          |                 |                               |       |
   | datasets/gov/rc/jadis/at280/1/ds |        |                               |       |
   |   | test                 | memory          |                               |       |
   |   |   |   | Israsas      |                 | nr                            |       |
   |   |   |   |   | nr       | string required |                               | 4     | open
   |   |   |   |   | first    | ref             | datasets/gov/rc/x/ext/1/a_b/C | 4     | open
   |   |   |   |   | second   | ref             | datasets/gov/rc/x/ext/1/a/b/C | 4     | open
""")


#: Models whose names, concatenated with their file property names, build one
#: operation id: `A` + `bc` and `Ab` + `c`.
MANIFEST_WITH_COLLIDING_OPERATION_IDS = striptable("""
id | d | r | b | m | property | type            | ref | level | access
   | datasets/gov/rc/jadis/at280/1/ds |         |     |       |
   |   | test                 | memory          |     |       |
   |   |   |   | A            |                 | x   |       |
   |   |   |   |   | x        | string required |     | 4     | open
   |   |   |   |   | bc       | file            |     | 4     | open
   |   |   |   | Ab           |                 | y   |       |
   |   |   |   |   | y        | string required |     | 4     | open
   |   |   |   |   | c        | file            |     | 4     | open
""")


#: One model referenced from two datasets of the service at different levels,
#: which the reference carries differently: an `_id` or the natural key.
MANIFEST_WITH_REF_SHAPES = striptable("""
id | d | r | b | m | property | type            | ref                               | level | access
   | datasets/gov/rc/x/ext/1/ext |           |                                   |       |
   |   | test                 | memory          |                                   |       |
   |   |   |   | Vieta        |                 | kodas                             |       |
   |   |   |   |   | kodas    | string required |                                   | 4     | open
   |                          |                 |                                   |       |
   | datasets/gov/rc/jadis/at280/1/pirmas |   |                                   |       |
   |   | test                 | memory          |                                   |       |
   |   |   |   | A            |                 | id                                |       |
   |   |   |   |   | id       | string required |                                   | 4     | open
   |   |   |   |   | vieta    | ref             | datasets/gov/rc/x/ext/1/ext/Vieta | 4     | open
   |                          |                 |                                   |       |
   | datasets/gov/rc/jadis/at280/1/antras |   |                                   |       |
   |   | test                 | memory          |                                   |       |
   |   |   |   | B            |                 | id                                |       |
   |   |   |   |   | id       | string required |                                   | 4     | open
   |   |   |   |   | vieta    | ref             | datasets/gov/rc/x/ext/1/ext/Vieta | 3     | open
""")


#: An array of references, where the item property carries the level, and the
#: array property carries one of its own.
MANIFEST_WITH_ARRAY_REFS = striptable("""
id | d | r | b | m | property   | type            | ref                               | level | access
   | datasets/gov/rc/x/ext/1/ext |             |                                   |       |
   |   | test                   | memory          |                                   |       |
   |   |   |   | Kalba          |                 | kodas                             |       |
   |   |   |   |   | kodas      | string required |                                   | 4     | open
   |                            |                 |                                   |       |
   | datasets/gov/rc/jadis/at280/1/ds |         |                                   |       |
   |   | test                   | memory          |                                   |       |
   |   |   |   | Israsas        |                 | id                                |       |
   |   |   |   |   | id         | string required |                                   | 4     | open
   |   |   |   |   | kalbos     | array           |                                   | 4     | open
   |   |   |   |   | kalbos[]   | ref             | datasets/gov/rc/x/ext/1/ext/Kalba | 3     | open
""")


#: A level 3 reference, whose natural key holds a level 4 reference of its own.
MANIFEST_WITH_NESTED_REF_LEVELS = striptable("""
id | d | r | b | m | property | type            | ref                           | level | access
   | datasets/gov/rc/x/ext/1/ext |           |                               |       |
   |   | test                 | memory          |                               |       |
   |   |   |   | C            |                 | kodas                         |       |
   |   |   |   |   | kodas    | string required |                               | 4     | open
   |   |   |   | B            |                 | cref                          |       |
   |   |   |   |   | cref     | ref             | datasets/gov/rc/x/ext/1/ext/C | 4     | open
   |   |   |   |   | pav      | string          |                               | 4     | open
   |                          |                 |                               |       |
   | datasets/gov/rc/jadis/at280/1/ds |       |                               |       |
   |   | test                 | memory          |                               |       |
   |   |   |   | A            |                 | id                            |       |
   |   |   |   |   | id       | string required |                               | 4     | open
   |   |   |   |   | bref     | ref             | datasets/gov/rc/x/ext/1/ext/B | 3     | open
""")


#: An array whose relation goes through an intermediate table, which the array
#: holds in `model`, the same attribute a reference holds its target in.
MANIFEST_WITH_INTERMEDIATE_TABLE = striptable("""
id | d | r | b | m | property | type            | ref                                          | level | access
   | datasets/gov/rc/jadis/at280/1/ds |       |                                              |       |
   |   | test                 | memory          |                                              |       |
   |   |   |   | Kalba        |                 | kodas                                        |       |
   |   |   |   |   | kodas    | string required |                                              | 4     | open
   |   |   |   | Israsas      |                 | id                                           |       |
   |   |   |   |   | id       | string required |                                              | 4     | open
   |   |   |   |   | kalbos   | array           | datasets/gov/rc/jadis/at280/1/ds/IsrasoKalba | 4     | open
   |   |   |   |   | kalbos[] | ref             | datasets/gov/rc/jadis/at280/1/ds/Kalba       | 4     | open
   |   |   |   | IsrasoKalba  |                 | israsas, kalba                               |       |
   |   |   |   |   | israsas  | ref             | datasets/gov/rc/jadis/at280/1/ds/Israsas     | 4     | open
   |   |   |   |   | kalba    | ref             | datasets/gov/rc/jadis/at280/1/ds/Kalba       | 4     | open
""")


#: A dynamic array, which declares no item property, and an array of arrays.
MANIFEST_WITH_ARRAY_LAYERS = striptable("""
id | d | r | b | m | property   | type            | ref                               | level | access
   | datasets/gov/rc/x/ext/1/ext |             |                                   |       |
   |   | test                   | memory          |                                   |       |
   |   |   |   | Kalba          |                 | kodas                             |       |
   |   |   |   |   | kodas      | string required |                                   | 4     | open
   |                            |                 |                                   |       |
   | datasets/gov/rc/jadis/at280/1/ds |         |                                   |       |
   |   | test                   | memory          |                                   |       |
   |   |   |   | Israsas        |                 | id                                |       |
   |   |   |   |   | id         | string required |                                   | 4     | open
   |   |   |   |   | zymos      | array           |                                   | 4     | open
   |   |   |   |   | kalbos     | array           |                                   | 4     | open
   |   |   |   |   | kalbos[]   | array           |                                   | 4     | open
   |   |   |   |   | kalbos[][] | ref             | datasets/gov/rc/x/ext/1/ext/Kalba | 4     | open
""")

#: An array standing among the reference properties of a reference.
MANIFEST_WITH_ARRAY_IN_REFERENCE = striptable("""
id | d | r | b | m | property | type            | ref                                   | level | access
   | datasets/gov/rc/x/ext/1/ext |           |                                       |       |
   |   | test                 | memory          |                                       |       |
   |   |   |   | Kalba        |                 | kodas                                 |       |
   |   |   |   |   | kodas    | string required |                                       | 4     | open
   |   |   |   | B            |                 | kalbos                                |       |
   |   |   |   |   | kalbos   | array           |                                       | 4     | open
   |   |   |   |   | kalbos[] | ref             | datasets/gov/rc/x/ext/1/ext/Kalba     | 4     | open
   |                          |                 |                                       |       |
   | datasets/gov/rc/jadis/at280/1/ds |       |                                       |       |
   |   | test                 | memory          |                                       |       |
   |   |   |   | A            |                 | id                                    |       |
   |   |   |   |   | id       | string required |                                       | 4     | open
   |   |   |   |   | bref     | ref             | datasets/gov/rc/x/ext/1/ext/B[kalbos] | 3     | open
""")


# Enum values real data services hold: `0`, which is a value and not a missing
# one, and a formula, which is not a value at all.
MANIFEST_WITH_ENUM_VALUES = striptable("""
id | d | r | b | m | property   | type            | source | prepare | level | access
   | datasets/gov/rc/jadis/at280/1/ds |          |        |         |       |
   |   | test                   | memory          |        |         |       |
   |   |   |   | Testamentas    |                 |        |         |       |
   |   |   |   |   | id         | string required |        |         | 4     | open
   |   |   |   |   | sudaryta   | integer         |        |         | 4     | open
   |                            | enum            |        | 1       |       |
   |                            |                 |        | 0       |       |
   |   |   |   |   | rusis      | integer         |        |         | 4     | open
   |                            | enum            |        | noop()  |       |
   |   |   |   |   | zyma       | string          |        |         | 4     | open
   |                            | enum            |        | ''      |       |
   |                            |                 | V      |         |       |
""")

# Names that hold characters an OpenAPI component name may not, as the data
# service template in the metadata repository does.
MANIFEST_WITH_UNNAMABLE_NAMES = striptable("""
id | d | r | b | m | property | type            | level | access
   | datasets/gov/rc/jadis/at280/1/(duom_rink) |   |     |
   |   | test                 | memory          |       |
   |   |   |   | Esybe        |                 |       |
   |   |   |   |   | kodas    | string required | 4     | open
""")


# A model declaring `_id` of its own, whose identifiers are the keys the data
# holds, `AE` of a country for one, and not UUIDs.
MANIFEST_WITH_DECLARED_ID = striptable("""
id | d | r | b | m | property | type            | ref   | source  | level | access
   | datasets/gov/rc/jadis/at280/1/ds |         |       |         |       |
   |   | test                 | memory          |       |         |       |
   |   |   |   | Salis        |                 | kodas | salys   |       |
   |   |   |   |   | _id      | string          |       |         |       | open
   |   |   |   |   | kodas    | string required |       | kodas   | 4     | open
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


@pytest.fixture
def manifest_with_services():
    return MANIFEST_WITH_SERVICES
