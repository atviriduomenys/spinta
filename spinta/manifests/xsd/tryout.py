from spinta.components import Context
from spinta.manifests.xsd.helpers import read_schema

# result = read_schema(context=Context("xsd"), path="https://ws.registrucentras.lt/broker/info.php?t=17&f=out")

# result = read_schema(context=Context("xsd"), path="https://ws.registrucentras.lt/broker/info.php?t=40&f=out")

# result = read_schema(context=Context("xsd"), path="https://ws.registrucentras.lt/broker/info.php?t=717&f=out")

result = read_schema(context=Context("xsd"), path="C:\\Users\karina.klinkeviciute\Projects\spinta\example.xml")
