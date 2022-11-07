from operator import attrgetter
from typing import Iterator
from typing import Tuple

from zeep import Client
from zeep import xsd

from spinta.manifests.components import ManifestSchema
from spinta.manifests.components import NodeSchema
from spinta.utils.naming import Deduplicator
from spinta.utils.naming import to_model_name
from spinta.utils.naming import to_property_name


def read_wsdl(path: str) -> Iterator[ManifestSchema]:
    dedup = Deduplicator('{}')
    client = Client(path)

    for service in client.wsdl.services.values():
        for port in service.ports.values():
            operations = sorted(
                port.binding._operations.values(),
                key=attrgetter("name"),
            )
            for op in operations:
                if (
                    isinstance(op.output.body.type, xsd.ComplexType) and
                    len(op.output.body.type.elements) == 1
                ):
                    # Skip body element.
                    _, el = op.output.body.type.elements[0]
                    props = dict(read_props(el))
                else:
                    props = dict(read_props(op.output.body))

                name = to_model_name(op.name)
                name = dedup(name)

                yield None, {
                    'type': 'model',
                    'name': name,
                    'external': {
                        'name': f'{service.name}.{port.name}.{op.name}',
                    },
                    'properties': props,
                }


XSD_TYPES = {
    'float': 'number',
    'decimal': 'number',
    'int': 'integer',
    'string': 'string',
}


def read_props(
    elem: xsd.Element,
    path: str = '',
) -> Iterator[Tuple[str, NodeSchema]]:
    if not elem:
        return

    if isinstance(elem.type, xsd.ComplexType):
        dedup = Deduplicator('_{}')
        for name, el in elem.type.elements:
            name = to_property_name(name)
            name = dedup(name)
            if path:
                el_path = f'{path}.{name}'
            else:
                el_path = name

            yield from read_props(el, el_path)
    else:
        yield path, {
            'type': XSD_TYPES[elem.type.name],
            'external': {
                'name': elem.qname,
            },
        }
