import enum
import itertools
from typing import Any, Tuple, Dict, List

from rdflib import Graph
from rdflib.query import Result
from rdflib.term import URIRef

from spinta.manifests.rdf.components import RdfManifest
from spinta.manifests.tabular.helpers import get_relative_model_name
from spinta.utils.naming import to_model_name, to_property_name

QUERY = '''
    select distinct
        ?schema
        ?base
        ?model
        ?property
        ?type
        ?title
        ?description
        ?order
    where {
        {
            ?model_shape a sh:NodeShape ;
                sh:targetClass ?model ;
                sh:property ?shape .

            ?shape sh:path ?property .

            optional { ?shape sh:datatype ?type }
            optional { ?shape sh:property ?type }
            optional { ?shape sh:class ?type }
            optional { ?shape sh:name ?title }
            optional { ?shape sh:description ?description }
            optional { ?shape sh:order ?order }

            bind("shacl" as ?schema)
        }

        union

        {
            ?shape a sh:NodeShape ;
                sh:targetClass ?model .

            optional { ?shape sh:name ?title }
            optional { ?shape sh:description ?description }

            bind("shacl" as ?schema)
        }

        union

        {
            ?property a rdf:Property .
            optional { ?property rdfs:domain ?model }
            optional { ?property rdfs:range ?type }
            optional { ?property rdfs:label ?title filter langMatches(lang(?title), "EN") }
            optional { ?property rdfs:comment ?description filter langMatches(lang(?description), "EN") }
            bind("rdfs" as ?schema)
        }

        union

        {
            ?property a rdf:Property .
            optional { ?property rdfs:isDefinedBy ?description }
            bind("rdfs" as ?schema)
        }
        
        union
        {
            ?property a owl:ObjectProperty .
            optional { ?property rdfs:label ?title }
            optional { ?property rdfs:comment ?description }
            optional { ?property rdfs:domain ?type }
            bind("owl" as ?schema)
        }

        union

        {
            ?property rdfs:domain ?model .
            optional { ?property rdfs:range ?type }
            optional { ?property rdfs:label ?title filter langMatches(lang(?title), "EN") }
            optional { ?property rdfs:comment ?description filter langMatches(lang(?description), "EN") }
            bind("rdfs" as ?schema)
        }

        union

        {
            ?property rdfs:domain ?model .
            optional { ?property rdfs:isDefinedBy ?description }
            bind("rdfs" as ?schema)
        }

        union

        {
            ?property rdfs:range ?type .
            optional { ?property rdfs:domain ?model }
            optional { ?property rdfs:label ?title filter langMatches(lang(?title), "EN") }
            optional { ?property rdfs:comment ?description filter langMatches(lang(?description), "EN") }
            bind("rdfs" as ?schema)
        }

        union

        {
            ?property rdfs:range ?type .
            optional { ?property rdfs:isDefinedBy ?description }
            bind("rdfs" as ?schema)
        }

        union

        {
            ?model a rdfs:Class .
            optional { ?model rdfs:label ?title filter langMatches(lang(?title), "EN") }
            optional { ?model rdfs:comment ?description filter langMatches(lang(?description), "EN") }
            bind("rdfs" as ?schema)
        }

        union

        {
            ?model a rdfs:Class .
            optional { ?model rdfs:isDefinedBy ?description }
            bind("rdfs" as ?schema)
        }
        
        union
        
        {
            ?model a owl:Class .
            optional { ?model rdfs:label ?title }
            optional { ?model rdfs:comment ?description }
            bind("owl" as ?schema)
        }

        union

        {
            ?p rdfs:domain ?model .
            optional { ?model rdfs:label ?title filter langMatches(lang(?title), "EN") }
            optional { ?model rdfs:comment ?description filter langMatches(lang(?description), "EN") }
            bind("rdfs" as ?schema)
        }

        union

        {
            ?p rdfs:domain ?model .
            optional { ?model rdfs:isDefinedBy ?description }
            bind("rdfs" as ?schema)
        }

        union

        {
            ?model rdfs:subClassOf ?base .
            bind("rdfs" as ?schema)
        }

    }
    '''


class Schema(enum.IntEnum):
    undefined = 0
    rdfs = 1
    owl = 2
    shacl = 3


def read_rdf_manifest(
    manifest: RdfManifest,
    dataset_name: str,
    lang: str = 'en',
):
    g = Graph(bind_namespaces='rdflib')
    g.parse(manifest.path, format=manifest.format)

    schemas = []
    dataset = {
        'type': 'dataset',
        'name': dataset_name if dataset_name else manifest.path.split('/')[-1].rsplit('.', 1)[0],
        'prefixes': {},
        'given_name': dataset_name
    }
    for i, (name, url) in enumerate(g.namespaces()):
        dataset['prefixes'][name] = {
            'type': 'prefix',
            'eid': i,
            'name': name,
            'uri': url,
        }
    schemas.append(dataset)

    models = _prepare_data(g, g.query(QUERY), dataset, lang)
    schemas.extend(models.values())

    yield from _get_schemas(
        g,
        schemas,
        dataset,
        list(models.keys())
    )


def _get_schemas(
    graph: Graph,
    schemas: List[Dict],
    dataset: Dict,
    models: List,
):
    for i, row in enumerate(schemas, 1):

        if 'schema' in row:
            row.pop('schema')

        if 'description' in row:
            row['description'] = '\n'.join(sorted(set(row['description'])))

        if 'properties' in row:
            for prop in row['properties'].values():
                if 'schema' in prop:
                    prop.pop('schema')

                prop['description'] = '\n'.join(sorted(set(prop['description'])))
                prop['type'], ref_model = _to_type_name(graph, dataset, models, prop['type'])
                if ref_model:
                    prop['model'] = ref_model

        yield i, row


def _prepare_data(
    graph: Graph,
    query: Result,
    dataset: Dict,
    lang: str = 'en'
) -> Dict:
    data = {}
    for (model, prop), rows in itertools.groupby(query, key=_get_key):
        dim = _get_dimension(model, prop)

        for row in rows:
            if dim == 'model':
                _read_model(graph, dataset, data, row, lang)

            elif dim == 'property':
                _read_property(graph, dataset, data, row, lang)

    return data


def _read_model(
    graph: Graph,
    dataset: Dict,
    data: Dict,
    row: Dict,
    lang: str = 'en'
) -> None:
    schema = _load_schema(str(row['schema']))
    model, uri = _to_model_name(graph, dataset, row['model'])
    base, _ = _to_model_name(graph, dataset, row['base'])
    title = _get_value_for_language(row['title'], lang)
    properties = {}
    descriptions = []

    if description := _get_value_for_language(row['description'], lang):
        descriptions.append(description)

    if model in data:
        existing_model = data[model]
        properties = existing_model['properties']
        title = title or existing_model['title']
        descriptions.extend(existing_model['description'])
        if not base and existing_model['base']:
            base = existing_model['base']['parent']

        if data[model]['schema'] > schema:
            schema = existing_model['schema']
            base = existing_model['base']['parent'] if existing_model['base'] else base
            title = existing_model['title'] or title

    if base and base not in data:
        # Add temporary model for base
        data.update({
            base: {
                'type': 'model',
                'schema': Schema.undefined,
                'name': base,
                'base': None,
                'properties': {},
                'uri': "",
                'access': 'open',
                'description': [],
                'title': '',
                'external': {
                    'dataset': dataset['name'],
                },
            }
        })

    data.update({
        model: {
            'type': 'model',
            'schema': schema,
            'name': model,
            'base': {
                'name': base.split('/')[-1],
                'parent': base,
            } if base else None,
            'title': title,
            'description': descriptions,
            'properties': properties,
            'uri': uri,
            'access': 'open',
            'external': {
                'dataset': dataset['name'],
            },
        }
    })


def _read_property(
    graph: Graph,
    dataset: Dict,
    data: Dict,
    row: Dict,
    lang: str = 'en'
) -> None:
    schema = _load_schema(str(row['schema']))
    model, model_uri = _to_model_name(graph, dataset, row['model'] or 'rdfs:Resource')
    prop, prop_uri = _to_property_name(graph, row['property'])
    type = _parse_uri(graph, row['type'])
    title = _get_value_for_language(row['title'], lang)
    descriptions = []

    if description := _get_value_for_language(row['description'], lang):
        descriptions.append(description)

    if model not in data:
        # Add temporary model for property
        data.update({
            model: {
                'type': 'model',
                'schema': Schema.undefined,
                'name': model,
                'base': None,
                'properties': {},
                'uri': model_uri,
                'access': 'open',
                'description': [],
                'title': '',
                'external': {
                    'dataset': dataset['name'],
                },
            }
        })

    model = data[model]

    if prop in model['properties']:
        existing_prop = model['properties'][prop]
        title = title or existing_prop['title']
        type = type or existing_prop['type']
        descriptions.extend(existing_prop['description'])

        if model['properties'][prop]['schema'] > schema:
            schema = existing_prop['schema']
            type = existing_prop['type'] or type
            title = existing_prop['title'] or title

    model['properties'].update({
        prop: {
            'schema': schema,
            'type': type,
            'title': title,
            'description': descriptions,
            'uri': prop_uri,
            'access': 'open',
        }
    })


def _get_key(row: Dict) -> Tuple[str, str]:
    return row['model'], row['property']


def _get_dimension(model: str, prop: str) -> str:
    if model and not prop:
        return 'model'
    elif prop:
        return 'property'
    return ''


def _load_schema(schema: str) -> Schema:
    if schema == 'rdfs':
        return Schema.rdfs
    elif schema == 'owl':
        return Schema.owl
    elif schema == 'shacl':
        return Schema.shacl
    else:
        return Schema.undefined


def _to_model_name(
    graph: Graph,
    dataset: Dict,
    name: str
) -> Tuple[str, str]:
    name = _parse_uri(graph, name)
    uri = ''
    if name:
        if ':' in name:
            uri = name
            name = to_model_name(name.split(':', 1)[1])
        name = get_relative_model_name(dataset, name)
    return name, uri


def _to_property_name(graph: Graph, name: str) -> Tuple[str, str]:
    name = _parse_uri(graph, name)
    uri = ''
    if name and ':' in name:
        uri = name
        name = to_property_name(name.split(':', 1)[1])
    return name, uri


TYPES = {
    'xsd:hexBinary': 'binary',
    'xsd:duration': 'string',
    'xsd:decimal': 'number',
    'xsd:dateTime': 'datetime',
    'xsd:nonNegativeInteger': 'integer',
    'xsd:date': 'date',
    'xsd:dateTimeStamp': 'datetime',
    'xsd:gYear': 'string',
    'xsd:gYearMonth': 'string',
    'xsd:string': 'string',
    'dct:MediaType': 'file',
    'rdfs:Literal': 'string',
    'vcard:Kind': 'string',
    'foaf:Document': 'file',
    'dcterms:Location': 'string',
    'owl:Thing': 'string',
}


def _to_type_name(
    graph: Graph,
    dataset: Dict,
    models: List,
    name: str,
) -> Tuple[str, str]:
    ref_model = None
    type = 'string'
    if name:
        if name in TYPES:
            type = TYPES[name]
        else:
            model_name, _ = _to_model_name(graph, dataset, name)
            if model_name in models:
                type = 'ref'
                ref_model = model_name
    return type, ref_model


def _parse_uri(graph: Graph, name: Any) -> str:
    if isinstance(name, URIRef):
        return graph.qname(name)
    elif name:
        return str(name)
    else:
        return name


def _get_value_for_language(
    value: Any,
    lang: str
) -> str:
    if (
        value and
        hasattr(value, 'language') and
        value.language == lang
    ):
        return str(value)
    else:
        return ''
