# 2023-04-06 15:42

poetry run pip install rdflib
poetry run python

import itertools

from rdflib import Graph
from rdflib.namespace import NamespaceManager

from spinta.formats.ascii.helpers import get_widths
from spinta.formats.ascii.helpers import get_displayed_cols
from spinta.formats.ascii.helpers import draw_border
from spinta.formats.ascii.helpers import draw_header
from spinta.formats.ascii.helpers import draw_row


g = Graph(bind_namespaces='rdflib')
g.bind('r5r', 'http://data.europa.eu/r5r/')
g.bind('adms', 'http://www.w3.org/ns/adms#')
g.bind('sh', 'http://www.w3.org/ns/shacl#')

dcat = 'https://www.w3.org/ns/dcat2.ttl'
g.parse(dcat, format='turtle')

dcat_ap_rdf = 'https://github.com/SEMICeu/DCAT-AP/raw/master/releases/2.1.1/dcat-ap_2.1.1.rdf'
g.parse(dcat_ap_rdf, format='application/rdf+xml')

dcat_ap_shacl_shapes = 'https://github.com/SEMICeu/DCAT-AP/raw/master/releases/2.1.1/dcat-ap_2.1.1_shacl_shapes.ttl'
g.parse(dcat_ap_shacl_shapes, format='turtle')

def query(g, qry):
    cols = []
    rrows = []
    nm = NamespaceManager(g, bind_namespaces='rdflib')
    rows = g.query(qry)
    rows = (
        {k: v.n3(nm) for k, v in row.asdict().items()}
        for row in rows
    )
    for row in rows:
        cols = list(row)
        rrows.append(row)
        break
    rows = itertools.chain(rrows, rows)
    max_value_length = 100
    read_rows, widths = get_widths(
        rows,
        cols,
        max_value_length,
        max_col_width = None,
    )
    rows = itertools.chain(read_rows, rows)
    separator = ' '
    shortened, displayed_cols = False, cols
    print(draw_header(widths, displayed_cols, separator, shortened), end='')
    print(draw_border(widths, displayed_cols, separator, shortened), end='')
    for row in rows:
        print(draw_row(
            row,
            widths,
            displayed_cols,
            max_value_length,
            separator,
            shortened
        ), end='')

query(g, '''
select
    ?model
    ?model_title_shacl
    ?model_title_rdfs
    ?prop
    ?prop_type_shacl
    ?prop_title_rdfs
where {
    {
        {
        ?model_shape a sh:NodeShape ;
            sh:targetClass ?model ;
            sh:name ?model_title_shacl ;
            sh:property ?prop_node
            .

        filter langMatches(lang(?model_title_shacl), "EN")
        }

        ?prop_node sh:path ?prop .

        optional {
            ?prop_node sh:datatype ?prop_type_shacl
        }
    }
    bind(str(?model_title_shacl) as ?model_title_shacl)

    {
        ?model rdfs:label ?model_title_rdfs .
        filter langMatches(lang(?model_title_rdfs), "EN")
    }
    bind(str(?model_title_rdfs) as ?model_title_rdfs)

    {
        ?prop rdfs:label ?prop_title_rdfs .
        filter langMatches(lang(?prop_title_rdfs), "EN")
    }
    bind(str(?prop_title_rdfs) as ?prop_title_rdfs)
}
''')

unset SPINTA_CONFIG
poetry run spinta --tb native inspect -r rdf https://www.w3.org/ns/dcat2.ttl
#| Loading MemoryManifest manifest default
#| Loading InlineManifest manifest default
#| Traceback (most recent call last):
#|   File "spinta/cli/inspect.py", line 94, in inspect
#|     _merge(context, manifest, manifest, resource, has_manifest_priority)
#|   File "spinta/cli/inspect.py", line 120, in _merge
#|     store = load_manifest(context)
#|   File "spinta/cli/helpers/store.py", line 116, in load_manifest
#|     commands.load(
#|   File "spinta/manifests/yaml/commands/load.py", line 108, in load
#|     commands.load(
#|   File "spinta/manifests/rdf/commands/load.py", line 31, in load
#|     load_manifest_nodes(context, into, schemas, source=manifest)
#|   File "spinta/manifests/helpers.py", line 136, in load_manifest_nodes
#|     node = _load_manifest_node(context, config, manifest, source, eid, schema)
#|   File "spinta/manifests/helpers.py", line 197, in _load_manifest_node
#|     commands.load(context, node, data, manifest, source=source)
#|   File "spinta/types/model.py", line 86, in load
#|     load_model_properties(context, model, Property, data.get('properties'))
#|   File "spinta/nodes.py", line 219, in load_model_properties
#|     prop = commands.load(context, prop, params, model.manifest)
#|   File "spinta/types/model.py", line 214, in load
#|     raise UnknownPropertyType(prop, type=data['type'])
#| spinta.exceptions.UnknownPropertyType: Unknown property type 'None'.
#|   Context:
#|     component: spinta.components.Property
#|     manifest: default
#|     schema: 2
#|     model: dcat2/Distribution
#|     property: accessurl
#|     type: None
