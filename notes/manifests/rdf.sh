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

poetry install
unset SPINTA_CONFIG
poetry run spinta inspect -r rdf https://www.w3.org/ns/dcat2.ttl
#| id | d | r | b | m | property                           | type   | ref           | access | uri                            | title | description
#|    | dcat2                                              |        |               |        |                                |       |
#|    |                                                    |        |               |        |                                |       |
#|    |   |   |   | Distribution                           |        |               | open   |                                |       | A specific representation of a dataset. A dataset might be available in multiple serializations that may differ in various ways, including natural language, media-type or form
#|    |   |   |   |   | accessurl                          | string |               | open   | dcat:accessURL                 |       | A URL of a resource that gives access to a distribution of the dataset. E.g. landing page, feed, SPARQL endpoint. Use for all cases except a simple download link, in which cas
#|    |   |   |   |   | byte_size                          | string |               | open   | dcat:byteSize                  |       | The size of a distribution in bytes.
#|    |   |   |   |   | compress_format                    | string |               | open   | dcat:compressFormat            |       | The compression format of the distribution in which the data is contained in a compressed form, e.g. to reduce the size of the downloadable file.
#|    |   |   |   |   | downloadurl                        | string |               | open   | dcat:downloadURL               |       | The URL of the downloadable file in a given format. E.g. CSV file or RDF file. The format is indicated by the distribution's dct:format and/or dcat:mediaType.
#|    |   |   |   |   | media_type                         | string |               | open   | dcat:mediaType                 |       | The media type of the distribution as defined by IANA
#|    |   |   |   |   | package_format                     | string |               | open   | dcat:packageFormat             |       | The package format of the distribution in which one or more data files are grouped together, e.g. to enable a set of related files to be downloaded together.
#|    |                                                    |        |               |        |                                |       |
#|    |   |   |   | Location                               |        |               | open   |                                |       |
#|    |   |   |   |   | bbox                               | string |               | open   | dcat:bbox                      |       |
#|    |   |   |   |   | centroid                           | string |               | open   | dcat:centroid                  |       |
#|    |                                                    |        |               |        |                                |       |
#|    |   |   |   | Resource                               |        |               | open   |                                |       | Resource published or curated by a single agent.
#|    |   |   |   |   | contact_point                      | string |               | open   | dcat:contactPoint              |       | Relevant contact information for the catalogued resource. Use of vCard is recommended.
#|    |   |   |   |   | keyword                            | string |               | open   | dcat:keyword                   |       | A keyword or tag describing a resource.
#|    |   |   |   |   | landing_page                       | file   |               | open   | dcat:landingPage               |       | A Web page that can be navigated to in a Web browser to gain access to the catalog, a dataset, its distributions and/or additional information.
#|    |   |   |   |   | theme                              | ref    | Concept       | open   | dcat:theme                     |       | A main category of the resource. A resource can have multiple themes.
#|    |   |   |   |   | accessurl                          | ref    | Resource      | open   | dcat:accessURL                 |       |
#|    |   |   |   |   | bbox                               | string |               | open   | dcat:bbox                      |       |
#|    |   |   |   |   | byte_size                          | string |               | open   | dcat:byteSize                  |       |
#|    |   |   |   |   | centroid                           | string |               | open   | dcat:centroid                  |       |
#|    |   |   |   |   | compress_format                    | file   |               | open   | dcat:compressFormat            |       |
#|    |   |   |   |   | dataset                            | ref    | Dataset       | open   | dcat:dataset                   |       |
#|    |   |   |   |   | distribution                       | ref    | Distribution  | open   | dcat:distribution              |       |
#|    |   |   |   |   | downloadurl                        | ref    | Resource      | open   | dcat:downloadURL               |       |
#|    |   |   |   |   | end_date                           | string |               | open   | dcat:endDate                   |       |
#|    |   |   |   |   | media_type                         | file   |               | open   | dcat:mediaType                 |       |
#|    |   |   |   |   | package_format                     | file   |               | open   | dcat:packageFormat             |       |
#|    |   |   |   |   | record                             | ref    | CatalogRecord | open   | dcat:record                    |       |
#|    |   |   |   |   | start_date                         | string |               | open   | dcat:startDate                 |       |
#|    |   |   |   |   | theme_taxonomy                     | ref    | Resource      | open   | dcat:themeTaxonomy             |       |
#|    |   |   |   |   | qualified_relation                 | string |               | open   | dcat:qualifiedRelation         |       | Link to a description of a relationship with another resource.
#|    |   |   |   |   | access_service                     | ref    | DataService   | open   | dcat:accessService             |       | A site or end-point that gives access to the distribution of the dataset.
#|    |   |   |   |   | spatial_resolution_in_meters       | number |               | open   | dcat:spatialResolutionInMeters |       |
#|    |   |   |   |   | temporal_resolution                | string |               | open   | dcat:temporalResolution        |       | minimum time period resolvable in a dataset.
#|    |   |   |   |   | service                            | ref    | DataService   | open   | dcat:service                   |       |
#|    |   |   |   |   | endpointurl                        | ref    | Resource      | open   | dcat:endpointURL               |       |
#|    |   |   |   |   | catalog                            | ref    | Catalog       | open   | dcat:catalog                   |       |
#|    |   |   |   |   | serves_dataset                     | ref    | Dataset       | open   | dcat:servesDataset             |       |
#|    |   |   |   |   | had_role                           | ref    | Role          | open   | dcat:hadRole                   |       |
#|    |                                                    |        |               |        |                                |       |
#|    |   |   | dcat2/Dataset                              |        |               |        |                                |       |
#|    |   |   |   | Catalog                                |        |               | open   |                                |       | A curated collection of metadata about resources (e.g., datasets and data services in the context of a data catalog).
#|    |   |   |   |   | dataset                            | string |               | open   | dcat:dataset                   |       | A collection of data that is listed in the catalog.
#|    |   |   |   |   | record                             | string |               | open   | dcat:record                    |       | A record describing the registration of a single dataset or data service that is part of the catalog.
#|    |   |   |   |   | theme_taxonomy                     | string |               | open   | dcat:themeTaxonomy             |       | The knowledge organization system (KOS) used to classify catalog's datasets.
#|    |   |   |   |   | catalog                            | string |               | open   | dcat:catalog                   |       | A catalog whose contents are of interest in the context of this catalog.
#|    |   |   |   |   | service                            | string |               | open   | dcat:service                   |       | A site or endpoint that is listed in the catalog.
#|    |                                                    |        |               |        |                                |       |
#|    |   |   | dcat2/Resource                             |        |               |        |                                |       |
#|    |   |   |   | Dataset                                |        |               | open   |                                |       | A collection of data, published or curated by a single source, and available for access or download in one or more representations.
#|    |   |   |   |   | distribution                       | string |               | open   | dcat:distribution              |       | An available distribution of the dataset.
#|    |                                                    |        |               |        |                                |       |
#|    |   |   | /                                          |        |               |        |                                |       |
#|    |   |   |   | PeriodOfTime                           |        |               | open   |                                |       |
#|    |   |   |   |   | end_date                           | string |               | open   | dcat:endDate                   |       |
#|    |   |   |   |   | start_date                         | string |               | open   | dcat:startDate                 |       |
#|    |                                                    |        |               |        |                                |       |
#|    |   |   | dcat2/Resource                             |        |               |        |                                |       |
#|    |   |   |   | DataService                            |        |               | open   |                                |       | A site or end-point providing operations related to the discovery of, access to, or processing functions on, data or related resources.
#|    |   |   |   |   | endpoint_description               | string |               | open   | dcat:endpointDescription       |       | A description of the service end-point, including its operations, parameters etc.
#|    |   |   |   |   | endpointurl                        | string |               | open   | dcat:endpointURL               |       | The root location or primary endpoint of the service (a web-resolvable IRI).
#|    |   |   |   |   | serves_dataset                     | string |               | open   | dcat:servesDataset             |       | A collection of data that this DataService can distribute.
#|    |                                                    |        |               |        |                                |       |
#|    |   |   | /                                          |        |               |        |                                |       |
#|    |   |   |   | nd20f9caf2a4645549e1bbf3462c58bd8b29   |        |               | open   |                                |       |
#|    |   |   |   |   | had_role                           | string |               | open   | dcat:hadRole                   |       | The function of an entity or agent with respect to another entity or resource.
#|    |                                                    |        |               |        |                                |       |
#|    |   |   | dcat2/nd20f9caf2a4645549e1bbf3462c58bd8b26 |        |               |        |                                |       |
#|    |   |   |   | CatalogRecord                          |        |               | open   |                                |       | A record in a data catalog, describing the registration of a single dataset or data service.
#|    |                                                    |        |               |        |                                |       |
#|    |   |   | /                                          |        |               |        |                                |       |
#|    |   |   |   | nd20f9caf2a4645549e1bbf3462c58bd8b25   |        |               | open   |                                |       |
#|    |                                                    |        |               |        |                                |       |
#|    |   |   |   | nd20f9caf2a4645549e1bbf3462c58bd8b26   |        |               | open   |                                |       |
#|    |                                                    |        |               |        |                                |       |
#|    |   |   |   | Service                                |        |               | open   |                                |       |
#|    |                                                    |        |               |        |                                |       |
#|    |   |   |   | Concept                                |        |               | open   |                                |       |
#|    |                                                    |        |               |        |                                |       |
#|    |   |   | dcat2/Concept                              |        |               |        |                                |       |
#|    |   |   |   | Role                                   |        |               | open   |                                |       |
