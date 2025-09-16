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
#| id | d | r | b | m | property                             | type   | ref                                  | source | prepare | level | access | uri                                         | title                            | description
#|    | dcat2                                                |        |                                      |        |         |       |        |                                             |                                  |
#|    |                                                      | prefix | brick                                |        |         |       |        | https://brickschema.org/schema/Brick#       |                                  |
#|    |                                                      |        | csvw                                 |        |         |       |        | http://www.w3.org/ns/csvw#                  |                                  |
#|    |                                                      |        | dc                                   |        |         |       |        | http://purl.org/dc/elements/1.1/            |                                  |
#|    |                                                      |        | dcat                                 |        |         |       |        | http://www.w3.org/ns/dcat#                  |                                  |
#|    |                                                      |        | dcam                                 |        |         |       |        | http://purl.org/dc/dcam/                    |                                  |
#|    |                                                      |        | doap                                 |        |         |       |        | http://usefulinc.com/ns/doap#               |                                  |
#|    |                                                      |        | foaf                                 |        |         |       |        | http://xmlns.com/foaf/0.1/                  |                                  |
#|    |                                                      |        | geo                                  |        |         |       |        | http://www.opengis.net/ont/geosparql#       |                                  |
#|    |                                                      |        | odrl                                 |        |         |       |        | http://www.w3.org/ns/odrl/2/                |                                  |
#|    |                                                      |        | org                                  |        |         |       |        | http://www.w3.org/ns/org#                   |                                  |
#|    |                                                      |        | prof                                 |        |         |       |        | http://www.w3.org/ns/dx/prof/               |                                  |
#|    |                                                      |        | prov                                 |        |         |       |        | http://www.w3.org/ns/prov#                  |                                  |
#|    |                                                      |        | qb                                   |        |         |       |        | http://purl.org/linked-data/cube#           |                                  |
#|    |                                                      |        | schema                               |        |         |       |        | https://schema.org/                         |                                  |
#|    |                                                      |        | sh                                   |        |         |       |        | http://www.w3.org/ns/shacl#                 |                                  |
#|    |                                                      |        | skos                                 |        |         |       |        | http://www.w3.org/2004/02/skos/core#        |                                  |
#|    |                                                      |        | sosa                                 |        |         |       |        | http://www.w3.org/ns/sosa/                  |                                  |
#|    |                                                      |        | ssn                                  |        |         |       |        | http://www.w3.org/ns/ssn/                   |                                  |
#|    |                                                      |        | time                                 |        |         |       |        | http://www.w3.org/2006/time#                |                                  |
#|    |                                                      |        | vann                                 |        |         |       |        | http://purl.org/vocab/vann/                 |                                  |
#|    |                                                      |        | void                                 |        |         |       |        | http://rdfs.org/ns/void#                    |                                  |
#|    |                                                      |        | wgs                                  |        |         |       |        | https://www.w3.org/2003/01/geo/wgs84_pos#   |                                  |
#|    |                                                      |        | owl                                  |        |         |       |        | http://www.w3.org/2002/07/owl#              |                                  |
#|    |                                                      |        | rdf                                  |        |         |       |        | http://www.w3.org/1999/02/22-rdf-syntax-ns# |                                  |
#|    |                                                      |        | rdfs                                 |        |         |       |        | http://www.w3.org/2000/01/rdf-schema#       |                                  |
#|    |                                                      |        | xsd                                  |        |         |       |        | http://www.w3.org/2001/XMLSchema#           |                                  |
#|    |                                                      |        | xml                                  |        |         |       |        | http://www.w3.org/XML/1998/namespace        |                                  |
#|    |                                                      |        | dct                                  |        |         |       |        | http://purl.org/dc/terms/                   |                                  |
#|    |                                                      |        | dctype                               |        |         |       |        | http://purl.org/dc/dcmitype/                |                                  |
#|    |                                                      |        | sdo                                  |        |         |       |        | http://schema.org/                          |                                  |
#|    |                                                      |        | vcard                                |        |         |       |        | http://www.w3.org/2006/vcard/ns#            |                                  |
#|    |                                                      |        |                                      |        |         |       |        |                                             |                                  |
#|    |                                                      |        |                                      |        |         |       |        |                                             |                                  |
#|    |   |   |   | Distribution                             |        |                                      |        |         |       | open   | dcat:Distribution                           | Distribution                     | A specific representation of a dataset. A dataset might be available in multiple serializations that may differ in various ways, including natural language, media-type or format, schematic organization, te
#|    |   |   |   |   | accessurl                            | ref    | Resource                             |        |         |       | open   | dcat:accessURL                              | access address                   | A URL of a resource that gives access to a distribution of the dataset. E.g. landing page, feed, SPARQL endpoint. Use for all cases except a simple download link, in which case downloadURL is preferred.
#|    |   |   |   |   | byte_size                            | string |                                      |        |         |       | open   | dcat:byteSize                               | byte size                        | The size of a distribution in bytes.
#|    |   |   |   |   | compress_format                      | file   |                                      |        |         |       | open   | dcat:compressFormat                         | compression format               | The compression format of the distribution in which the data is contained in a compressed form, e.g. to reduce the size of the downloadable file.
#|    |   |   |   |   | downloadurl                          | ref    | Resource                             |        |         |       | open   | dcat:downloadURL                            | download URL                     | The URL of the downloadable file in a given format. E.g. CSV file or RDF file. The format is indicated by the distribution's dct:format and/or dcat:mediaType.
#|    |   |   |   |   | media_type                           | file   |                                      |        |         |       | open   | dcat:mediaType                              | media type                       | The media type of the distribution as defined by IANA
#|    |   |   |   |   | package_format                       | file   |                                      |        |         |       | open   | dcat:packageFormat                          | packaging format                 | The package format of the distribution in which one or more data files are grouped together, e.g. to enable a set of related files to be downloaded together.
#|    |                                                      |        |                                      |        |         |       |        |                                             |                                  |
#|    |   |   |   | Location                                 |        |                                      |        |         |       | open   | dct:Location                                |                                  |
#|    |   |   |   |   | bbox                                 | string |                                      |        |         |       | open   | dcat:bbox                                   | bounding box                     |
#|    |   |   |   |   | centroid                             | string |                                      |        |         |       | open   | dcat:centroid                               | centroid                         |
#|    |                                                      |        |                                      |        |         |       |        |                                             |                                  |
#|    |   |   |   | Resource                                 |        |                                      |        |         |       | open   | dcat:Resource                               | Catalogued resource              | Resource published or curated by a single agent.
#|    |   |   |   |   | contact_point                        | string |                                      |        |         |       | open   | dcat:contactPoint                           | contact point                    | Relevant contact information for the catalogued resource. Use of vCard is recommended.
#|    |   |   |   |   | keyword                              | string |                                      |        |         |       | open   | dcat:keyword                                | keyword                          | A keyword or tag describing a resource.
#|    |   |   |   |   | landing_page                         | file   |                                      |        |         |       | open   | dcat:landingPage                            | landing page                     | A Web page that can be navigated to in a Web browser to gain access to the catalog, a dataset, its distributions and/or additional information.
#|    |   |   |   |   | theme                                | ref    | Concept                              |        |         |       | open   | dcat:theme                                  | theme                            | A main category of the resource. A resource can have multiple themes.
#|    |   |   |   |   | accessurl                            | ref    | Distribution                         |        |         |       | open   | dcat:accessURL                              | access address                   | A URL of a resource that gives access to a distribution of the dataset. E.g. landing page, feed, SPARQL endpoint. Use for all cases except a simple download link, in which case downloadURL is preferred.
#|    |   |   |   |   | bbox                                 | string |                                      |        |         |       | open   | dcat:bbox                                   |                                  |
#|    |   |   |   |   | byte_size                            | string |                                      |        |         |       | open   | dcat:byteSize                               |                                  |
#|    |   |   |   |   | centroid                             | string |                                      |        |         |       | open   | dcat:centroid                               |                                  |
#|    |   |   |   |   | compress_format                      | ref    | Distribution                         |        |         |       | open   | dcat:compressFormat                         | compression format               | The compression format of the distribution in which the data is contained in a compressed form, e.g. to reduce the size of the downloadable file.
#|    |   |   |   |   | dataset                              | ref    | Catalog                              |        |         |       | open   | dcat:dataset                                | dataset                          | A collection of data that is listed in the catalog.
#|    |   |   |   |   | distribution                         | ref    | Dataset                              |        |         |       | open   | dcat:distribution                           | distribution                     | An available distribution of the dataset.
#|    |   |   |   |   | downloadurl                          | ref    | Distribution                         |        |         |       | open   | dcat:downloadURL                            | download URL                     | The URL of the downloadable file in a given format. E.g. CSV file or RDF file. The format is indicated by the distribution's dct:format and/or dcat:mediaType.
#|    |   |   |   |   | end_date                             | string |                                      |        |         |       | open   | dcat:endDate                                |                                  |
#|    |   |   |   |   | media_type                           | ref    | Distribution                         |        |         |       | open   | dcat:mediaType                              | media type                       | The media type of the distribution as defined by IANA
#|    |   |   |   |   | package_format                       | ref    | Distribution                         |        |         |       | open   | dcat:packageFormat                          | packaging format                 | The package format of the distribution in which one or more data files are grouped together, e.g. to enable a set of related files to be downloaded together.
#|    |   |   |   |   | record                               | ref    | Catalog                              |        |         |       | open   | dcat:record                                 | record                           | A record describing the registration of a single dataset or data service that is part of the catalog.
#|    |   |   |   |   | start_date                           | string |                                      |        |         |       | open   | dcat:startDate                              |                                  |
#|    |   |   |   |   | theme_taxonomy                       | ref    | Catalog                              |        |         |       | open   | dcat:themeTaxonomy                          | theme taxonomy                   | The knowledge organization system (KOS) used to classify catalog's datasets.
#|    |   |   |   |   | access_service                       | ref    | DataService                          |        |         |       | open   | dcat:accessService                          | data access service              | A site or end-point that gives access to the distribution of the dataset.
#|    |   |   |   |   | catalog                              | ref    | Catalog                              |        |         |       | open   | dcat:catalog                                | catalog                          | A catalog whose contents are of interest in the context of this catalog.
#|    |   |   |   |   | endpoint_description                 | ref    | DataService                          |        |         |       | open   | dcat:endpointDescription                    | description of service end-point | A description of the service end-point, including its operations, parameters etc.
#|    |   |   |   |   | endpointurl                          | ref    | DataService                          |        |         |       | open   | dcat:endpointURL                            | service end-point                | The root location or primary endpoint of the service (a web-resolvable IRI).
#|    |   |   |   |   | had_role                             | ref    | ncd510c40687344beb74ddbd2856bb636b29 |        |         |       | open   | dcat:hadRole                                | hadRole                          | The function of an entity or agent with respect to another entity or resource.
#|    |   |   |   |   | qualified_relation                   | ref    | Resource                             |        |         |       | open   | dcat:qualifiedRelation                      | qualified relation               | Link to a description of a relationship with another resource.
#|    |   |   |   |   | serves_dataset                       | ref    | DataService                          |        |         |       | open   | dcat:servesDataset                          | serves dataset                   | A collection of data that this DataService can distribute.
#|    |   |   |   |   | service                              | ref    | Catalog                              |        |         |       | open   | dcat:service                                | service                          | A site or endpoint that is listed in the catalog.
#|    |   |   |   |   | homepage                             | string |                                      |        |         |       | open   | foaf:homepage                               |                                  |
#|    |   |   |   |   | primary_topic                        | string |                                      |        |         |       | open   | foaf:primaryTopic                           |                                  |
#|    |   |   |   |   | spatial_resolution_in_meters         | number |                                      |        |         |       | open   | dcat:spatialResolutionInMeters              |                                  |
#|    |   |   |   |   | temporal_resolution                  | string |                                      |        |         |       | open   | dcat:temporalResolution                     | temporal resolution              | minimum time period resolvable in a dataset.
#|    |                                                      |        |                                      |        |         |       |        |                                             |                                  |
#|    |   |   | Dataset                                      |        |                                      |        |         |       |        |                                             |                                  |
#|    |   |   |   | Catalog                                  |        |                                      |        |         |       | open   | dcat:Catalog                                | Catalog                          | A curated collection of metadata about resources (e.g., datasets and data services in the context of a data catalog).
#|    |   |   |   |   | dataset                              | ref    | Dataset                              |        |         |       | open   | dcat:dataset                                | dataset                          | A collection of data that is listed in the catalog.
#|    |   |   |   |   | record                               | ref    | CatalogRecord                        |        |         |       | open   | dcat:record                                 | record                           | A record describing the registration of a single dataset or data service that is part of the catalog.
#|    |   |   |   |   | theme_taxonomy                       | ref    | Resource                             |        |         |       | open   | dcat:themeTaxonomy                          | theme taxonomy                   | The knowledge organization system (KOS) used to classify catalog's datasets.
#|    |   |   |   |   | catalog                              | ref    | Catalog                              |        |         |       | open   | dcat:catalog                                | catalog                          | A catalog whose contents are of interest in the context of this catalog.
#|    |   |   |   |   | service                              | ref    | DataService                          |        |         |       | open   | dcat:service                                | service                          | A site or endpoint that is listed in the catalog.
#|    |                                                      |        |                                      |        |         |       |        |                                             |                                  |
#|    |   |   | Resource                                     |        |                                      |        |         |       |        |                                             |                                  |
#|    |   |   |   | Dataset                                  |        |                                      |        |         |       | open   | dcat:Dataset                                | Dataset                          | A collection of data, published or curated by a single source, and available for access or download in one or more representations.
#|    |   |   |   |   | distribution                         | ref    | Distribution                         |        |         |       | open   | dcat:distribution                           | distribution                     | An available distribution of the dataset.
#|    |                                                      |        |                                      |        |         |       |        |                                             |                                  |
#|    |   |   | /                                            |        |                                      |        |         |       |        |                                             |                                  |
#|    |   |   |   | PeriodOfTime                             |        |                                      |        |         |       | open   | dct:PeriodOfTime                            |                                  |
#|    |   |   |   |   | end_date                             | string |                                      |        |         |       | open   | dcat:endDate                                | end date                         |
#|    |   |   |   |   | start_date                           | string |                                      |        |         |       | open   | dcat:startDate                              | start date                       |
#|    |                                                      |        |                                      |        |         |       |        |                                             |                                  |
#|    |   |   | Service                                      |        |                                      |        |         |       |        |                                             |                                  |
#|    |   |   |   | DataService                              |        |                                      |        |         |       | open   | dcat:DataService                            | Data service                     | A site or end-point providing operations related to the discovery of, access to, or processing functions on, data or related resources.
#|    |   |   |   |   | endpoint_description                 | string |                                      |        |         |       | open   | dcat:endpointDescription                    | description of service end-point | A description of the service end-point, including its operations, parameters etc.
#|    |   |   |   |   | endpointurl                          | ref    | Resource                             |        |         |       | open   | dcat:endpointURL                            | service end-point                | The root location or primary endpoint of the service (a web-resolvable IRI).
#|    |   |   |   |   | serves_dataset                       | ref    | Dataset                              |        |         |       | open   | dcat:servesDataset                          | serves dataset                   | A collection of data that this DataService can distribute.
#|    |                                                      |        |                                      |        |         |       |        |                                             |                                  |
#|    |   |   | /                                            |        |                                      |        |         |       |        |                                             |                                  |
#|    |   |   |   | ncd510c40687344beb74ddbd2856bb636b29     |        |                                      |        |         |       | open   |                                             |                                  |
#|    |   |   |   |   | had_role                             | ref    | Role                                 |        |         |       | open   | dcat:hadRole                                | hadRole                          | The function of an entity or agent with respect to another entity or resource.
#|    |                                                      |        |                                      |        |         |       |        |                                             |                                  |
#|    |   |   | ncd510c40687344beb74ddbd2856bb636b25         |        |                                      |        |         |       |        |                                             |                                  |
#|    |   |   |   | CatalogRecord                            |        |                                      |        |         |       | open   | dcat:CatalogRecord                          | Catalog Record                   | A record in a data catalog, describing the registration of a single dataset or data service.
#|    |                                                      |        |                                      |        |         |       |        |                                             |                                  |
#|    |   |   | /                                            |        |                                      |        |         |       |        |                                             |                                  |
#|    |   |   |   | Relationship                             |        |                                      |        |         |       | open   | dcat:Relationship                           | Relationship                     | An association class for attaching additional information to a relationship between DCAT Resources.
#|    |                                                      |        |                                      |        |         |       |        |                                             |                                  |
#|    |   |   | Concept                                      |        |                                      |        |         |       |        |                                             |                                  |
#|    |   |   |   | Role                                     |        |                                      |        |         |       | open   | dcat:Role                                   | Role                             | A role is the function of a resource or agent with respect to another resource, in the context of resource attribution or resource relationships.
#|    |                                                      |        |                                      |        |         |       |        |                                             |                                  |
#|    |   |   | /                                            |        |                                      |        |         |       |        |                                             |                                  |
#|    |   |   |   | ncd510c40687344beb74ddbd2856bb636b25     |        |                                      |        |         |       | open   |                                             |                                  |
#|    |                                                      |        |                                      |        |         |       |        |                                             |                                  |
#|    |   |   |   | Service                                  |        |                                      |        |         |       | open   |                                             |                                  |
#|    |                                                      |        |                                      |        |         |       |        |                                             |                                  |
#|    |   |   |   | Concept                                  |        |                                      |        |         |       | open   |                                             |                                  |
