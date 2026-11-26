
```nu

let dir = ".tmp/manifests/jsonld"
mkdir $dir

"
d | r | m | property | type    | ref     | access | uri                                  | title       | description
locn                 |         |         | open   |                                      | Location    | Location dataset
  |   |   |          | prefix  | locn    |        | http://www.w3.org/ns/locn#           |             |
  |   |   |          |         | dct     |        | http://purl.org/dc/terms/            |             |
  |   |   |          |         | skos    |        | http://www.w3.org/2004/02/skos/core# |             |
  |   |   |          |         |         |        |                                      |             |
  | locn_src         |         | default | open   |                                      | Locn source |
  |   |   |          |         |         |        |                                      |             |
  |   | Country      |         | id      |        | locn:Country                         | Country     |
  |   |   | id       | integer |         |        | dct:identifier                       | Identifier  |
  |   |   | code     | string  |         |        | skos:notation                        | Code        |
" | str trim | save $dir/manifest.txt

spinta check ($dir)/manifest.txt
spinta copy .tmp/manifests/jsonld/manifest.txt -m locn/Country -o .tmp/inheritance/context.jsonld
```

Result `.tmp/inheritance/context.jsonld`:

```json
{
  "@context": {
    "code": "skos:notation",
    "dct": "http://purl.org/dc/terms/",
    "id": {
      "@id": "dct:identifier",
      "@type": "xsd:integer"
    },
    "locn": "http://www.w3.org/ns/locn#",
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "xsd": "http://www.w3.org/2001/XMLSchema#"
  }
}
```
