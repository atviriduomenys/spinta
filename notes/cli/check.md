## Missing references

```nu

mkdir .tmp/check

"
d | m | property             | type    | ref
datasets/gov/rc/ar/uapi/addr |         |
  |   |                      |         |
  | Country                  |         | id
  |   | id                   | integer |
  |   | code                 | string  |
  |   | name@lt              | string  |
  |   |                      |         |
  | City                     |         | id
  |   | id                   | integer |
  |   | name@lt              | string  |
  |   | country              | ref     | Country2

" | str trim | save .tmp/check/manifest.txt

spinta show .tmp/check/manifest.txt

spinta check .tmp/check/manifest.txt

spinta check -f json .tmp/check/manifest.txt
```

## Prefixes

```nu

mkdir .tmp/check/prefix

"
d | m | property             | type    | ref  | uri
datasets/gov/rc/ar/uapi/addr |         |      |
  |   |                      |         |      |
  | City                     |         | id   | locn:Location
  |   | id                   | integer |      | dct:identifier
  |   | code                 | string  |      | skos:notation
  |   | name@lt              | string  |      | rdfs:label
" | str trim | save .tmp/check/prefix/manifest.txt


spinta show .tmp/check/prefix/manifest.txt

spinta check .tmp/check/prefix/manifest.txt

```
