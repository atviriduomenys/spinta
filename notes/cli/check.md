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
