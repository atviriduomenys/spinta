```nu
spinta --version


spinta --tb=native copy ~/dev/data/metadata/datasets/gov/vulsk/santa_his/espbi/dsa.csv -o .tmp/simple/metadata/datasets/gov/vulsk/santa_his/espbi/dsa.simple.json

python scripts/check.py ~/dev/data/metadata .tmp/simple | save .tmp/report.md

spinta --tb=native copy ~/dev/data/metadata/datasets/gov/rc/jar/at77_israsas/dsa.csv -o ~/dev/data/spinta/2106-linked-data/.tmp/simple/datasets/gov/rc/jar/at77_israsas/dsa.simple.json
spinta --tb=native copy ~/dev/data/metadata/datasets/gov/rc/ir/at5509_santrauka/dsa.csv -o ~/dev/data/spinta/2106-linked-data/.tmp/simple/datasets/gov/rc/ir/at5509_santrauka/dsa.simple.json

spinta --tb=native copy ~/dev/data/metadata/datasets/gov/rc/gr/at605_isplestinis/dsa.csv -o ~/dev/data/spinta/2106-linked-data/.tmp/simple/datasets/gov/rc/gr/at605_isplestinis/dsa.simple.json
```
