INSTANCE=datasets/json
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

unset SPINTA_CONFIG

http -b get https://api.vilnius.lt/api/calendar/get-non-working-days
#| {
#|     "nonWorkingDates": [
#|         {
#|             "date": "2023-01-01",
#|             "name": "Naujieji metai"
#|         }
#|     ],
#|     "workingHolidayDates": [
#|         {
#|             "date": "2022-12-31",
#|             "name": "Naujųjų Metų išvakarės"
#|         }
#|     ]
#| }

poetry run spinta inspect -r json https://api.vilnius.lt/api/calendar/get-non-working-days
#| │ /home/sirex/dev/data/spinta/spinta/manifests/dict/helpers.py:14 in read_schema                   │
#| │                                                                                                  │
#| │    11                                                                                            │
#| │    12 def read_schema(manifest_type: DictFormat, path: str):                                     │
#| │    13 │   if path.startswith(('http://', 'https://')):                                           │
#| │ ❱  14 │   │   value = urlopen(path).read()                                                       │
#| │    15 │   else:                                                                                  │
#| │    16 │   │   with pathlib.Path(path).open(encoding='utf-8-sig') as f:                           │
#| │    17 │   │   │   value = f.read()                                                               │
#| │                                                                                                  │
#| │ ╭────────────────────────────────── locals ──────────────────────────────────╮                   │
#| │ │ manifest_type = <DictFormat.JSON: 'json'>                                  │                   │
#| │ │          path = 'https://api.vilnius.lt/api/calendar/get-non-working-days' │                   │
#| │ ╰────────────────────────────────────────────────────────────────────────────╯                   │
#| HTTPError: HTTP Error 403: Forbidden
