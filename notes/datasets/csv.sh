INSTANCE=datasets/csv
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

unset SPINTA_CONFIG

http -b get https://data.gov.lt/dataset/144/download/3039/STIS_2013.csv | head
#| "PRM_PRIEMONE";"PRM_PAVADINIMAS";"ASM_TIPAS";"ADR_SAV_PAVAD";"DEKLARUOJAMA_REIKSME";"PATVIRTINTA_REIKSME";"ISMOKETA_PARAMOS_SUMA";"ASM_LYTIS";"PRS_REG_DATA";"PRASOMA_PARAMOS_SUMA"
#| "TI";"Pagrindinė tiesioginių išmokų dalis";"Fizinis";"Jurbarko r. sav.";"5,06";"5,05";"635,47";"Moteris";"2013-05-06";"656,42"
#| "TI";"Pagrindinė tiesioginių išmokų dalis";"Fizinis";"Jurbarko r. sav.";"3,92";"3,92";"508,53";"Moteris";"2013-05-20";"508,53"
#| "MPŪT";"Mažo nepalankumo vietovių dalis";"Fizinis";"Utenos r. sav.";"9,98";"9,98";"0";"Vyras";"2013-06-05";"544,7"
#| "TI";"Pagrindinė tiesioginių išmokų dalis";"Fizinis";"Utenos r. sav.";"9,88";"9,88";"1166,35";"Vyras";"2013-06-05";"1281,7"
#| "TI";"Pagrindinė tiesioginių išmokų dalis";"Fizinis";"Utenos r. sav.";"5,35";"5,35";"694,04";"Vyras";"2013-05-07";"694,04"
#| "AGRO";"Natūralių ir pusiau natūralių pievų tvarkymas";"Fizinis";"Utenos r. sav.";"5,35";"5,35";"508,54";"Vyras";"2013-05-07";"524,26"
#| "TI";"Pagrindinė tiesioginių išmokų dalis";"Fizinis";"Panevėžio r. sav.";"5,11";"5,11";"662,9";"Moteris";"2013-05-20";"662,9"
#| "MPŪT";"Mažo nepalankumo vietovių dalis";"Fizinis";"Panevėžio r. sav.";"5,11";"5,11";"278,9";"Moteris";"2013-05-20";"278,9"
#| "AGRO";"Natūralių ir pusiau natūralių pievų tvarkymas";"Fizinis";"Telšių r. sav.";"1,83";"1,83";"179,33";"Vyras";"2013-06-05";"179,33"

poetry run spinta inspect -r csv https://data.gov.lt/dataset/144/download/3039/STIS_2013.csv
#| ╭─────────────────────────────── Traceback (most recent call last) ────────────────────────────────╮
#| │ /home/sirex/dev/data/spinta/spinta/manifests/tabular/helpers.py:1163 in _read_csv_manifest       │
#| │                                                                                                  │
#| │   1160 │   │   for i, row in enumerate(rows, 1):                                                 │
#| │   1161 │   │   │   yield str(i), row                                                             │
#| │   1162 │   else:                                                                                 │
#| │ ❱ 1163 │   │   with pathlib.Path(path).open(encoding='utf-8') as f:                              │
#| │   1164 │   │   │   rows = csv.reader(f)                                                          │
#| │   1165 │   │   │   for i, row in enumerate(rows, 1):                                             │
#| │   1166 │   │   │   │   yield str(i), row                                                         │
#| │                                                                                                  │
#| │ ╭─────────────────────────────── locals ───────────────────────────────╮                         │
#| │ │ file = None                                                          │                         │
#| │ │ path = 'https://data.gov.lt/dataset/144/download/3039/STIS_2013.csv' │                         │
#| │ ╰──────────────────────────────────────────────────────────────────────╯                         │
#| ╰──────────────────────────────────────────────────────────────────────────────────────────────────╯
#| FileNotFoundError: [Errno 2] No such file or directory: 'https:/data.gov.lt/dataset/144/download/3039/STIS_2013.csv'
# TODO: https://github.com/atviriduomenys/spinta/issues/87


cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property              | type   | ref      | source                                          | prepare           | level | access
$DATASET                              |        |          |                                                 |                   |       |
  | stis                              | csv    |          | https://data.gov.lt/dataset/144/download/{}.csv | tabular(sep: ";") |       |
  |   |   | Parama                    |        | priemone | 3039/STIS_2013                                  |                   | 4     |
  |   |   |   | priemone              | string |          | PRM_PRIEMONE                                    |                   | 4     | open
  |   |   |   | pavadinimas           | string |          | PRM_PAVADINIMAS                                 |                   | 4     | open
  |   |   |   | registruota           | date   |          | ASM_TIPAS                                       |                   | 4     | open
  |   |   |   | asmuo_tipas           | string |          | ADR_SAV_PAVAD                                   |                   | 4     | open
  |   |   |   | asmuo_lytis           | string |          | DEKLARUOJAMA_REIKSME                            |                   | 4     | open
  |   |   |   | adr_sav_pavad         | string |          | PATVIRTINTA_REIKSME                             |                   | 4     | open
  |   |   |   | deklaruojama_reiksme  | number |          | ISMOKETA_PARAMOS_SUMA                           |                   | 4     | open
  |   |   |   | patvirtinta_reiksme   | number |          | ASM_LYTIS                                       |                   | 4     | open
  |   |   |   | ismoketa_paramos_suma | number |          | PRS_REG_DATA                                    |                   | 4     | open
  |   |   |   | prasoma_paramos_suma  | number |          | PRASOMA_PARAMOS_SUMA                            |                   | 4     | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show $BASEDIR/manifest.csv

# Run server with external mode (read from db.sqlite directly)
test -n "$PID" && kill $PID
poetry run spinta run --mode external $BASEDIR/manifest.csv &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# notes/spinta/client.sh    Configure client
http GET "$SERVER/$DATASET/Parama?limit(1)"
#| HTTP/1.1 200 OK
#| 
#| {
#|     "_data": [
#|         {
#|             "_base": null,
#|             "_id": "fc75afa4-3e30-4102-8057-4b7bc4080aa8",
#|             "_revision": null,
#|             "_type": "datasets/csv/Parama",
#|             "adr_sav_pavad": "5,05",
# TODO: convert `number` type properties to `float`.
#|             "asmuo_lytis": "5,06",
#|             "asmuo_tipas": "Jurbarko r. sav.",
#|             "deklaruojama_reiksme": "635,47",
#|             "ismoketa_paramos_suma": "2013-05-06",
#|             "patvirtinta_reiksme": "Moteris",
#|             "pavadinimas": "Pagrindinė tiesioginių išmokų dalis",
#|             "prasoma_paramos_suma": "656,42",
#|             "priemone": "TI",
#|             "registruota": "Fizinis"
#|         },
#|         {
#|             "_base": null,
#|             "_id": "fc75afa4-3e30-4102-8057-4b7bc4080aa8",
#|             "_revision": null,
#|             "_type": "datasets/csv/Parama",
#|             "adr_sav_pavad": "3,92",
#|             "asmuo_lytis": "3,92",
#|             "asmuo_tipas": "Jurbarko r. sav.",
#|             "deklaruojama_reiksme": "508,53",
#|             "ismoketa_paramos_suma": "2013-05-20",
#|             "patvirtinta_reiksme": "Moteris",
#|             "pavadinimas": "Pagrindinė tiesioginių išmokų dalis",
#|             "prasoma_paramos_suma": "508,53",
#|             "priemone": "TI",
#|             "registruota": "Fizinis"
#|         }
# TODO: `limit(1)` was given, but two objects returned.
#|     ]
#| }
