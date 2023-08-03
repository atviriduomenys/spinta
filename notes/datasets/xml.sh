INSTANCE=datasets/xml
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

unset SPINTA_CONFIG

http -b get http://apps.lrs.lt/sip/p2b.ad_seimo_kadencijos | head
#| <?xml version="1.0" encoding="UTF-8"?>
#|     <SeimoInformacija
#|         pavadinimas="Seimo kadencijos"
#|         suformavimo_laikas="2023-07-26 11:44:45"
#|         šaltinis="www.lrs.lt"
#|         tiekėjas="Seimo kanceliarija"
#|         licencija="CC BY 4.0"
#|     >
#|         <SeimoKadencija
#|             kadencijos_id="1"
#|             pavadinimas="1990–1992 metų kadencija"
#|             data_nuo="1990-03-10"
#|             data_iki="1992-11-22"
#|         />

poetry run spinta inspect -r xml http://apps.lrs.lt/sip/p2b.ad_seimo_kadencijos
#| id | d | r | b | m | property                             | type                     | ref    | source                                         | prepare | level | access | uri | title | description
#|    | dataset                                              |                          |        |                                                |         |       |        |     |       |
#|    |   | resource                                         | xml                      |        | http://apps.lrs.lt/sip/p2b.ad_seimo_kadencijos |         |       |        |     |       |
#|    |                                                      |                          |        |                                                |         |       |        |     |       |
#|    |   |   |   | Model1                                   |                          |        | .                                              |         |       |        |     |       |
#|    |   |   |   |   | seimo_informacija_pavadinimas        | string required unique   |        | SeimoInformacija/@pavadinimas                  |         |       |        |     |       |
#|    |   |   |   |   | seimo_informacija_suformavimo_laikas | datetime required unique |        | SeimoInformacija/@suformavimo_laikas           |         |       |        |     |       |
#|    |   |   |   |   | seimo_informacija_saltinis           | string required unique   |        | SeimoInformacija/@šaltinis                     |         |       |        |     |       |
#|    |   |   |   |   | seimo_informacija_tiekejas           | string required unique   |        | SeimoInformacija/@tiekėjas                     |         |       |        |     |       |
#|    |   |   |   |   | seimo_informacija_licencija          | string required unique   |        | SeimoInformacija/@licencija                    |         |       |        |     |       |
#|    |                                                      |                          |        |                                                |         |       |        |     |       |
#|    |   |   |   | SeimoKadencija                           |                          |        | /SeimoInformacija/SeimoKadencija               |         |       |        |     |       |
#|    |   |   |   |   | kadencijos_id                        | integer required unique  |        | @kadencijos_id                                 |         |       |        |     |       |
#|    |   |   |   |   | pavadinimas                          | string required unique   |        | @pavadinimas                                   |         |       |        |     |       |
#|    |   |   |   |   | data_nuo                             | date required unique     |        | @data_nuo                                      |         |       |        |     |       |
#|    |   |   |   |   | data_iki                             | date unique              |        | @data_iki                                      |         |       |        |     |       |
#|    |   |   |   |   | parent                               | ref                      | Model1 | ../..                                          |         |       |        |     |       |

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property       | type                     | ref | source                                         | prepare | level | access
$DATASET                       |                          |     |                                                |         |       |
  | kadencijos                 | xml                      |     | http://apps.lrs.lt/sip/p2b.ad_seimo_kadencijos |         |       |
  |   |   | SeimoKadencija     |                          | id  | /SeimoInformacija/SeimoKadencija               |         | 4     |
  |   |   |   | id             | integer required unique  |     | @kadencijos_id                                 |         | 4     | open
  |   |   |   | pavadinimas    | string required          |     | @pavadinimas                                   |         | 4     | open
  |   |   |   | data_nuo       | date required            |     | @data_nuo                                      |         | 3     | open
  |   |   |   | data_iki       | date                     |     | @data_iki                                      |         | 3     | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show $BASEDIR/manifest.csv

# Run server with external mode (read from db.sqlite directly)
test -n "$PID" && kill $PID
poetry run spinta run --mode external $BASEDIR/manifest.csv &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# notes/spinta/client.sh    Configure client

http GET "$SERVER/$DATASET/SeimoKadencija?limit(1)"
#| HTTP/1.1 200 OK
#| 
#| {
#|     "_data": [
#|         {
#|             "_base": null,
#|             "_id": "42e43c2a-0bb5-40a4-9c0a-bd07d14745f1",
#|             "_revision": null,
#|             "_type": "datasets/xml/SeimoKadencija",
#|             "data_iki": "1992-11-22",
#|             "data_nuo": "1990-03-10",
#|             "id": 1,
#|             "pavadinimas": "1990–1992 metų kadencija"
#|         },
#|         {
#|             "_base": null,
#|             "_id": "4fbafcc0-2bae-4564-8e8b-13a9ab5902a8",
#|             "_revision": null,
#|             "_type": "datasets/xml/SeimoKadencija",
#|             "data_iki": "1996-11-22",
#|             "data_nuo": "1992-11-24",
#|             "id": 2,
#|             "pavadinimas": "1992–1996 metų kadencija"
#|         }
#|     ]
# FIXME: `limit(1)` was given, but 2 objects returned, instead of 1.
#| }
