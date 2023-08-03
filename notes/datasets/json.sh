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
#| id | d | r | b | m | property            | type                   | ref | source                                                   | prepare | level | access | uri | title | description
#|    | dataset                             |                        |     |                                                          |         |       |        |     |       |
#|    |   | resource                        | json                   |     | https://api.vilnius.lt/api/calendar/get-non-working-days |         |       |        |     |       |
#|    |                                     |                        |     |                                                          |         |       |        |     |       |
#|    |   |   |   | NonWorkingDates         |                        |     | nonWorkingDates                                          |         |       |        |     |       |
#|    |   |   |   |   | date                | date required unique   |     | date                                                     |         |       |        |     |       |
#|    |   |   |   |   | name                | string required unique |     | name                                                     |         |       |        |     |       |
#|    |                                     |                        |     |                                                          |         |       |        |     |       |
#|    |   |   |   | WorkingHolidayDates     |                        |     | workingHolidayDates                                      |         |       |        |     |       |
#|    |   |   |   |   | date                | date required unique   |     | date                                                     |         |       |        |     |       |
#|    |   |   |   |   | name                | string required        |     | name                                                     |         |       |        |     |       |


cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type                 | ref  | source                                                   | prepare | level | access
$DATASET                 |                      |      |                                                          |         |       |
  | resource             | json                 |      | https://api.vilnius.lt/api/calendar/get-non-working-days |         |       |
                         |                      |      |                                                          |         |       |
  |   |   | Holiday      |                      | date | nonWorkingDates                                          |         | 4     |
  |   |   |   | date     | date required unique |      | date                                                     |         | 4     | open
  |   |   |   | name     | string required      |      | name                                                     |         | 4     | open
                         |                      |      |                                                          |         |       |
  |   |   | Workday      |                      | date | workingHolidayDates                                      |         | 4     |
  |   |   |   | date     | date required unique |      | date                                                     |         | 4     | open
  |   |   |   | name     | string required      |      | name                                                     |         | 4     | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show $BASEDIR/manifest.csv

# Run server with external mode (read from db.sqlite directly)
test -n "$PID" && kill $PID
poetry run spinta run --mode external $BASEDIR/manifest.csv &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# notes/spinta/client.sh    Configure client

http GET "$SERVER/$DATASET/Holiday?limit(10)&format(ascii)"
#| date        name                              
#| ----------  ----------------------------------
#| 2023-01-01  Naujieji metai
#| 2023-02-16  Lietuvos nepriklausomybės atkūrimo diena
#| 2023-03-11  Lietuvos valstybės atkūrimo diena
#| 2023-04-09  Velykos
#| 2023-04-10  antroji Velykų diena
#| 2023-05-01  Tarptautinė darbo diena
#| 2023-06-24  Joninės/Rasos
#| 2023-07-06  Valstybės (Lietuvos karaliaus Mindaugo karūnavimo) diena
#| 2023-08-15  Žolinė (Švč. Mergelės Marijos ėmimo į dangų diena)
#| 2023-11-01  Visų šventųjų diena
#| 2023-11-02  Mirusiųjų atminimo (Vėlinių) diena

http GET "$SERVER/$DATASET/Workday?select(date,name)&limit(10)&format(ascii)"
#| HTTP/1.1 500 Internal Server Error
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "NotImplementedError",
#|             "message": "Could not find signature for and: <DaskDataFrameQueryBuilder, Expr>"
#|         }
#|     ]
#| }
# TODO: Implemente `select`.

http GET "$SERVER/$DATASET/Workday?limit(10)&format(ascii)"
#| date        name            
#| ----------  ----------------
#| 2022-12-31  Naujųjų Metų išvakarės
#| 2023-02-14  Valentino diena
#| 2023-02-21  Užgavėnės
#| 2023-03-26  Vasaros laikas prasideda
#| 2023-04-07  Didysis Penktadienis
#| 2023-04-08  Didysis Šeštadienis
#| 2023-10-29  Vasaros laikas baigiasi
#| 2023-10-31  Helovinas
#| 2023-12-31  Naujųjų Metų išvakarės
