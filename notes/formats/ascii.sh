# 2023-03-03 14:08

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=formats/ascii
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property    | type    | ref     | access
$DATASET                    |         |         |
  |   |   | Dataset         |         | id      |
  |   |   |   | id          | integer |         | open
  |   |   |   | title       | string  |         | open
  |   |   |   | description | string  |         | open
  |   |   |   |             |         |         |
  |   |   | Country         |         | id      |
  |   |   |   | id          | integer |         | open
  |   |   |   | name        | string  |         | open
  |   |   |   |             |         |         |
  |   |   | City            |         | id      |
  |   |   |   | id          | integer |         | open
  |   |   |   | name        | string  |         | open
  |   |   |   | country     | ref     | Country | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# notes/spinta/server.sh    Run migrations
# notes/spinta/server.sh    Run server
# notes/spinta/client.sh    Configure client


http POST "$SERVER/$DATASET/Dataset" $AUTH \
    id:=42 \
    title="Change in administrative burden for economic operators" \
    description="Based on the adopted draft legal acts, the data of the assessment of the change of administrative burden by the institutions for the economic entities that are the target group of the draft legal acts. The administrative burden is assessed on the basis of the Resolution “On Approval of the Methodology for Determining the Administrative Burden on Economic Entities” adopted on 11 January 2012. Data: year; half a year; institution; the draft legal acts are being evaluated; change in administrative burden, Eur. Periodicity: half a year. Beginning of the period: 2016"

http POST "$SERVER/$DATASET/Dataset" $AUTH \
    id:=329203 \
    title="Aktyvaus gydymo paslaugas teikiančios stacionarinės asmens sveikatos priežiūros įstaigos, pasirašiusios sutartis su teritorinėmis ligonių kasomis" \
    description="Inpatient personal health care institutions providing internal medicine, pediatric diseases, surgery and midwifery, which have signed agreements with territorial health insurance funds. Data from the METAS subsystem of the Compulsory Health Insurance Fund (PSDF IS) and the State Accreditation Service for Health Care Activities under the Ministry of Health (HACCP) were used to create the layer. Institutions (with branches) selected according to the PSDF IS and HACCP data, which: - Have a valid license to provide internal medicine, pediatric diseases, surgery and / or obstetric services; - Have signed an agreement with the territorial health insurance fund on the financing of the provision of inpatient personal health care services with the funds of the PSDF. Data date: 2018 August 3 The layer was created by planning a one-time study of the availability of health care services. During the survey, the layer can be revised, later it is not planned to update the layer."

http GET "$SERVER/$DATASET/Dataset?format(ascii)"
#| Table: formats/ascii/Dataset
#| ---------------------  ------------------------------------  ------------------------------------  ----  -----------------------------------------------  ...
#| _type                  _id                                   _revision                             id    title                                            ...
#| formats/ascii/Dataset  645488a5-c9fe-4038-9d60-8a46ce9a5c1b  80f1d2c0-cceb-4756-bd5d-5518628fbb11  42    Change in administrative burden for economic op  ...\
#|                                                                                                          erators                                          ...
#| formats/ascii/Dataset  495c7133-635a-45ba-9cbf-d3af66a1b411  005e613f-c017-460d-9183-613870d781d1  3292  Aktyvaus gydymo paslaugas teikiančios stacionar  ...\
#|                                                                                                    03    inės asmens sveikatos priežiūros įstaigos, pasi  ...\
#|                                                                                                          rašius...                                        ...
#| ---------------------  ------------------------------------  ------------------------------------  ----  -----------------------------------------------  ...

http GET "$SERVER/$DATASET/Dataset?format(ascii, width: 100)"
#| HTTP/1.1 500 Internal Server Error
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "AssertionError",
#|             "message": "{'name': 'bind', 'args': ['width', 100]}"
#|         }
#|     ]
#| }
tail -100 $BASEDIR/spinta.log
#| Traceback (most recent call last):
#|   File "spinta/api.py", line 85, in homepage
#|     params: UrlParams = prepare(context, UrlParams(), Version(), request)
#|   File "spinta/urlparams.py", line 37, in prepare
#|     prepare_urlparams(context, params, request)
#|   File "spinta/urlparams.py", line 85, in prepare_urlparams
#|     _prepare_urlparams_from_path(params)
#|   File "spinta/urlparams.py", line 172, in _prepare_urlparams_from_path
#|     assert len(arg['args']) == 1, arg
#| AssertionError: {'name': 'bind', 'args': ['width', 100]}
# TODO: Keyword arguments should be accepted.
http GET "$SERVER/$DATASET/Dataset?format(ascii, width(100))"
#| Table: formats/ascii/Dataset
#| ---------------------  ------------------------------------  ------------------------------------  ...
#| _type                  _id                                   _revision                             ...
#| formats/ascii/Dataset  645488a5-c9fe-4038-9d60-8a46ce9a5c1b  80f1d2c0-cceb-4756-bd5d-5518628fbb11  ...
#| formats/ascii/Dataset  495c7133-635a-45ba-9cbf-d3af66a1b411  005e613f-c017-460d-9183-613870d781d1  ...
#| ---------------------  ------------------------------------  ------------------------------------  ...

http GET "$SERVER/$DATASET/Dataset?select(id,title,description)&format(ascii)"
#| ----  -----------------------------------------------  ---------------------------------------------------------  
#| id    title                                            description                                                
#| 42    Change in administrative burden for economic op  Based on the adopted draft legal acts, the data of the as\
#|       erators                                          sessment of the change of administrative bu...           
#| 3292  Aktyvaus gydymo paslaugas teikiančios stacionar  Inpatient personal health care institutions providing int\
#| 03    inės asmens sveikatos priežiūros įstaigos, pasi  ernal medicine, pediatric diseases, surgery...           \
#|       rašius...                                                                                                 
#| ----  -----------------------------------------------  ---------------------------------------------------------  


http POST "$SERVER/$DATASET/Country" $AUTH id:=1 name="Lithunia" _id="54308ab0-7ca0-4728-bb07-0d8819bfc2a7"
http POST "$SERVER/$DATASET/Country" $AUTH id:=1 name="Latvia" _id="8fc0849d-1907-4cb2-8f62-64b41798824a"
http POST "$SERVER/$DATASET/City" $AUTH id:=1 name="Vilnius" "country[_id]"="54308ab0-7ca0-4728-bb07-0d8819bfc2a7"
http POST "$SERVER/$DATASET/City" $AUTH id:=2 name="Kaunas" "country[_id]"="54308ab0-7ca0-4728-bb07-0d8819bfc2a7"
http POST "$SERVER/$DATASET/City" $AUTH id:=3 name="Ryga" "country[_id]"="8fc0849d-1907-4cb2-8f62-64b41798824a"
http PATCH "$SERVER/$DATASET/City/a2fd854f-496e-4995-a8c8-09d08e67ef44" $AUTH id:=42 _revision=b5970a01-7563-4e36-b3b9-00be0d5fdd1b

http GET "$SERVER/$DATASET/City/:changes?format(ascii, width(1000))"
#| _cid  _created                    _op     _id                                   _txn                                  _revision                             id  name     country._id                         
#| ----  --------------------------  ------  ------------------------------------  ------------------------------------  ------------------------------------  --  -------  ------------------------------------
#| 1     2023-05-12T11:28:37.937121  insert  a2fd854f-496e-4995-a8c8-09d08e67ef44  e85b88b3-ea1e-4c62-8694-b6c137484b30  b5970a01-7563-4e36-b3b9-00be0d5fdd1b  1   Vilnius  54308ab0-7ca0-4728-bb07-0d8819bfc2a7
#| 2     2023-05-12T11:28:49.891273  insert  acf3c401-c46b-49cd-ac21-07aca3b278b7  9a58c66e-5a21-40d7-9a34-b361d20e31d1  544043f5-854c-4ebc-8ad4-2af3e25b6149  2   Kaunas   54308ab0-7ca0-4728-bb07-0d8819bfc2a7
#| 3     2023-05-12T11:29:34.385886  insert  4fb63623-fa66-4b89-b19f-0b1eb911946d  55d04b0c-2f25-4b2d-9649-84df5b7fd43a  85829d97-7b45-4e3e-aa76-cd04f4ac9df4  3   Ryga     8fc0849d-1907-4cb2-8f62-64b41798824a
#| 4     2023-05-12T11:37:56.675473  patch   a2fd854f-496e-4995-a8c8-09d08e67ef44  86953415-585a-45ad-ba22-15bc70902d09  04085671-e858-4eac-9557-db8f0034fc7f  42  ∅        ∅
http GET "$SERVER/$DATASET/City/a2fd854f-496e-4995-a8c8-09d08e67ef44/:changes?format(ascii, width(1000))"
#| _cid  _created                    _op     _id                                   _txn                                  _revision                             id  name     country._id                         
#| ----  --------------------------  ------  ------------------------------------  ------------------------------------  ------------------------------------  --  -------  ------------------------------------
#| 1     2023-05-12T11:28:37.937121  insert  a2fd854f-496e-4995-a8c8-09d08e67ef44  e85b88b3-ea1e-4c62-8694-b6c137484b30  b5970a01-7563-4e36-b3b9-00be0d5fdd1b  1   Vilnius  54308ab0-7ca0-4728-bb07-0d8819bfc2a7
#| 4     2023-05-12T11:37:56.675473  patch   a2fd854f-496e-4995-a8c8-09d08e67ef44  86953415-585a-45ad-ba22-15bc70902d09  04085671-e858-4eac-9557-db8f0034fc7f  42  ∅        ∅
http GET "$SERVER/$DATASET/Country/:changes?format(ascii, width(1000))"
#| _cid  _created                    _op     _id                                   _txn                                  _revision                             id  name    
#| ----  --------------------------  ------  ------------------------------------  ------------------------------------  ------------------------------------  --  --------
#| 1     2023-05-12T11:27:12.398533  insert  54308ab0-7ca0-4728-bb07-0d8819bfc2a7  8a095976-28d9-4f5b-b652-a17401ebef52  3ee9a07b-c5a3-441d-a314-d6528d86d37c  1   Lithunia
#| 2     2023-05-12T11:27:14.373875  insert  8fc0849d-1907-4cb2-8f62-64b41798824a  29d30920-8c38-403d-b38e-7c97b4deeedc  2d5dcca7-4bfe-488a-8707-93dd3f8f23c9  1   Latvia
