# 2023-03-03 14:08

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=formats/ascii
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property    | type    | ref | access
$DATASET                    |         |     |
  |   |   | Dataset         |         | id  |
  |   |   |   | id          | integer |     | open
  |   |   |   | title       | string  |     | open
  |   |   |   | description | string  |     | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# notes/spinta/server.sh    Check configuration
# notes/spinta/server.sh    Run migrations
# notes/spinta/server.sh    Add client
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
