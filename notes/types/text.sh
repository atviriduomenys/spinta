# 2023-02-17 16:10

git log -1 --oneline
#| 0ebfd06 (HEAD -> geometry-z, origin/master, origin/HEAD, master) Check if public data was written

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=types/text
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type  | ref | access
$DATASET                 |       |     |
  |   |   | Country      |       |     |
  |   |   |   | name@lt  | text  |     | open
  |   |   |   | name@en  | text  |     | open
EOF

poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
#| id,dataset,resource,base,model,property,type,ref,source,prepare,level,access,uri,title,description
#| ,types/text,,,,,,,,,,,,,
#| ,,,,,,,,,,,,,,
#| ,,,,Country,,,,,,,,,,
#| ,,,,,name,text,,,,,,,,
# FIXME: When saving tabular manifet, instead of two name properties only one DONE
#        is left.
cat > $BASEDIR/manifest.csv <<'EOF'
id,dataset,resource,base,model,property,type,ref,source,prepare,level,access,uri,title,description
,types/text,,,,,,,,,,,,,
,,,,,,,,,,,,,,
,,,,Country,,,,,,,,,,
,,,,,name@lt,text,,,,,open,,,
,,,,,name@en,text,,,,,open,,,
EOF
poetry run spinta show $BASEDIR/manifest.csv
#| id | d | r | b | m | property | type | ref | source | prepare | level | access | uri | title | description
#|    | types/text               |      |     |        |         |       |        |     |       |
#|    |                          |      |     |        |         |       |        |     |       |
#|    |   |   |   | Country      |      |     |        |         |       |        |     |       |
#|    |   |   |   |   | name     | text |     |        |         |       |        |     |       |
# FIXME: Same thing here, one property instead of two. Done

# notes/spinta/server.sh    Prepare manifest
# notes/spinta/server.sh    Check configuration
# notes/spinta/server.sh    Run migrations
# notes/spinta/server.sh    Add client
# notes/spinta/server.sh    Run server

psql -h localhost -p 54321 -U admin spinta -c '\dt public.*'
psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/Country"'
#|                     Table "public.types/text/Country"
#|   Column   |            Type             | Collation | Nullable | Default 
#| -----------+-----------------------------+-----------+----------+---------
#|  name      | jsonb                       |           |          | 

# notes/spinta/client.sh    Configure client

http POST "$SERVER/$DATASET/Country" $AUTH name="Lithuania"
#| HTTP/1.1 201 Created
#|
#| {
#|     "name": "Lithuania"
#| }
# FIXME: We can't save `name`, because no souch thing is defined in manifest Done
#        table. Only `name@lt` or `name@en` can be given. We could accept
#        `name` if there would be only one language option defined.

http GET "$SERVER/$DATASET/Country"
#| HTTP/1.1 401 Unauthorized
# FIXME: Probably related to the above issue, the access level is lost.

http GET "$SERVER/$DATASET/Country" $AUTH
#| HTTP/1.1 200 OK
#| 
#| {
#|     "_data": [
#|         {
#|             "name": "Lithuania"
#|         }
#|     ]
#| }
# FIXME: Same here, `name@lang` should be returned.

http POST "$SERVER/$DATASET/Country" $AUTH <<'EOF'
{
    "name@en": "Lithuania"
}
EOF
#| HTTP/1.1 400 Bad Request
#|
#| "message": "Unknown property 'name@en'.",
# FIXME: This should be accepted. Done

http POST "$SERVER/$DATASET/Country" $AUTH <<'EOF'
{
    "name": {
        "en": "Lithuania"
    }
}
EOF
#| HTTP/1.1 201 Created

psql -h localhost -p 54321 -U admin spinta -c 'SELECT name FROM "'$DATASET'/Country";'
#|         name         
#| ---------------------
#|  "Lithuania"
#|  {"en": "Lithuania"}
# FIXME: In database, we should save `("en": "Lithuania"}`, not `"Lithuania"`.

http GET "$SERVER/$DATASET/Country?select(name)&format(ascii)" $AUTH
#| name.lt    name.en 
#| ===================
#| None      None     
#| None      Lithuania
# FIXME; Show only `name` and select correct language from Accept-Language
#        header.

http GET "$SERVER/$DATASET/Country?select(name@en)&format(ascii)" $AUTH
#| HTTP/1.1 500 Internal Server Error
tail -200 $BASEDIR/spinta.log
#| Traceback (most recent call last):
#|   File "spinta/api.py", line 85, in homepage
#|     params: UrlParams = prepare(context, UrlParams(), Version(), request)
#|   File "spinta/urlparams.py", line 35, in prepare
#|     parse_url_query(urllib.parse.unquote(request.url.query))
#|   File "spinta/urlparams.py", line 77, in parse_url_query
#|     rql = spyna.parse(query)
#|   File "spinta/spyna.py", line 86, in parse
#|     ast = _parser.parse(rql)
#|   File "lark/lark.py", line 581, in parse
#|     return self.parser.parse(text, start=start, on_error=on_error)
#|   File "lark/lexer.py", line 398, in next_token
#|     raise UnexpectedCharacters(lex_state.text, line_ctr.char_pos, line_ctr.line, line_ctr.column,
#| lark.exceptions.UnexpectedCharacters: No terminal matches '@' in the current parser context, at line 1 col 12
#| 
#| select(name@en)&format(ascii)
#|            ^
#| Expected one of: 
#|         * RPAR
#|         * LPAR
#|         * VBAR
#|         * TERM
#|         * FACTOR
#|         * COMP
#|         * COMMA
#|         * LSQB
#|         * AMPERSAND
#|         * DOT
#|         * COLON
#| 
#| Previous tokens: Token('NAME', 'name')
# FIXME: Fix the grammer to allow @ as an attribute separator. DONE


http GET "$SERVER/$DATASET/Country?name='Lithuania'" $AUTH
#| HTTP/1.1 400 Bad Request
#| "message": "Invalid value.",
# FIXME: This should work.

http GET "$SERVER/$DATASET/Country?sort(name)" $AUTH

