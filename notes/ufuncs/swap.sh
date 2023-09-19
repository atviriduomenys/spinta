# notes/sqlite.sh           Configure SQLite

INSTANCE=ufuncs/swap
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

rm $BASEDIR/db.sqlite
sqlite3 $BASEDIR/db.sqlite <<'EOF'
CREATE TABLE IF NOT EXISTS cities (
    id           INTEGER PRIMARY KEY,
    name         TEXT
);
INSERT INTO cities VALUES (1, 'Vilnius');
INSERT INTO cities VALUES (2, 'Vėlnius');
INSERT INTO cities VALUES (3, 'Vilna');
EOF
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM cities;"

cat > $BASEDIR/manifest.txt <<EOF
d | r |m | property | type    | ref | source                        | prepare         | level | access
$DATASET            |         |     |                               |                 |       |
  | db              | sql     |     | sqlite:///$BASEDIR/db.sqlite  |                 |       |
  |   |City         |         | id  | cities                        |                 | 4     |
  |   |  | id       | integer |     | id                            |                 | 4     | open
  |   |  | name     | string  |     | name                          |                 | 4     | open
  |   |  |          |         |     | Vėlnius                       | swap("Vilnius") | 4     | open
  |   |  |          |         |     | Vilna                         | swap("Vilnius") | 4     | open
  |   |  |          |         |     | Kauna                         | swap("Kaunas")  | 4     | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
#| lark/parser_frontends.py:106 in parse                                                                                            │
#| 
#|   103 │   │   chosen_start = self._verify_start(start)
#|   104 │   │   stream = text if self.skip_lexer else LexerThread(self.lexer, text)
#|   105 │   │   kw = {} if on_error is None else {'on_error': on_error}
#| ❱ 106 │   │   return self.parser.parse(stream, chosen_start, **kw)
#|   107 │
#|   108 │   def parse_interactive(self, text=None, start=None):
#|   109 │   │   chosen_start = self._verify_start(start)
#| 
#| ╭──────────────────────────────────────── locals ────────────────────────────────────────╮
#| │         text = 'swap(Vėlnius, "Vilnius").swap(Vilna, "Vilnius").swap(Kauna, "Kaunas")' │
#| ╰────────────────────────────────────────────────────────────────────────────────────────╯
#| UnexpectedCharacters: No terminal matches 'ė' in the current parser context, at line 1 col 7
#| 
#| swap(Vėlnius, "Vilnius").swap(Vilna, "Vilnius"
#|       ^
# FIXME: Values coming from `source` must be enclosed with quoates.
cat $BASEDIR/manifest.csv
poetry run spinta show
