psql -h localhost -p 54321 -U admin postgres -c 'DROP DATABASE spinta_external'
psql -h localhost -p 54321 -U admin postgres -c 'CREATE DATABASE spinta_external'

# export BASEDIR=x
#

cat > "$BASEDIR"/manifest.csv <<EOF
id,dataset,resource,base,model,property,type,ref,source,prepare,level,access,uri,title,description
,datasasets/gov/ivpk/svetainiu_atitikties_vertinimas,,,,,,,,,,,,,
,,,,,,,,,,,,,,
,,,,,,prefix,spinta,,,,,https://github.com/atviriduomenys/spinta/issues/,,
,,,,,,,,,,,,,,
,,csv,,,,csv,,{dist.dowload_url},tabular(sep: ';'),,,,,
,,,,,,comment,type,,"create(type: ""param"", ref: ""dist"", source: ""/datasets/gov/ivpk/adp/catalog/DatasetDistribution"")",4,open,Spinta:210,,
,,,,,,,,,,,,,,
,,svetaine,,,,xlsx,,https://data.gov.lt/dataset/1002/download/14243/Svetainiu%20Atitikties_vertinimo%20kriterijams%20lentele.xlsx,,,,,,
,,,,Sritis,,,,Pagrindinis,,,,,,
,,,,,pavadinimas,string,,,,1,open,,Srities pavadinimas,
,,,,,sritis,ref,Sritis,,,1,open,,,
,,,,Svetaine,,,,Pagrindinis,,,,,,
,,,,,pavadinimas,string,,,,1,open,,Svetainės pavadinimas,
,,,,,nuoroda,string,,,,1,open,,Nuoroda į svetainę,
,,,,,sritis,ref,Sritis,,,1,open,,Svetainės sritis,
,,,,Kategorija,,,,Pagrindinis,,,,,,
,,,,,pavadinimas,string,,,,1,open,,Kategorijos pavadinimas,
,,,,,kategorija,ref,Kategorija,,,1,open,,,
,,,,Kriterijus,,,,Pagrindinis,,,,,,
,,,,,pavadinimas,string,,,,1,open,,Kriterijaus pavadinimas,
,,,,,kategorija,ref,Kategorija,,,1,open,,Kategorija kuriai priklauso kriterijus,
,,,,,atitinka,boolean,,,,1,open,,Atitikmuo,
,,,,,,enum,,0,false,,,,Neatitinka,
,,,,,,,,1,true,,,,Atitinka,
EOF

cat > "$BASEDIR"/config.yml <<EOF
env: production
data_path: $BASEDIR

keymaps:
  default:
    type: sqlalchemy
    dsn: sqlite:///$BASEDIR/keymap.db

backends:
  external:
    type: sql
    dsn: postgresql://admin:admin123@localhost:54321/spinta_external

manifest: default
manifests:
  default:
    type: csv
    path: $BASEDIR/manifest.csv
    backend: external
    keymap: default
    mode: external

accesslog:
  type: file
  file: $BASEDIR/accesslog.json
EOF

export SPINTA_CONFIG=$BASEDIR/config.yml

spinta bootstrap

