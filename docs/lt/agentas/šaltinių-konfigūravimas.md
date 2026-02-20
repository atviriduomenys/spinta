# Šaltinių konfigūravimas

Tam, kad Agentas galėtų pasiekti duomenis ir teikti juos UDTS formatu, reikia nurodyti, kokie duomenys bus teikiami ir kaip juos pasiekti. Tai daroma naudojantis duomenų struktūros aprašais (DSA). Šiuo atveju, mums reikės Šaltinio duomenų struktūros aprašų (ŠDSA).

Kaip nurodyta konfigūracijos faile, turite pateikti duomenų struktūros aprašą, kurio pagrindu veiks agentas.

Spinta palaikomi šaltiniai:

- SQL
- WSDL/SOAP
- XML
- JSON

## Manifest konfigūravimas

Manifest — tai CSV formato failas, kuriame aprašyta, **kokius duomenis** agentas teiks ir **kaip juos pasiekti**. Jis atitinka [DSA 1.1 specifikaciją](https://ivpk.github.io/dsa/1.1/ "DSA 1.1 specifikacija").

### Prisijungimo duomenys saugomi config.yml, ne manifest faile

Prisijungimo duomenys (DSN su slaptažodžiu) **niekada nerašomi į manifest.csv**. Manifest faile nurodomas tik backend'o **pavadinimas** — nuoroda į `config.yml`, kurį mato tik sistemos administratorius.

```{figure} ../static/manifest-architektura.png
:width: 100%
:alt: config.yml ir manifest.csv ryšys — slaptažodžiai atskirti nuo DSA struktūros
:target: ../_static/manifest-architektura.png

config.yml ir manifest.csv ryšys (spustelėkite norėdami padidinti)
```

**config.yml** (tik administratorius):

```yaml
backends:
  myapp_db:
    type: sql
    dsn: postgresql+psycopg2://user:slaptazodis@localhost:5432/myapp
  products_db:
    type: sql
    dsn: postgresql+psycopg2://user:slaptazodis@localhost:5433/products

manifests:
  default:
    type: csv
    path: /opt/spinta/manifest.csv
    backend: myapp_db
    mode: external
```

**manifest.csv** (veiklos žmonės gali matyti ir redaguoti — jokių slaptažodžių):

```
id,dataset,...,source,...
,datasets/gov/lt/myapp,,,,,,,,,,,,,,,,,,,, ← vardų erdvė
,,myapp_db,,,,sql,myapp_db,,,,,,,,,,,,,,   ← nuoroda į backend pavadinimą
```

### Keli duomenų šaltiniai viename manifest faile

Vienas agentas gali teikti duomenis iš **kelių šaltinių** — jų skaičius neribojamas. Kiekvienas šaltinis aprašomas atskira `resource` eilute su savo backend'o pavadinimu. Visi backend'ų pavadinimai ir jų DSN yra `config.yml` faile.

```{figure} ../static/manifest-struktura.png
:width: 100%
:alt: manifest.csv su dviem backend'ais — eilučių struktūra
:target: ../_static/manifest-struktura.png

manifest.csv su dviem šaltiniais viename faile (spustelėkite norėdami padidinti)
```

Manifest faile kiekvienas šaltinis turi savo blokų seką:

```
dataset eilutė  → vardų erdvė (pvz. datasets/gov/lt/myapp)
resource eilutė → backend pavadinimas (pvz. myapp_db) + šaltinio tipas (sql/wsdl/xml/json)
(tuščia eilutė) → vizualinis atskyriklis
model eilutė    → duomenų objektas (lentelė/klasė)
property eilutės → laukai (stulpeliai)
```

Jei norite pridėti antrą šaltinį — tiesiog tęskite tą patį failą nauju dataset/resource bloku.

### Manifest CSV stulpelių struktūra

Manifest CSV turi tiksliai **21 stulpelį** pagal DSA 1.1 specifikaciją:

```
id, dataset, resource, base, model, property, type, ref, source, source.type,
prepare, origin, count, level, status, visibility, access, uri, eli, title, description
```

**Stulpelių eilės tvarka** — nesvarbi. Spinta skaito pagal stulpelio **pavadinimą**, ne poziciją.

**Praleisti stulpeliai** — leidžiama. Stulpeliai, kurių nėra antraštėje, automatiškai gauna tuščią reikšmę `""`. Jūs neprivalote įtraukti visų 21 stulpelio — tik tuos, kuriuos naudojate.

**Papildomi (savi) stulpeliai pastaboms** — leidžiama, bet tik jei antraštėje yra **visi 21 standartiniai stulpeliai**, o savas stulpelis eina **22 pozicijoje ar vėliau**. Spinta tokį stulpelį ignoruos — jis skirtas tik žmonėms (pvz. audito žymoms, komentarams):

```
id,...,description,original_access  ← 22-as stulpelis, Spinta neskaitys
```

:::{important}
Kiekviena eilutė privalo turėti **lygiai tiek reikšmių** kiek yra antraštėje — net jei reikšmė tuščia. Jei antraštė turi 22 stulpelius, kiekvienoje eilutėje turi būti 22 kableliais atskirtos reikšmės.
:::

**Sutrumpinti stulpelių pavadinimai** — Spinta priima trumpinius:

| Trumpinys | Pilnas pavadinimas |
|-----------|-------------------|
| `d` | `dataset` |
| `r` | `resource` |
| `b` | `base` |
| `m` | `model` |
| `p` | `property` |
| `t` | `type` |

---

## WSDL ir SOAP šaltiniai

WSDL ir SOAP šaltinio struktūros parengimas aprašytas čia:

[Duomenų šaltiniai - DSA](https://ivpk.github.io/dsa/1.1/saltiniai.html#wsdl "Duomenų šaltiniai - DSA 1.1")

:::{note}
**Žemiau pateiktas manifest pavyzdys yra skirtas pradiniam testavimui** — jis naudoja
viešai prieinamą demo WSDL paslaugą, kurią galima pasiekti be kredencialų. Jis
leidžia patikrinti ar agentas apskritai veikia teisingai dar prieš jungiantis prie
realaus šaltinio.

Kai testavimas sėkmingas — šį manifestą reikia **pakeisti** savo institucijos
realiuoju sDSA (sugeneruotu su `spinta inspect` iš jūsų šaltinio). Kaip tai padaryti
aprašyta skyriuje [Agento paruošimas](agento-paruošimas.md).
:::

Pasikeiskite aktyvų naudotoją ir katalogą:

```bash
sudo -Hsu spinta
cd
```

Struktūros aprašo, skirto WSDL duomenims gauti, sudarymo pavyzdys:

```bash
cat > manifest.csv << 'EOF'
id,dataset,resource,base,model,property,type,ref,source,prepare,level,status,visibility,access,uri,eli,title,description
,datasets/gov/vssa/demo/rctest,,,,,dataset,,,,,,,,,,,
,,rc_wsdl,,,,wsdl,,https://test-data.data.gov.lt/api/v1/rc/get-data/?wsdl,,,,,,,,,
,,get_data,,,,soap,,Get.GetPort.GetPort.GetData,wsdl(rc_wsdl),,,,,,,,
,,,,,,param,action_type,input/ActionType,input(),,,,,,,,
,,,,,,param,caller_code,input/CallerCode,input(),,,,,,,,
,,,,,,param,end_user_info,input/EndUserInfo,input(),,,,,,,,
,,,,,,param,parameters,input/Parameters,input(),,,,,,,,
,,,,,,param,time,input/Time,input(),,,,,,,,
,,,,,,param,signature,input/Signature,"creds(""sub"").input()",,,,,,,,
,,,,,,param,caller_signature,input/CallerSignature,input(),,,,,,,,
,,,,GetData,,,,/,,,,,,,,,
,,,,,response_code,string,,ResponseCode,,,,,,,,,
,,,,,response_data,string,,ResponseData,base64(),,,,,,,,
 ,,,,,decoded_parameters,string,,DecodedParameters,,,,,,,,,
 ,,,,,action_type,string,,,param(action_type),,,,,,,,
,,,,,end_user_info,string,,,param(end_user_info),,,,,,,,
,,,,,caller_code,string,,,param(caller_code),,,,,,,,
,,,,,parameters,string,,,param(parameters),,,,,,,,
,,,,,time,string,,,param(time),,,,,,,,
,,,,,signature,string,,,param(signature),,,,,,,,
,,,,,caller_signature,string,,,param(caller_signature),,,,,,,,
,,,,,,,,,,,,,,,,,
,,nested_read,,,,dask/xml,,,eval(param(nested_xml)),,,,,,,,
 ,,,,,,param,nested_xml,GetData,read().response_data,,,,,,,,
,,,,Country,,,,countries/countryData,,,,,,,,,
,,,,,id,string,,id,,,,,,,,,
,,,,,title,string,,title,,,,,,,,,
EOF
```
