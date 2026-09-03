# OAS generavimas duomenų paslaugai

Kad duomenų paslaugą būtų galima įdiegti į vartus, iš Spinta manifesto
sugeneruojamas **vienas OpenAPI (OAS) failas vienai duomenų paslaugai**, apimantis
visus tos paslaugos duomenų rinkinius.

Duomenų paslaugą apibrėžia kelio priekinė dalis:

```
https://{domain}/datasets/{form}/{org}/{is}/{service}/{version}/{dataset}/{model}
                 └──────────── duomenų paslauga ────────────┘ └── turinys ──┘

/datasets/gov/rc/jadis/at280/1  /  at280_israsas/DalyvioAsmensIsrasas
```

Viename manifeste gali būti kelios paslaugos (net kelios informacinės sistemos),
todėl generuojant nurodoma, kurios paslaugos OAS reikia.

## Kokios paslaugos yra manifeste

```sh
spinta udts oas manifest.csv --list
```

```
datasets/gov/rc/jadis/at280/1
  datasets/gov/rc/jadis/at280/1/at280_adresai
  datasets/gov/rc/jadis/at280/1/at280_israsas
datasets/gov/rc/ntr/n249/1
  datasets/gov/rc/ntr/n249/1/n249_israsas
```

## Generavimas

```sh
spinta udts oas manifest.csv -o at280.json \
    --path datasets/gov/rc/jadis/at280/1 \
    --udts-cfg vartai.yml
```

- `--path` – duomenų paslaugos kelias. Atrenkami visi tos paslaugos rinkiniai:
  kurių pavadinimas lygus keliui arba prasideda juo, po kurio eina `/`. Todėl
  `.../at280/1` nepagauna `.../at280/10`, o `.../at280` be versijos nepagauna
  versijuotos `.../at280/1` paslaugos. Jei manifeste yra tik viena paslauga,
  `--path` galima praleisti.
- `-o` – išvesties failas. `.yml` arba `.yaml` plėtinys duoda YAML, kitu atveju
  rašomas JSON. Be `-o` specifikacija spausdinama į standartinę išvestį.
- `--udts-cfg` – konfigūracijos failas (žr. žemiau). **Privalomas**; vienintelė
  išimtis – `--list`, kuris dokumento nerašo.
- `--api-version` – `info.version` reikšmė.

## Konfigūracijos failas

Manifeste nėra aplinkų adresų, paslaugos lygmens aprašo ir autorizacijos
serverio – tai nurodoma atskirame YAML faile. Pavyzdinis failas yra Spinta
pakete, `spinta/manifests/open_api/udts_cfg.example.yml`.

**Failas privalomas**, ir jame privalomi du dalykai:

- **`info.title`** – vardas, kuriuo paslauga matoma vartuose;
- **`servers`** – bent vienas įrašas; iš pirmojo vartai išsiveda API
  context-path.

Be jų aprašo į vartus įkelti nepavyktų, todėl `spinta udts oas` to nė
nebando ir nutraukia darbą su aiškia klaida. `--list` konfigūracijos
nereikalauja – jis tik parodo, kokias paslaugas mato manifeste.

```yaml
info:
  title: JADIS duomenų paslauga
  summary: Juridinių asmenų dalyvių informacinės sistemos duomenų paslauga.
  version: "1"

servers:
  - url: https://get.data.gov.lt
    description: Gamybinė (išoriniai vartai)
  - url: https://test-get.data.gov.lt
    description: Testavimo
```

Laukai, kurių OpenAPI neapibrėžia, į specifikaciją nepatenka – apie tokį lauką
parodomas įspėjimas. Taip pastebimos rašybos klaidos (`titel` vietoj `title`),
kurios kitaip tyliai paliktų lauką neužpildytą.

`x-` plėtiniai išsaugomi ten, kur juos apibrėžia pati OpenAPI, t. y. `info`,
`info.contact`, `info.license`, `servers` įrašuose ir `externalDocs`. Failo
viršuje ir `auth` viduje jie neturi kur patekti – `auth` yra ne OpenAPI
objektas, o mūsų pačių laukas, – tad apie juos parodomas toks pat įspėjimas
kaip apie bet kurį nežinomą lauką.

Kiekviena aplinka aprašoma savo `url`, todėl OpenAPI `servers[].variables`
šablonai nenaudojami – nurodyti juos galima, bet jie praleidžiami.

`auth.token_url` neprivalomas: nenurodžius, jis išvedamas iš pirmojo `servers`
įrašo ir `/:token`, t. y. iš to paties adreso, kuriuo token'ą per vartus pasiekia
gavėjas. Nurodyti verta tik tada, kai autorizacijos serveris yra kitur.
Nurodytas adresas turi būti pilnas ir su HTTPS – reliatyvų adresą leidžia tik
`servers` įrašai, o OpenAPI schema token'o adresą apibrėžia kaip absoliutų.

`servers` – po vieną įrašą kiekvienai aplinkai. Adresą galima nurodyti dviem
būdais:

- **tik adresas, be kelio** – tuomet prie jo prilipdomas `--path` paslaugos
  kelias. Taip vienas failas tinka visoms to paties agento paslaugoms;
- **pilnas adresas su paslaugos keliu** – naudojamas toks, koks yra. Jei jo
  kelias nesutampa su `--path`, parodomas įspėjimas.

## Scope'ai specifikacijoje

Kiekviena duomenų operacija specifikacijoje nurodo scope'ą, kurio Spinta iš
tikrųjų reikalauja – jis sudaromas iš modelio (ar savybės) ir veiksmo tuo pačiu
formatteriu, kurį naudoja pati autorizacija, tad seka ir `scope_prefix_udts` bei
`scope_max_length` nustatymus.

Vienas atvejis nusipelno paaiškinimo. Kolekcijos skaitymas autorizuojamas
skirtingai, priklausomai nuo užklausos:

```
GET /at280_israsas/Adresas              → …/:getall
GET /at280_israsas/Adresas?limit(10)    → …/:search
```

Tai ta pati operacija, o OpenAPI neturi būdo susieti reikalaujamo scope'o su
užklausos parametrais. Todėl specifikacijoje abu scope'ai surašomi kaip **dvi
alternatyvos**, o operacijos aprašyme pasakyta, kuris kuriai užklausos formai
priklauso. Alternatyvos nėra sukeičiamos: tokenui reikia to scope'o, kurį
atitinka jo daroma užklausa.

Praktinė pasekmė: jei vartai kada nors bus sukonfigūruoti scope'us tikrinti
pagal šį failą, jie gali praleisti užklausą, kurią Spinta atmes su 403.
Alternatyva – reikalauti abiejų scope'ų – elgtųsi blogiau: vartai atmestų
užklausą, kurią Spinta būtų aptarnavusi, o klaida ateitų iš vartų, tad nė
nesimatytų, kad API ją priima. Sprendžia ir tikrina Spinta; specifikacija čia
aprašo sutartį.

## Ką su failu daryti vartuose

Tas pats failas naudojamas dviem paskirtim:

1. **Endpoint'ų importas.** Vartai API `context-path` išsiveda iš **pirmojo**
   `servers[].url` kelio dalies, todėl ji turi būti lygi paslaugos keliui.
   Importuojant pažymima „Create policies on path", kad būtų sukurti visi
   specifikacijoje aprašyti keliai.
2. **Validacija.** Tas pats failas pridedamas kaip Content Provider Inline
   Resource ir naudojamas `OpenAPI Specification Validation` politikos –
   ir Request, ir Response fazėse.

Kadangi `paths` yra reliatyvūs paslaugos bazei, rankomis jų karpyti nereikia.
Jei paslaugos kelias vartuose vis dėlto skiriasi, jį galima nurodyti politikos
`basePath` lauke.

Agento lygmens endpoint'ai (`/version`, `/health`, `/auth/token`) guli agento
šaknyje, o ne po paslaugos keliu. Specifikacijoje kiekvienas jų aprašomas
**dukart**, nes failą skaito du skirtingi vartotojai:

- **`/:version`, `/:health`, `/:token`** – forma, kuria vartai juos
  maršrutizuoja paslaugos viduje. Šie keliai eina nuo dokumento `servers`, t. y.
  nuo paslaugos bazės;
- **`/version`, `/health`, `/auth/token`** – adresai, kuriais juos aptarnauja
  pati Spinta. Šie keliai turi savo `servers` įrašą (paslaugos kelias
  nukirptas), tad veikia ir kreipiantis tiesiai į agentą – pavyzdžiui,
  įsikėlus specifikaciją į Postmaną.

Vartuose `:`-formai reikia Dynamic Routing taisyklių:

| Match expression | Redirect to |
|---|---|
| `/:version` | `{#api.properties['uapi_version']}` |
| `/:health` | `{#api.properties['uapi_health']}` |
| `/:token` | `{#api.properties['uapi_token']}` |
| `/(.*)` | `{#api.properties['uapi_data_prefix']}{#group[0]}` |

`/health` atsako visada `200`; ar paslauga sveika, sako `healthy` laukas, nes
`503` reiškia, kad paslauga apskritai neatsakė. Tikrinantis komponentas turi
skaityti `healthy`, o ne atsakymo kodą.

## Užklausų tikrinimas vartuose

Viskas, ką gavėjas **atsiunčia**, apraše apribota, kad vartai galėtų atmesti
netinkamą užklausą dar nepasiekusią paslaugos:

| Kur | Riba |
|---|---|
| `_limit` | nuo 1 iki `limits.max_limit` (pagal nutylėjimą 100000) |
| `_select`, `_sort` | vardai, keliai su taškais ir funkcijos, iki 1000 simbolių |
| `{id}` | vienas kelio segmentas be pasvirojo brūkšnio, iki 512 simbolių; modeliams su savu `_id` – su `=` prefiksu |
| `scope` | tarpais skirti scope'ai |
| `traceparent` | W3C trace-context forma, šešioliktainė nuo pradžios iki galo |
| `tracestate`, `Cache-Control`, `Accept-Language` | spausdinami ASCII simboliai, iki 1024 |

Viršutinė `_limit` riba yra **vartų politika, o ne Spintos elgsena** – Spinta
atsako į bet kokį didesnį už nulį limitą. Todėl ji nurodoma konfigūracijoje:

```yaml
limits:
  max_limit: 10000
```

Atsakymų laukams ribų nededama: jų formos aprašo manifestas, ir bet koks
spėjimas ten reikštų, kad vartai atmestų teisėtus duomenis.

`components.securitySchemes.UAPI_auth` token'o adresas išvedamas iš pirmojo
`servers` įrašo, t. y. `:`-forma. Kreipiantis tiesiai į agentą, tinkamą adresą
reikia nurodyti `auth.token_url` lauke.
