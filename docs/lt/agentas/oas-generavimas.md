# OAS generavimas duomenų paslaugai

Kad duomenų paslaugą būtų galima įdiegti į vartus, iš Spinta manifesto
sugeneruojamas **vienas OpenAPI 3.0 (OAS) failas vienai duomenų paslaugai**, apimantis
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

(oas-config)=
## Konfigūracijos failas

Manifeste nėra aplinkų adresų, paslaugos lygmens aprašo ir autorizacijos
serverio – tai nurodoma atskirame YAML faile. Pavyzdinis failas yra Spinta
pakete, `spinta/manifests/open_api/udts_cfg.example.yml`.

**Failas privalomas**, ir jame privalomi trys dalykai:

- **`info.title`** – vardas, kuriuo paslauga matoma vartuose;
- **`info.contact`** su visais laukais: `name` – institucijos, į kurią
  kreipiasi paslaugos naudotojai, pavadinimas, `url` – jos interneto svetainė
  ir `email` – adresas, kuriuo jai rašoma;
- **`servers`** – lygiai vienas įrašas; iš jo vartai išsiveda API
  context-path.

Be jų aprašo į vartus įkelti nepavyktų, todėl `spinta udts oas` to nė
nebando ir nutraukia darbą su aiškia klaida. `--list` konfigūracijos
nereikalauja – jis tik parodo, kokias paslaugas mato manifeste.

`servers[].url` – **vartų** bazinis adresas, ne agento. Vartai jį rodo kūrėjų
portalo dokumentacijoje, o `endpoint.targetUrl` nustatomas rankomis. Serveris
nurodomas vienas: kelis vartai naudotų pakaitomis (round-robin).

`servers` adresą galima nurodyti dviem būdais:

- **tik adresas, be kelio** – tuomet prie jo prilipdomas `--path` paslaugos
  kelias. Taip vienas failas tinka visoms to paties agento paslaugoms;
- **pilnas adresas su paslaugos keliu** – naudojamas toks, koks yra. Jei jo
  kelias nesutampa su `--path`, parodomas įspėjimas.

Pavyzdys su visais palaikomais laukais:

```yaml
info:
  title: JADIS duomenų paslauga                # privaloma
  summary: Juridinių asmenų dalyvių informacinės sistemos duomenų paslauga.
  description: |
    Išsamesnis paslaugos aprašas, matomas vartų dokumentacijoje.
  version: "1"
  termsOfService: https://www.registrucentras.lt/naudojimo-salygos
  contact:                                     # privaloma, visi trys laukai
    name: Registrų centras
    url: https://www.registrucentras.lt/
    email: info@registrucentras.lt
  license:
    name: CC-BY 4.0
    url: https://creativecommons.org/licenses/by/4.0/

servers:                                       # privaloma, lygiai vienas įrašas
  - url: https://get.data.gov.lt
    description: Vartai

auth:
  token_url: https://am.example.lt/auth/token

limits:
  max_limit: 100000

externalDocs:
  description: UAPI specifikacija
  url: https://ivpk.github.io/uapi
```

| Laukas | Privalomas | Paaiškinimas |
|---|---|---|
| `info.title` | taip | Vardas, kuriuo paslauga matoma vartuose. |
| `info.summary` | ne | Trumpas paslaugos aprašas. OpenAPI 3.0 `info` šio lauko neturi, todėl jis tampa pirma `info.description` pastraipa. |
| `info.description` | ne | Išsamesnis paslaugos aprašas. Nenurodžius, lieka bendras Spintos tekstas apie UDTS agento duomenų paslaugą. |
| `info.version` |  | Paslaugos aprašo versija. |
| `info.termsOfService` | ne | Nuoroda į paslaugos naudojimo sąlygas; pilnas URL. |
| `info.contact.name` | taip | Institucijos, į kurią kreipiasi paslaugos naudotojai, pavadinimas. |
| `info.contact.url` | taip | Tos institucijos interneto svetainė; pilnas URL. |
| `info.contact.email` | taip | Adresas, kuriuo institucijai rašoma. |
| `info.license.name` | ne\* | Licencijos pavadinimas. Nenurodžius `info.license`, lieka CC-BY 4.0. |
| `info.license.url` | ne | Nuoroda į licencijos tekstą; pilnas URL. |
| `servers[].url` | taip | Vartų bazinis adresas, ne agento; tik adresas arba pilnas adresas su paslaugos keliu (žr. aukščiau). |
| `servers[].description` | ne | Aplinkos pavadinimas, pavyzdžiui, `Testavimo`. |
| `auth.token_url` | ne\*\* | Adresas, kuriuo gavėjas gauna token'ą. Žr. {ref}`Token'o adresas <oas-token-url>`. |
| `limits.max_limit` | ne | Didžiausias `_limit`, kurį vartai praleis; sveikasis skaičius nuo 1. Nenurodžius – 100000. |
| `externalDocs.url` | ne\* | Nuoroda į išorinę dokumentaciją; pilnas URL. Nenurodžius `externalDocs`, lieka UAPI specifikacijos nuoroda. |
| `externalDocs.description` | ne | Nuorodos aprašas. |

\* Privalomas, jei nurodomas jį gaubiantis objektas (`info.license` ar `externalDocs`).

\*\* Privalomas, kai autorizacijos serveris yra kitur, gavėjas kreipiasi tiesiai į agentą arba `servers` adresas reliatyvus.

Laukai, kurių OpenAPI neapibrėžia, į specifikaciją nepatenka – apie tokį lauką
parodomas įspėjimas. Taip pastebimos rašybos klaidos (`titel` vietoj `title`),
kurios kitaip tyliai paliktų lauką neužpildytą.

`x-` plėtiniai išsaugomi ten, kur juos apibrėžia pati OpenAPI, t. y. `info`,
`info.contact`, `info.license`, `servers` įrašuose ir `externalDocs`. Failo
viršuje ir `auth` viduje jie neturi kur patekti – `auth` yra ne OpenAPI
objektas, o mūsų pačių laukas, – tad apie juos parodomas toks pat įspėjimas
kaip apie bet kurį nežinomą lauką.

Kiekvienai aplinkai generuojamas atskiras aprašas su savo `url`, todėl OpenAPI
`servers[].variables` šablonai nenaudojami – nurodyti juos galima, bet jie praleidžiami.

(oas-token-url)=
### Token'o adresas: `auth.token_url`

`auth.token_url` – adresas, kuriuo duomenų gavėjas gauna token'ą. Jis įrašomas į
`components.securitySchemes.UAPI_auth` kaip `clientCredentials` srauto
`tokenUrl`.

```yaml
auth:
  token_url: https://am.example.lt/auth/token
```

Nenurodžius, adresas išvedamas iš `servers` įrašo ir `/token`, t. y. iš to
paties adreso, kuriuo token'ą per vartus pasiekia gavėjas.

Nurodyti **būtina**:

- kai autorizacijos serveris yra kitur, ne už vartų;
- kai gavėjas kreipiasi tiesiai į agentą – agentas token'ą aptarnauja adresu
  `/auth/token`, ne `/token`;
- kai `servers` adresas nurodytas reliatyviu keliu – tada absoliutaus adreso
  išvesti nėra iš ko, o OpenAPI token'o adresą apibrėžia kaip absoliutų.

Adresas turi būti pilnas, su schema ir hostu. Kliento identifikatorius ir
slaptažodis siunčiami atviru tekstu, todėl RFC 6749 (2.3.1) reikalauja TLS:
`http` adresas priimamas, bet parodomas įspėjimas, kad kredencialai eis atviru
tekstu.

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

## Papildomos pastabos

(oas-visibility)=
### Kas patenka į aprašą: `visibility`

Į aprašą patenka tik modeliai, savybės ir enum reikšmės, kurių `visibility` yra
`protected`, `package` arba `public`; tuščias `visibility` reiškia `private`, todėl
nepažymėti elementai į aprašą nepatenka. `visibility` valdo tik metaduomenų
matomumą, o duomenų prieigą valdo `access`: lauką, kurio `access` tai leidžia, API
grąžins, net jei aprašas jo nemini.

### Užklausų tikrinimas vartuose

Viskas, ką gavėjas **atsiunčia**, apraše apribota, kad vartai galėtų atmesti
netinkamą užklausą dar nepasiekusią paslaugos:

| Kur | Riba |
|---|---|
| `_limit` | nuo 1 iki `limits.max_limit` (pagal nutylėjimą 100000) |
| `_select`, `_sort` | vardai, keliai su taškais ir funkcijos, iki 1000 simbolių |
| `_page` | puslapio žymė iš ankstesnio atsakymo `_page.next` (URL saugus Base64 su `=` užpildu) |
| `{id}` | UUID v4, kai identifikatorių duoda pati Spinta. Kai modelis deklaruoja savo `_id`, riba priklauso nuo tipo: `string` ir `base32` – vienas kelio segmentas be pasvirojo brūkšnio, iki 512 simbolių; `integer` – sveikasis skaičius, telpantis į 64 bitus; `uuid` – kanoninė UUID v4 forma mažosiomis. `=` prefiksas – tik `base32` ir `string` tipo `_id`, kai modelio raktas nesudėtinis (žr. `is_accessible_by_equals_sign`); kitų tipų `_id` siunčiamas be jo |
| `scope` | tarpais skirti scope'ai, ne ilgiau, nei visi apraše deklaruoti scope'ai kartu |
| `traceparent` | W3C trace-context forma, šešioliktainė nuo pradžios iki galo |
| `tracestate`, `Cache-Control`, `Accept-Language` | spausdinami ASCII simboliai, iki 1024 |

`_select`, `_limit`, `_sort` ir `_page` apraše yra atskiri užklausos
parametrai, tad vartai kiekvieną tikrina atskirai. Filtras pagal savybes
(`?kodas='LT'`), `count()` ir iškvietimo formos (`limit(10)`, `page('…')`)
fiksuoto pavadinimo neturi, todėl aprašyti sąrašo operacijos aprašyme.

Viršutinė `_limit` riba yra **vartų politika, o ne Spintos elgsena** – Spinta
atsako į bet kokį didesnį už nulį limitą. Todėl ji nurodoma konfigūracijoje:

```yaml
limits:
  max_limit: 10000
```

Atsakymų laukams ribų nededama: jų formos aprašo manifestas, ir bet koks
spėjimas ten reikštų, kad vartai atmestų teisėtus duomenis.

## Greita instrukcija

1. Nurodykite agento konfigūracijos failą – tą patį `config.yml`, kurį naudoja
   agentas:

   ```sh
   export SPINTA_CONFIG=/opt/spinta/config.yml
   ```

   Praleidus šį žingsnį, Spinta neranda manifeste minimų backend'ų ir generavimas
   nutrūksta su klaida (pavyzdžiui, `BackendNotFound`), iš kurios nematyti, kad
   trūksta konfigūracijos.

2. Pažiūrėkite, kokios paslaugos yra manifeste:

   ```sh
   spinta udts oas manifest.csv --list
   ```

3. Paruoškite OAS konfigūracijos failą `vartai.yml` (tai ne agento `config.yml`).
   Užtenka privalomų laukų ir token'o adreso:

   ```yaml
   info:
     title: JADIS duomenų paslauga
     contact:
       name: Registrų centras
       url: https://www.registrucentras.lt/
       email: info@registrucentras.lt

   servers:
     - url: https://get.data.gov.lt

   auth:
     token_url: https://am.example.lt/auth/token
   ```

   Visi laukai aprašyti skyriuje {ref}`Konfigūracijos failas <oas-config>`.

4. Sugeneruokite OAS pasirinktai paslaugai:

   ```sh
   spinta udts oas manifest.csv -o at280.json \
       --path datasets/gov/rc/jadis/at280/1 \
       --udts-cfg vartai.yml
   ```

5. Peržiūrėkite generuojant parodytus įspėjimus. Jei apraše trūksta modelių ar
   savybių, DSA patikrinkite jų `visibility` (žr. {ref}`visibility <oas-visibility>`).
