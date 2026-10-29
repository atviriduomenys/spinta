# OAS generavimas duomenų paslaugai

**OAS** (OpenAPI Specification) – mašininis API aprašas. Duomenų paslaugai jis aprašo,
kokius adresus paslauga turi, kokias užklausas priima ir ką atsako. Šiame puslapyje
aprašyta, kaip jį sugeneruoti iš Spinta manifesto komanda `spinta udts oas`.

## Kam reikalingas OAS

- **Paslaugai įdiegti į vartus.** API vartuose (Gravitee) paslaugos API kuriamas importuojant
  OAS: iš jo paimami paslaugos adresai, juo tikrinamos gavėjų užklausos ir paslaugos
  atsakymai, pagal jį vartų kūrėjų portale rodoma paslaugos dokumentacija.
- **Pirmiausia – pačiai institucijai.** Paslaugas į vartus diegia institucija; VSSA suteikia
  vartų platformą ir prieigą prie jos. Prieš diegiant OAS pateikiamas **VSSA patikrai**.
- **Duomenų gavėjams.** Iš OAS gavėjas sužino, kaip kreiptis į paslaugą. Planuojama, kad
  paslaugos OAS bus pasiekiamas ir Kataloge. Todėl OAS `servers` nurodomas **vartų adresas,
  kurį mato duomenų gavėjas**, o ne vidinis agento adresas (FQDN), kuriuo vartai kreipiasi į
  agentą.

Sugeneravę OAS: patikrinkite jį (žr. {ref}`oas-tikrinimas`), pateikite VSSA patikrai ir,
gavę pritarimą, diekite paslaugą į vartus (žr. [Registracija ir diegimas į vartus](registracija.md)).

## Kuo skiriasi nuo OAS eksporto Kataloge

Kataloge, duomenų rinkinio struktūros puslapyje, yra mygtukas „Eksportuoti OpenAPI“. Tas
aprašas skirtas struktūrai peržiūrėti, **vartams jis netinka**:

| | Katalogo eksportas | `spinta udts oas` |
|---|---|---|
| Apimtis | Vienas duomenų rinkinys | Visa duomenų paslauga – visi jos rinkiniai, atrenkami pagal `--path` |
| `servers` | Nėra | Vartų adresas su paslaugos keliu |
| `/version`, `/health`, `/token` | Nėra | Yra |
| Reikalavimai | – | Suderinta su vartų (VIISP duomenų teikimo paslaugų) reikalavimais |
| Paskirtis | Struktūros peržiūra | Importas į vartus (Gravitee) |

UDTS specifikacija reikalauja, kad aprašas būtų **OpenAPI 3.0**. Vartai papildomai tikrina
OAS savo taisyklių rinkiniu (OWASP taisyklės, reguliariųjų išraiškų suderinamumas su vartų
linteriu, užklausų parametrų ribos ir kt.). `spinta udts oas` šiuos reikalavimus jau įgyvendina.

(oas-versija)=
## Kokia Spinta versija generuoti

:::{important}
`spinta udts oas` yra **nuo Spinta 1.2.0**. Senesnėmis versijomis sugeneruoti aprašai vartuose
nepriimami (juose nėra `info.description`, o `servers` adresas – `get.data.gov.lt`).

OAS generuokite **ne senesne nei Spinta 1.3.1** versija – jos sugeneruotas OAS sėkmingai
importuotas į vartus. Kintant vartų ir kitiems reikalavimams, atitinkamai atnaujinamas ir
generatorius, todėl rekomenduojama naudoti **naujausią** Spinta versiją. Jei agentas senesnis,
pirmiausia jį [atnaujinkite](atnaujinimas.md).
:::

(oas-vartai)=
## OAS ir vartai: kas į ką virsta

Duomenų paslaugą apibrėžia kelio priekinė dalis:

```
https://{domain}/datasets/{form}/{org}/{is}/{service}/{version}/{dataset}/{model}
                 └───────────── duomenų paslauga ─────────────┘ └─── turinys ───┘

/datasets/gov/rc/jadis/jadis_udts/1  /  at280_israsas/DalyvioAsmensIsrasas
```

Kelio segmentai ir dažniausios klaidos aprašyti skyriuje
{ref}`Duomenų paslaugos adresas (UDTS URL) <duomenu-paslaugos-kelias>`.

Importuojant OAS į vartus:

| OAS | Vartuose (Gravitee) |
|---|---|
| `servers[0].url` adresas, pvz., `https://apigw.gov.lt` | Vartų adresas, kuriuo kreipiasi gavėjai |
| `servers[0].url` kelias, pvz., `/datasets/gov/vssa/demo/demo_udts/1` | API **context-path** |
| `paths`, pvz., `/rctest/Country`, `/rctest/Country/{id}` | API endpoint'ai, santykiniai su context-path |
| – (OAS nėra) | **Backend** – agento adresas, į kurį vartai persiunčia užklausas, pvz., `https://spinta.institucija.lt`; nustatomas diegiant |
| Schemos, parametrai | Užklausų ir atsakymų tikrinimas (OpenAPI Specification Validation) |

Gavėjo užklausa keliauja taip:

```
GET https://apigw.gov.lt/datasets/gov/vssa/demo/demo_udts/1/rctest/Country
    └─ vartų adresas ──┘└───────── context-path ──────────┘└─ endpoint ──┘
                         ↓ vartai
GET https://spinta.institucija.lt/datasets/gov/vssa/demo/demo_udts/1/rctest/Country
```

**Išimtys – agento endpoint'ai.** `/version`, `/health` ir `/token` OAS aprašyti paslaugos
viduje (santykiniai su `servers`, kaip ir duomenys), bet agentas juos aptarnauja savo šaknyje.
Vartai juos nukreipia atskiromis (Dynamic Routing) taisyklėmis:

| Gavėjas kreipiasi | Vartai nukreipia į |
|---|---|
| `…/demo_udts/1/version` | agento `/version` |
| `…/demo_udts/1/health` | agento `/health` |
| `…/demo_udts/1/token` | žetono išdavimo adresą (agento `/auth/token` arba autorizacijos serverį – pagal vartų konfigūraciją) |

Todėl paslaugoje negali būti duomenų rinkinio ar modelio, pavadinto `version`, `health` ar
`token`, tiesiai po paslaugos keliu.

## Ar OAS būtina generuoti su Spinta

Ne. Jei Spinta nenaudojama arba institucija aprašą rengia pati, OAS galima parengti ir kitaip,
bet jam galioja tie patys reikalavimai: OpenAPI 3.0, vartų taisyklių rinkinys, kelias pagal UDTS.
`spinta udts oas` – rekomenduojamas būdas, nes šie reikalavimai jame jau įgyvendinti.

(oas-tikrinimas)=
## OAS patikrinimas

Prieš teikiant VSSA, OAS verta patikrinti OpenAPI validatoriumi, pvz., `vacuum lint` ar
[Swagger Editor](https://editor.swagger.io/). Validatorius parodo, ar aprašas atitinka
OpenAPI specifikaciją, bet **negarantuoja**, kad jis praeis vartų taisyklių rinkinio ir VSSA
patikras.

## Kai keičiasi DSA (manifestas)

Pakeitus paslaugos DSA (manifestą), OAS **generuojamas iš naujo**, o jei paslauga jau įdiegta
vartuose – paslauga vartuose **atnaujinama** nauju OAS.

:::{danger}
Paslaugos kelio (UDTS URL) keisti negalima – žr.
{ref}`Duomenų paslaugos adresas (UDTS URL) <duomenu-paslaugos-kelias>`.
:::

Viename manifeste gali būti kelios paslaugos (net kelios informacinės sistemos),
todėl generuojant nurodoma, kurios paslaugos OAS reikia.

## Kokios paslaugos yra manifeste

```sh
spinta udts oas manifest.csv --list
```

```{code-block} text
:caption: Pavyzdinės išvesties paaiškinimas

datasets/gov/rc/jadis/jadis_udts/1                  ← paslauga (jos kelias – --path reikšmė)
  datasets/gov/rc/jadis/jadis_udts/1/at280_adresai  ← šios paslaugos duomenų rinkinys
  datasets/gov/rc/jadis/jadis_udts/1/at280_israsas  ← šios paslaugos duomenų rinkinys
datasets/gov/rc/ntr/ntr_udts/1                      ← kita paslauga
  datasets/gov/rc/ntr/ntr_udts/1/n249_israsas       ← jos duomenų rinkinys
```

Šiame pavyzdyje vienas agentas aptarnauja dvi paslaugas, todėl generuojami du OAS – po vieną
kiekvienai paslaugai, su visais jos duomenų rinkiniais. Dažniausiai vienas agentas aptarnauja
vieną paslaugą.

Jei rinkinys atsiduria ne po ta paslauga, kurios tikėjotės (arba rodomas kaip atskira paslauga),
patikrinkite kelią manifeste – žr.
{ref}`Duomenų paslaugos adresas (UDTS URL) <duomenu-paslaugos-kelias>`.

## Generavimas

```sh
spinta udts oas manifest.csv -o jadis_udts.json \
    --path datasets/gov/rc/jadis/jadis_udts/1 \
    --udts-cfg vartai-jadis_udts.yml
```

- `--path` – duomenų paslaugos kelias. Atrenkami visi tos paslaugos rinkiniai:
  kurių pavadinimas lygus keliui arba prasideda juo, po kurio eina `/`. Todėl
  `.../jadis_udts/1` nepagauna nei `.../jadis_udts/2`, nei `.../jadis_udts/10` – tai kitos
  paslaugos versijos. Jei manifeste yra tik viena paslauga,
  `--path` galima praleisti.
- `-o` – išvesties failas. `.yml` arba `.yaml` plėtinys duoda YAML, kitu atveju
  rašomas JSON. Be `-o` specifikacija spausdinama į standartinę išvestį.
- `--udts-cfg` – konfigūracijos failas (žr. žemiau). **Privalomas**; vienintelė
  išimtis – `--list`, kuris dokumento nerašo.
- `--api-version` – `info.version` reikšmė.

:::{important}
**Iki kur nurodyti `--path`**

`--path` nurodomas **iki paslaugos versijos segmento** – ne iki IS, ne iki paslaugos be
versijos ir ne iki duomenų rinkinio. Tinkamos reikšmės – `--list` išvestyje paslaugomis pažymėtos eilutės. Testavimo ir gamybinio agento OAS generuojami su tuo pačiu `--path`, skiriasi
tik konfigūracijos failo `servers` adresas.

| `--path` | |
|---|---|
| `datasets/gov/vssa/demo/demo_udts/1` | ✅ pirma paslaugos versija |
| `datasets/gov/vssa/demo/demo_udts/2` | ✅ nauja, nesuderinama paslaugos versija |
| `datasets/gov/vssa/demo/demo_udts` | ❌ be versijos – versijos segmentas privalomas |
| `datasets/gov/vssa/demo` | ❌ iki IS – į aprašą patektų visos IS paslaugos |
| `datasets/gov/vssa/demo/demo_udts/1/rctest` | ❌ iki duomenų rinkinio – į aprašą patektų tik vienas rinkinys |

Kai `--path` nėra paslaugos lygio, `spinta udts oas` parodo pranešimą
„is not an UDTS data service path“ – tokio aprašo vartams neteikite.
Kelią **be versijos** `spinta udts oas` priima be įspėjimo, todėl, ar versija nurodyta,
patikrinkite patys.
:::

(oas-config)=
## Konfigūracijos failas

Manifeste nėra aplinkų adresų, paslaugos lygmens aprašo ir autorizacijos
serverio – tai nurodoma atskirame YAML faile.

Paslaugas vieną nuo kitos atskiria `--path`, todėl **kiekvienai paslaugai (kiekvienam
`--path`) ruošiamas savas konfigūracijos failas** – su savo pavadinimu ir aprašu.
Patogu failą vadinti pagal paslaugą, pvz., `vartai-jadis_udts.yml`. Pavyzdinis failas yra Spinta
pakete, `spinta/manifests/open_api/udts_cfg.example.yml`.

**Failas privalomas**, ir jame privalomi trys dalykai:

- **`info.title`** – vardas, kuriuo paslauga matoma vartų portale; rekomenduojama
  toks pat, kaip paslaugos pavadinimas Kataloge. Kiekvienai paslaugai – atskiras;
- **`info.contact`** su visais laukais: `name` – institucijos, į kurią
  kreipiasi paslaugos naudotojai, pavadinimas, `url` – jos interneto svetainė
  ir `email` – adresas, kuriuo jai rašoma;
- **`servers`** – lygiai vienas įrašas; iš jo vartai išsiveda API
  context-path.

Be jų aprašo į vartus įkelti nepavyktų, todėl `spinta udts oas` to nė
nebando ir nutraukia darbą su aiškia klaida. `--list` konfigūracijos
nereikalauja – jis tik parodo, kokias paslaugas mato manifeste.

Privaloma nurodyti ir **`info.version`** – paslaugos versiją, pvz., `"1"` (kabutėse, kad
YAML ją skaitytų kaip tekstą). Ji turi sutapti su UDTS URL versijos segmentu: kai
`--path` baigiasi `/1`, `info.version` – `"1"`. Jos `spinta udts oas` netikrina: nenurodžius įrašoma
`1.0.0`, todėl, ar ji nurodyta, patikrinkite patys.

`servers[].url` – **vartų** bazinis adresas, ne agento. Vartai jį rodo kūrėjų
portalo dokumentacijoje, o `endpoint.targetUrl` nustatomas rankomis. Serveris
nurodomas vienas: kelis vartai naudotų pakaitomis (round-robin).

`servers` adresą galima nurodyti dviem būdais:

- **tik adresas, be kelio** (rekomenduojama) – tuomet prie jo prilipdomas
  `--path` paslaugos kelias;
- **pilnas adresas su paslaugos keliu** – naudojamas toks, koks yra. Jei jo
  kelias nesutampa su `--path`, parodomas įspėjimas.

Pavyzdžiui, konfigūracijoje nurodžius

```yaml
servers:
  - url: https://apigw.gov.lt
```

ir generuojant su `--path datasets/gov/vssa/demo/demo_udts/1`, OAS faile bus:

```yaml
servers:
  - url: https://apigw.gov.lt/datasets/gov/vssa/demo/demo_udts/1
```

Tai normalu – kelias paimamas iš `--path`, konfigūracijoje jo rašyti nereikia.

(oas-adresai)=
:::{important}
**Kokius adresus nurodyti**

`servers[].url` – adresas be kelio tų vartų, į kuriuos bus diegiama paslauga.
**Vidiniai vartai** skirti KVTC klientėms (institucijoms SVDPT naudotojų sąraše),
**išoriniai** – visoms kitoms. Žr. {ref}`Vartų tipai <vartu-tipai>`.

| Vartai | Kada | `servers[].url` |
|---|---|---|
| Vidiniai, TEST | Testinė agento aplinka su testiniais duomenimis | `https://test-apigw.gov.lt` |
| Vidiniai, PROD | Produkcinė agento aplinka (testiniai arba produkciniai duomenys); testinė agento aplinka su produkciniais duomenimis | `https://apigw.gov.lt` |
| Išoriniai, PROD | Produkcinė agento aplinka (testiniai arba produkciniai duomenys); testinė agento aplinka su produkciniais duomenimis | `https://api.gov.lt` |

- Kiekvienai aplinkai generuojamas **atskiras** OAS failas su savo `servers` adresu.
- Galutinis API context-path vartuose (pvz. `/stage` priešdėlis testiniam agentui su
  produkciniais duomenimis) nustatomas diegiant paslaugą į vartus pagal VSSA nurodymus.
  Generuojant OAS jo nurodyti nereikia.
- **`auth.token_url` nenurodykite** – token'as gaunamas per vartus, o adresas išvedamas
  automatiškai (vartų adresas + `/token`).
- **`https://get.data.gov.lt` nenaudokite** – tai atvirų duomenų portalo adresas, ne vartų.
:::

Pavyzdys su visais palaikomais laukais:

```yaml
info:
  title: JADIS duomenų paslauga                # privaloma
  summary: Juridinių asmenų dalyvių informacinės sistemos duomenų paslauga.
  description: |                              # privaloma vartams
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
  - url: https://apigw.gov.lt
    description: Produkciniai vartai

auth:                                          # dažniausiai nenurodoma, žr. žemiau
  token_url: https://am.example.lt/auth/token

limits:
  max_limit: 100000

externalDocs:
  description: UAPI specifikacija
  url: https://ivpk.github.io/uapi
```

| Laukas | Privalomas | Paaiškinimas |
|---|---|---|
| `info.title` | taip | Vardas, kuriuo paslauga matoma vartų portale; rekomenduojama toks pat, kaip Kataloge. Kiekvienai paslaugai atskiras. |
| `info.summary` | ne | Trumpas paslaugos aprašas. OpenAPI 3.0 `info` šio lauko neturi, todėl jis tampa pirma `info.description` pastraipa. |
| `info.description` | taip (vartams) | Išsamesnis paslaugos aprašas. Nenurodžius įrašomas bendras angliškas tekstas; rekomenduojama nurodyti savo paslaugos aprašą lietuviškai. |
| `info.version` | taip | Paslaugos versija, sutampanti su UDTS URL versijos segmentu, pvz., `"1"`. Generatorius jos netikrina – nenurodžius įrašoma `1.0.0`. Galima nurodyti ir parametru `--api-version`. |
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

Kiekvienai OAS operacijai nurodytas scope, kurio reikia duomenims gauti. Scope pavadinimas
sudaromas taip pat, kaip jį tikrina agentas (atsižvelgiant į agento nustatymus
`scope_prefix_udts` ir `scope_max_length`), todėl OAS nurodyti scope sutampa su tais, kurių
reikalauja agentas.

Sąrašo užklausai (`GET /{rinkinys}/{Modelis}`) OAS nurodomi **du scope**, nes agentas jai taiko
skirtingą scope priklausomai nuo to, ar užklausoje yra parametrų:

| Užklausa | Reikalingas scope |
|---|---|
| `GET /rctest/Country` – be parametrų | `…/:getall` |
| `GET /rctest/Country?_limit=10` – su filtru, `_select`, `_sort`, `_limit` ar kt. | `…/:search` |

Praktiškai: duomenų gavėjui, kuris naudosis sąrašu, reikalingi **abu** scope (`:getall` ir
`:search`), kitaip dalis jo užklausų bus atmesta su klaida 403.

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

3. Paruoškite paslaugos OAS konfigūracijos failą, pvz., `vartai-jadis_udts.yml` (tai ne
   agento `config.yml`). Kiekvienai paslaugai – atskiras failas. Užtenka privalomų laukų. `servers` adresą parinkite pagal
   {ref}`lentelę <oas-adresai>`, `auth.token_url` nenurodykite:

   ```yaml
   info:
     title: JADIS duomenų paslauga
     description: Juridinių asmenų dalyvių informacinės sistemos duomenų paslauga.
     version: "1"
     contact:
       name: Registrų centras
       url: https://www.registrucentras.lt/
       email: info@registrucentras.lt

   servers:
     - url: https://apigw.gov.lt
   ```

   Visi laukai aprašyti skyriuje {ref}`Konfigūracijos failas <oas-config>`.

4. Sugeneruokite OAS pasirinktai paslaugai (`--path` – viena iš 2 žingsnio sąrašo paslaugų):

   ```sh
   spinta udts oas manifest.csv -o jadis_udts.json \
       --path datasets/gov/rc/jadis/jadis_udts/1 \
       --udts-cfg vartai-jadis_udts.yml
   ```

5. Peržiūrėkite generuojant parodytus įspėjimus. Jei apraše trūksta modelių ar
   savybių, DSA patikrinkite jų `visibility` (žr. {ref}`visibility <oas-visibility>`).
