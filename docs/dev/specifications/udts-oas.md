# UDTS duomenų paslaugos OpenAPI aprašas

Kaip Spinta iš manifesto sugeneruoja vienos UDTS duomenų paslaugos OpenAPI
aprašą. Naudotojo instrukcija (komanda, konfigūracija, darbas vartuose) –
[`docs/lt/agentas/oas-generavimas.md`](../../lt/agentas/oas-generavimas.md).

## Paskirtis

Aprašą API vartai naudoja dviem tikslais:

1. **endpoint'ams importuoti** – vartai iš aprašo sukuria API, o API context-path
   išveda iš pirmojo `servers` įrašo kelio;
2. **užklausoms ir atsakymams tikrinti** – *OpenAPI Specification Validation*
   politika Request ir Response fazėse.

Todėl aprašas turi atitikti tai, ką Spinta iš tikrųjų priima ir grąžina:
užklausų pusėje – kiek įmanoma griežtai, atsakymų pusėje – niekada ne griežčiau,
nei Spinta gali atsakyti.

## Kodas ir testai

| Kur | Kas |
|---|---|
| `spinta/cli/udts/oas.py` | CLI komanda `spinta udts oas` |
| `spinta/manifests/open_api/service.py` | duomenų paslaugos kelio logika |
| `spinta/manifests/open_api/udts_config.py` | `--udts-cfg` skaitymas ir tikrinimas (`UdtsConfig`) |
| `spinta/manifests/open_api/udts_cfg.example.yml` | pavyzdinė konfigūracija |
| `spinta/manifests/open_api/openapi_generator.py` | generatorius (`OpenAPIGenerator`) |
| `spinta/manifests/open_api/openapi_config.py` | kelių, parametrų, atsakymų, schemų šablonai |
| `spinta/manifests/open_api/helpers.py` | `create_openapi_manifest`, `write_openapi_manifest` |
| `spinta/spyna.py` | `_page=<token>` užklausos sintaksė |
| `tests/manifests/open_api/` | generatoriaus, konfigūracijos ir aprašo validumo testai |
| `tests/cli/test_udts_oas.py` | CLI testai |

Susiję ADR: [ADR-0001](../../adr/0001-oas-429-atviras-objektas.md) (`429`),
[ADR-0002](../../adr/0002-oas-sveikuju-skaiciu-ribos-nenurodomos.md)
(sveikųjų skaičių ribos), [ADR-0003](../../adr/0003-oas-versija-3-0.md)
(OpenAPI 3.0), [ADR-0004](../../adr/0004-oas-tuscias-identifikatorius-neaprasomas.md)
(tuščias identifikatorius).

## Įėjimai

### Režimai

`create_openapi_manifest` veikia dviem režimais:

| Režimas | Kaip iškviečiamas | `servers` | Agento endpoint'ai | Schemų vardai |
|---|---|---|---|---|
| **Duomenų paslauga** | `service_path=…` (`spinta udts oas`) | iš `--udts-cfg` | abi formos | kelias paslaugos viduje |
| **Katalogas** | be `service_path` (visas manifestas arba `main_dataset_name`) | nėra | tik agento adresai | pilnas vardas arba `basename` |

Toliau aprašomas duomenų paslaugos režimas; katalogo režimo skirtumai – lentelėje.

### Duomenų paslaugos kelias

Duomenų paslaugą apibrėžia rinkinio kelio pradžia:

```
datasets/{form}/{org}/{is}/{service}/{version}/{dataset}/{model}
└────────────── duomenų paslauga ─────────────┘
```

- `{version}` neprivaloma; jei yra, tai teigiamas sveikasis skaičius.
- `--path` atrenka rinkinius, kurių paslaugos kelias **lygus** nurodytam, todėl
  `.../at280/1` neapima `.../at280/10`, o neversijuotas `.../at280` neapima
  `.../at280/1`. Kitokios formos kelias priimamas su įspėjimu ir atrenka pagal
  prefiksą su segmento riba.
- Be `--path`: jei manifeste viena paslauga – imama ji, jei kelios – klaida su
  sąrašu. `--list` išvardija paslaugas ir jų rinkinius.

### Konfigūracija (`--udts-cfg`)

YAML failas, privalomas generuojant paslaugos aprašą.

| Raktas | Privaloma | Tikrinimas |
|---|---|---|
| `info.title` | taip | ne tuščia eilutė |
| `info.contact.name` | taip | ne tuščia eilutė |
| `info.contact.url` | taip | absoliutus URL |
| `info.contact.email` | taip | žr. žemiau |
| `servers` (≥ 1) | taip | `url` – `http`/`https` arba reliatyvus; be šablonų (`{…}`) |
| `auth.token_url` | kai pirmasis serveris reliatyvus | absoliutus `http`/`https` URL |
| `info.summary`, `info.description`, `info.version`, `info.termsOfService`, `info.license` (`name`, `url`) | ne | OpenAPI tipai |
| `externalDocs` (`url`, `description`) | ne | |
| `limits.max_limit` | ne | sveikasis skaičius 1…2⁶³−1, numatytoji 100000 |

- Nežinomas raktas praleidžiamas su įspėjimu; `x-` plėtiniai paliekami ten, kur
  juos leidžia OpenAPI (`info`, `info.contact`, `info.license`, `servers` įrašai,
  `externalDocs`).
- `http` adresas priimamas su įspėjimu (kredencialai eitų atviru tekstu).
- **El. paštas** tikrinamas įprastos formos: prieš `@` – RFC 5322 dot-atom
  žodžiai, sujungti po vieną tašką; po `@` – bent dviejų dalių domenas, dalys
  neprasideda ir nesibaigia `-`; ilgis iki 64 simbolių prieš `@` ir iki 254 viso.
  Adresai su kabutėmis ir IP domenu nepriimami.

### Kas patenka į aprašą: `visibility`

Į aprašą patenka tik metaduomenys, kurių `visibility` yra `protected`, `package`
arba `public`. `private` ir **tuščias** `visibility` (DSA numatytoji reikšmė
`private`) nepublikuojami:

| Elementas | Nepublikuojamas reiškia |
|---|---|
| modelis | nėra jo kelių, schemų, žymės |
| savybė | nėra schemoje, pavyzdžiuose, užklausų pavyzdžiuose; `file`/`object` savybė negauna savo kelio |
| enum reikšmė | nėra `enum` sąraše |
| kalbos savybė (`name@lt`) | savybė lieka, kol publikuojama bent viena kalba |
| nuoroda į nepublikuojamą modelį | rodo į bendrą `UnpublishedReference` schemą (atviras objektas, nieko neatskleidžia) |

Kiek modelių ir savybių liko už aprašo, pasakoma `UserWarning`; jei
nepublikuojamas nė vienas modelis – atskiru įspėjimu. `visibility` slepia tik
metaduomenis: duomenų prieigą valdo `access`.

## Dokumento struktūra

```
openapi: 3.0.3
info            ← --udts-cfg info (+ --api-version)
externalDocs    ← --udts-cfg externalDocs
servers         ← --udts-cfg servers, kiekvienas su paslaugos keliu
tags            ← utility + po vieną modeliui, surikiuota
paths           ← agento endpoint'ai + modelių keliai
components
  schemas       ← modeliai, sąrašai, nuorodos, bendros schemos
  parameters    ← antraštės, per modelį: id, _select, _sort; per dokumentą: _limit; _page
  headers, responses, securitySchemes
```

### `info`

Imama iš konfigūracijos. OpenAPI 3.0 neturi `info.summary`, todėl `summary`
tampa pirmąja `description` pastraipa. Katalogo režime `summary` ir `description`
imami iš rinkinio `title` ir `description`.

### `servers`

Kiekvienas konfigūracijos įrašas:

- `url` be kelio → prie jo prilipdomas paslaugos kelias
  (`https://get.data.gov.lt` → `https://get.data.gov.lt/datasets/gov/rc/jadis/at280/1`);
- `url` su paslaugos keliu → toks, koks yra;
- `url` su kitu keliu → toks, koks yra, su įspėjimu.

Be konfigūracijos (tik per Python API) – vienas reliatyvus `/{paslaugos kelias}`.

### Keliai

**Agento endpoint'ai** aprašomi dviem formomis, pažymėtomis kelio plėtiniu
`x-spinta-context`:

| Kelias | `x-spinta-context` | `servers` | Paskirtis |
|---|---|---|---|
| `/:version`, `/:health`, `/:token` | `gateway` | dokumento (paslaugos bazė) | taip juos maršrutizuoja vartai paslaugos viduje |
| `/version`, `/health`, `/auth/token` | `agent-direct` | savo: serverio adresas be kelio | taip juos aptarnauja pats agentas |

Katalogo režime aprašoma tik `agent-direct` forma.
Duomenų keliai `x-spinta-context` neturi – jie aptarnaujami abiem atvejais.

**Modelio keliai** (reliatyvūs paslaugos bazei, `{dataset}/{Model}`):

| Kelias | Operacijos | Kada |
|---|---|---|
| `/{dataset}/{Model}` | `get`, `head` | visada |
| `/{dataset}/{Model}/{id}` | `get`, `head` | visada |
| `/{dataset}/{Model}/{id}/{property}` | `get`, `head` | `file`, `image` savybei; atsako dvejetainiu turiniu, palaiko `Range` (`206`, `416`) |
| `/{dataset}/{Model}/{id}/{property}:ref` | `get`, `head` | `file`, `image` savybei; atsako failo metaduomenimis |
| `/{dataset}/{Model}/{id}/{property}` | `get`, `head` | `object` savybei; atsako objektu su `_type`, `_revision` |

**Parametrai** nurodomi kiekvienai operacijai atskirai (kelio lygmeniu parametrų
nėra), nes vartai skaito tik operaciją.

### Užklausų parametrai

| Parametras | Komponentas | Schema |
|---|---|---|
| `{id}` (UUID) | `id` | `UUID_REQUEST_PATTERN`: kanoninė, be brūkšnelių, `{…}`, `urn:`/`uuid:` prefiksai; v4 |
| `{id}` (modelis deklaruoja `_id`) | `id_{Schema}` | pagal tipą: `string` – vienas segmentas iki 512; `base32` – `=` + Base32; `integer` – `int64` ribos; `enum` – reikšmės |
| `_select` | `select_{Schema}` | vardai, keliai, funkcijos, `*`; iki 1000 simbolių; pavyzdys iš modelio savybių |
| `_sort` | `sort_{Schema}` | vardai su `+`/`-`; iki 1000 simbolių |
| `_limit` | `limit` | `integer`, `minimum: 1`, `maximum: limits.max_limit`, `int32` jei telpa, kitaip `int64` |
| `_page` | `page` | URL saugus Base64 su `=` užpildu, iki 8192 simbolių |
| `traceparent` | | W3C Trace Context; ne `ff` versija, ne nuliniai identifikatoriai (`not`) |
| `tracestate`, `Cache-Control`, `If-None-Match`, `Accept-Language`, `Range` | | spausdinami ASCII, `maxLength: 1024` |

`{id}` `=` prefiksas naudojamas, kai `is_accessible_by_equals_sign` – `base32`
arba nesudėtinio rakto `string` identifikatorius.

Filtras pagal savybes, `count()` / `_count` ir iškvietimo formos (`limit(…)`,
`select(…)`, `sort(…)`, `page(…)`) fiksuoto pavadinimo neturi, todėl aprašyti
sąrašo operacijos `description`.

`_page=<token>` Spintoje palaikomas per `spinta/spyna.py` gramatiką (terminalas
`_page=`), lygiai kaip `_limit=`. Tai laikinas sprendimas iki
[#2023](https://github.com/atviriduomenys/spinta/issues/2023). Netinkama žymė
(ne Base64, ne JSON sąrašas, ne eilutė) atmetama su `InvalidPageKey`.

### Schemos

**Vardai** (`SchemaNamer`): paslaugos režime – modelio kelias paslaugos viduje su
`_` vietoj `/` (`at280_israsas_DalyvioAsmensIsrasas`); OpenAPI komponento vardui
netinkami simboliai keičiami `_`; sutapimai gauna skaitinę galūnę. Iš modelio
vardo `X` išvedami:

| Schema | Kas |
|---|---|
| `X` | modelio objektas |
| `XCollection` | sąrašas: `_data` (privalomas), `_page` |
| `X_Ref` (+ `_2`, …) | nuoroda į `X`, po vieną kiekvienai nuorodos formai (lygis, `refprops`) |
| `X_{prop}` | `object` savybės atsakymas |
| `X_{prop}_ref` | `file` savybės `:ref` atsakymas |

Tomis pačiomis schemų vardais pavadinamos žymės (`tags`) ir sudaromi `operationId`.

**Modelio schema:**

- `_type` – `enum` su pilnu modelio vardu; `_id` – UUID v4 (kanoninė forma) arba
  deklaruoto `_id` tipas; `_revision` – UUID arba modelio sudaromos reikšmės
  tipas.
- `required` nėra: atsakymas turi tai, ko paprašyta (`_select`).
- Neprivaloma (`required` manifeste nepažymėta) savybė yra `nullable`; `enum`
  papildomas `null`; nuoroda – `anyOf: [{$ref}, NULL_OBJECT_SCHEMA]`.
- `integer` savybėms `format` ir ribų nėra (ADR-0002).
- `uuid` savybė – mažosiomis raidėmis v4 (`UUID.load`); `base32` – RFC 4648
  abėcėlė be užpildo.

**Nuorodos schema** (`X_Ref`):

- lygis ≥ 4 → `{_id}`, `_id` privalomas;
- lygis < 4 → `refprops` savybės be `_id`;
- nuoroda nuorodoje turi savo lygį; masyvas – elemento lygį;
- masyvas per tarpinę lentelę aprašomas kaip nuorodų sąrašas į galutinį modelį.

**Pavyzdžiai:** schema pavyzdį turi kaip vieną `example` (OpenAPI 3.0), ir jis
atitinka tą schemą. Identifikatoriai deterministiškai išvedami iš modelio vardo
(`_example_uuid`), todėl pergeneruotas failas skiriasi tik ten, kur pasikeitė
manifestas; nuorodos pavyzdys yra nurodomo modelio pavyzdžio identifikatorius.

**Bendros schemos** (`COMMON_SCHEMAS`) įtraukiamos tik tada, kai į jas rodoma:
klaidų objektai, `page`, `file`, `image`, `health`, `RateLimited`,
`UnpublishedReference` ir kt.

### Atsakymai

| Kodas | Kada | Kūnas |
|---|---|---|
| `200` | visos operacijos | modelis, sąrašas, savybė, `version`, `health`, token |
| `206`, `416` | `file`/`image` turinys | dvejetainis |
| `301` | vienas objektas | nėra; antraštė `Location` |
| `304` | `get`, `head` | nėra; tik talpyklos antraštės |
| `400`, `401`, `403`, `404`, `500`, `503` | pagal operaciją | `{"errors": [...]}` |
| `429` | visos operacijos | `RateLimited` – atviras objektas (ADR-0001) |

**Klaidos:** klaidos objektas turi penkis laukus (`type`, `code`, `template`,
`context`, `message`) ir tik juos. Kiekvienam kodui išvardijamos pavadintos
klaidos, sudarytos iš `spinta.exceptions` klasių (`code` ir `template` – `enum`
su klasės reikšme); paskutinė alternatyva – bendras `Error`, nes Spinta turi
daugiau klaidų, nei galima išvardyti, o `authlib` klaidos turi tik `code` ir
`message`. Token endpoint'o `400`/`401` – OAuth 2.0 klaida (RFC 6749 5.2).

### Saugumas

| Schema | Tipas | Kur |
|---|---|---|
| `UAPI_auth` | `oauth2` `clientCredentials` | duomenų operacijos |
| `UAPI_client` | `http` `basic` | token endpoint'ai |

- `tokenUrl` – `auth.token_url` arba pirmasis serveris + `/:token`.
- `scopes` – tik tie, kurių prašo operacijos.
- Operacijos scope'ai sudaromi tuo pačiu `scope_formatter`, kuriuo autorizuoja
  Spinta, iš modelio (savybės) ir veiksmo; kiekviena vardų erdvė virš modelio
  pateikiama kaip alternatyva.
- Sąrašas: `:getall` arba `:search` (alternatyvos); vienas objektas ir savybė:
  `:getone`. `HEAD` autorizuojamas kaip `GET`. `_page` be kitų parametrų
  autorizuojamas su `:getall`.
- Kiekviena autorizuojama operacija deklaruoja `401` ir `403`.

## Reguliariosios išraiškos

Visi `pattern` rašomi taip, kad juos skaitytų ir ECMA 262, ir RE2 (vartų
linteris): **be lookaround** ir **be kartojimų virš 1000**. Ribos, kurių šablonas
negali pasakyti, nurodomos `maxLength` arba `not` šalia jo.

## Ribos

- **Užklausos pusėje** viskas ribota, kad vartai atmestų netinkamą užklausą:
  `_limit`, `_select`, `_sort`, `_page`, `{id}`, `scope`, antraštės.
- **Atsakymų pusėje** ribojama tik tai, ką Spinta pati sudaro (UUID, `base32`,
  puslapio žymė); duomenų reikšmėms ribos nespėjamos.

## Galutinis sutvarkymas

- Nenaudojami bendri komponentai (parametrai, schemos, antraštės, atsakymai)
  pašalinami, kartojant visas rūšis, kol nebėra ką šalinti.
- Modelių schemos paliekamos visada.

## Išvestis

`write_openapi_manifest`: `.yml`/`.yaml` → YAML (be inkarų ir nuorodų), kitaip
JSON (`indent=2`, UTF-8); be `--output` – į stdout.

## Validumas

Testai tikrina, kad kiekvienas sugeneruotas aprašas:

- praeina `openapi_spec_validator` OpenAPI 3.0 validaciją;
- neturi RE2 neskaitomų šablonų;
- kiekvienas `example` atitinka savo schemą (`OAS30Validator`);
- neturi nenaudojamų bendrų komponentų;
- kiekviena operacija aprašo visus savo kelio parametrus;
- pavadintos klaidos atitinka `spinta.exceptions` klases.
