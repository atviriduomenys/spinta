# ADR-0003: Duomenų paslaugos aprašas rašomas OpenAPI 3.0

- **Būsena:** Priimtas
- **Data:** 2026-09-15
- **Susiję:** [#2004](https://github.com/atviriduomenys/spinta/issues/2004),
  [PR #2012](https://github.com/atviriduomenys/spinta/pull/2012)

## Kontekstas

`spinta udts oas` ir `create_openapi_manifest` generuoja duomenų paslaugos
OpenAPI aprašą. Generatorius rašė OpenAPI `3.1.0`.

- OpenAPI `3.0` versija nurodyta teisės aktuose ir kituose reikalavimuose
  duomenų paslaugoms, todėl tai ne vien techninis pasirinkimas.
- Kolegos pranešė, kad sugeneruoti aprašai neįsikelia į API vartus (Gravitee).
  Gravitee dokumentacija nurodo, kad importas palaiko Swagger 2.0 ir OpenAPI 3.0.

OpenAPI 3.1 ir 3.0 schemos skiriasi iš esmės: 3.1 schemos objektas yra JSON
Schema 2020-12, o 3.0 – jos pritaikytas poaibis. Todėl pakeisti vien versijos
numerį nepakanka.

## Sprendimas

Aprašas rašomas **OpenAPI `3.0.3`**, ir jame nenaudojama nieko, ko 3.0 neturi:

| 3.1 | 3.0 |
|---|---|
| `"type": ["string", "null"]` | `"type": "string", "nullable": true` |
| nuoroda, kuri gali būti `null` | `anyOf: [{$ref}, {"type": "object", "nullable": true, "enum": [null]}]` |
| `example` greta `$ref` | `allOf: [{$ref}]` su `example` šalia (3.0 ignoruoja `$ref` brolius) |
| schemos `examples` sąrašas | vienas `example` |
| `const` | `enum` su viena reikšme |
| `info.summary` | pirmoji `info.description` pastraipa |
| `info.license.identifier` | nerašomas; konfigūracijoje praleidžiamas su įspėjimu |

Be to, visi `pattern` rašomi taip, kad juos skaitytų ir RE2 (vartų linteris 3.0
dokumente jį taiko): be lookaround ir be kartojimų virš 1000; ribos, kurių
šablonas negali pasakyti, nurodomos `maxLength` arba `not`.

## Svarstytos alternatyvos

1. **Palikti 3.1.** Neatitinka reikalavimų ir neįsikelia į vartus. Atmesta.
2. **Generuoti abi versijas pagal parametrą.** Dvigubai daugiau kodo ir testų,
   o 3.1 aprašo niekas nenaudotų, kol reikalavimai nurodo 3.0. Atmesta.
3. **Generuoti 3.1 ir konvertuoti išoriniu įrankiu.** Konvertavimas nėra
   vienareikšmis (`nullable` nuorodos, `examples`), o jo rezultato nevaliduotų
   Spintos testai. Atmesta.

## Pasekmės

- Aprašas įsikelia į vartus ir praeina `openapi_spec_validator` OpenAPI 3.0
  validaciją; testai tikrina ir tai, kad kiekvienas `example` atitinka savo
  schemą pagal OpenAPI 3.0 taisykles (`OAS30Validator`).
- Testuose schemos tikrinamos OpenAPI 3.0 validatoriumi, ne JSON Schema, nes
  JSON Schema validatorius `nullable` nesupranta.
- Swagger UI nebeįspėja apie pasenusį schemos `example` – 3.0 tai įprastas
  laukas.
- 3.1 konstrukcijų į generatorių negrąžinti. Jei reikalavimai kada nors
  nurodys 3.1, rašomas naujas ADR.
- Įgyvendinta commite `c3dea4e9`.
