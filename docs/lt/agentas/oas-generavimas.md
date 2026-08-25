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
- `--udts-cfg` – konfigūracijos failas (žr. žemiau). Neprivalomas, bet be jo
  `servers` bus tik reliatyvus paslaugos kelias, be aplinkos adreso.
- `--api-version` – `info.version` reikšmė.

## Konfigūracijos failas

Manifeste nėra aplinkų adresų, paslaugos lygmens aprašo ir autorizacijos
serverio – tai nurodoma atskirame YAML faile. Pavyzdinis failas yra Spinta
pakete, `spinta/manifests/open_api/udts_cfg.example.yml`.

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
kurios kitaip tyliai paliktų lauką neužpildytą. `x-` plėtiniai išsaugomi.

`auth.token_url` neprivalomas: nenurodžius, jis išvedamas iš pirmojo `servers`
įrašo ir `/:token`, t. y. iš to paties adreso, kuriuo token'ą per vartus pasiekia
gavėjas. Nurodyti verta tik tada, kai autorizacijos serveris yra kitur.

`servers` – po vieną įrašą kiekvienai aplinkai. Adresą galima nurodyti dviem
būdais:

- **tik adresas, be kelio** – tuomet prie jo prilipdomas `--path` paslaugos
  kelias. Taip vienas failas tinka visoms to paties agento paslaugoms;
- **pilnas adresas su paslaugos keliu** – naudojamas toks, koks yra. Jei jo
  kelias nesutampa su `--path`, parodomas įspėjimas.

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

Agento lygmens endpoint'ai (`/version`, `/auth/token`) guli agento šaknyje, o ne
po paslaugos keliu, todėl specifikacijoje jie aprašomi veiksmo forma – `/:version`
ir `/:token`. Vartuose jiems reikia atskirų Dynamic Routing taisyklių:

| Match expression | Redirect to |
|---|---|
| `/:version` | `{#api.properties['uapi_version']}` |
| `/:token` | `{#api.properties['uapi_token']}` |
| `/(.*)` | `{#api.properties['uapi_data_prefix']}{#group[0]}` |
