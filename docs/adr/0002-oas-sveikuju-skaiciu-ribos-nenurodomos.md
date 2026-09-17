# ADR-0002: OAS sveikųjų skaičių savybėms `format` ir ribos nenurodomi

- **Būsena:** Priimtas
- **Data:** 2026-09-17
- **Susiję:** [#2004](https://github.com/atviriduomenys/spinta/issues/2004),
  [PR #2012](https://github.com/atviriduomenys/spinta/pull/2012)

## Kontekstas

Vartų komanda sugeneruotus duomenų paslaugų OpenAPI aprašus tikrina `vacuum` su
vartų taisyklių rinkiniu. Jame yra dvi OWASP taisyklės, kurios `integer` tipo
schemai reikalauja:

- `owasp-integer-format` – `format: int32` arba `format: int64`;
- `owasp-integer-limit` – `minimum` ir `maximum` (arba `exclusiveMinimum` ir
  `exclusiveMaximum`).

Realiuose aprašuose (KEIS, LIBIS, VEPIS, NKDVIS) šios dvi taisyklės yra vienintelės
likusios `vacuum` klaidos – nuo 2 iki 30 kiekvienos viename faile. Visos jos yra
modelių savybių, kurių DSA tipas `integer`, atsakymų schemose, pavyzdžiui:

```json
"duration": {"type": "integer", "nullable": true}
```

Spinta šių reikšmių ribų nežino. DSA `integer` tipas ribų neturi, o UDTS agentas
duomenis skaito iš institucijos šaltinio, kurio stulpelio plotis (16, 32, 64 bitai
ar neribotas skaičius) manifeste neaprašytas. Tikros galimos reikšmės yra šaltinio
duomenų savybė, o ne Spintos.

Užklausų pusėje ribos jau nurodytos, nes ten jos yra vartų politika – viskas, ką
atsiunčia klientas, turi būti ribota, kad vartai netinkamą užklausą atmestų dar
nepasiekusią paslaugos: `_limit` turi `minimum: 1`, `maximum` iš
`limits.max_limit` ir `format`, o sveikojo skaičiaus `{id}` kelio parametras –
`int64` ribas.

## Sprendimas

Modelių savybių `integer` schemoms `format`, `minimum` ir `maximum` **nenurodomi**,
o `owasp-integer-format` ir `owasp-integer-limit` pastabos šioms schemoms
**netaisomos**.

Aprašas sako tik tai, ką Spinta tikrai žino: kad reikšmė yra sveikasis skaičius
(ir, jei savybė neprivaloma, kad ji gali būti `null`).

## Svarstytos alternatyvos

1. **Visoms savybėms rašyti `format: int64` ir 64 bitų ribas.** Pastabos dingtų,
   bet aprašas teigtų tai, ko Spinta nežino: šaltinio stulpelis gali būti ir
   siauresnis, ir platesnis (pvz., `NUMERIC` be tikslumo). Tai būtų taisyklės
   apėjimas, o ne problemos sprendimas – ribos neapsaugotų nuo nieko, nes jų
   niekas nenustatė. Platesnio už 64 bitus stulpelio reikšmes vartų atsakymo
   validacija dar ir atmestų. Atmesta.
2. **Nurodyti `format: int32` ir jo ribas.** Tas pats, tik dar rizikingiau:
   įprasti 64 bitų identifikatoriai ir skaitikliai būtų atmesti. Atmesta.
3. **Leisti nurodyti ribas DSA.** Tikslus sprendimas, bet DSA specifikacijoje
   tam nėra stulpelio. Tai DSA specifikacijos klausimas, o ne OAS generatoriaus.
   Šio sprendimo apimtyje neatlikta.

## Pasekmės

- Kol DSA neturi būdo nurodyti reikšmių ribas, `vacuum` su vartų taisyklių
  rinkiniu kiekvienam aprašui, turinčiam `integer` savybių, rodys
  `owasp-integer-format` ir `owasp-integer-limit` klaidas. Vartų komanda jas
  vertina kaip žinomas ir priimtas (pvz., įtraukia į ignoruojamų pastabų sąrašą
  arba šias taisykles atsakymų schemoms išjungia).
- Užklausų pusėje ribos lieka nurodytos (`_limit`, sveikojo skaičiaus `{id}`),
  nes ten jos yra vartų politika. `{id}` `int64` riba yra prielaida, kad raktas
  telpa į 64 bitus; jei šaltinio raktas platesnis, tokio objekto per vartus
  pasiekti nepavyktų. Šios OWASP taisyklės skirtos būtent užklausoms riboti.
- Jei DSA atsirastų reikšmių ribų aprašymas, šis ADR keičiamas nauju, o
  generatorius ribas ima iš manifesto.
