# Spintos programuotojų dokumentacija

Dokumentacija tiems, kas Spintą kuria ir prižiūri: kaip ji sutvarkyta ir kaip
veikia jos funkcionalumas. Naudotojams skirta dokumentacija yra `docs/en` ir
`docs/lt`, o pokyčių santrauka – `CHANGES.rst`.

## Kas kur rašoma

| Kur | Kam | Kas |
|---|---|---|
| `CHANGES.rst` | naudotojui | trumpai, kas pasikeitė ir ką tai reiškia naudojant Spintą |
| `docs/en`, `docs/lt` | naudotojui | kaip Spintą naudoti: komandos, konfigūracija, pavyzdžiai |
| `docs/dev/architektura/` | programuotojui | bendra Spintos sandara: komponentai, jų ryšiai, duomenų srautai |
| `docs/dev/specifikacijos/` | programuotojui | vieno funkcionalumo techninė specifikacija: ką jis daro, iš ko ir kaip |
| `docs/adr/` | programuotojui | kodėl priimtas sprendimas ir kokios buvo alternatyvos |

## Taisyklės

- **Specifikacija aprašo galutinę būseną, be istorijos.** Kaip buvo anksčiau ir
  kodėl pakeista, rašoma ne čia, o ADR arba commitų istorijoje. Pasikeitus
  elgsenai, specifikacija atnaujinama taip, kad vėl aprašytų tai, kas yra.
- **Vienas funkcionalumas – vienas failas** kataloge `specifikacijos/`, vardas
  pagal funkcionalumą (`udts-oas.md`).
- **Specifikacija nurodo kodą ir testus**, kuriuose funkcionalumas įgyvendintas,
  ir ADR, kurie jį lemia.
- **Nekartoti naudotojo dokumentacijos.** Jei kažkas jau aprašyta `docs/lt` ar
  `docs/en`, pateikiama nuoroda.

## Turinys

### Architektūra

Dar neaprašyta, žr. [`architektura/README.md`](architektura/README.md).

### Specifikacijos

| Specifikacija | Funkcionalumas |
|---|---|
| [UDTS duomenų paslaugos OpenAPI aprašas](specifikacijos/udts-oas.md) | `spinta udts oas`, `create_openapi_manifest` |
