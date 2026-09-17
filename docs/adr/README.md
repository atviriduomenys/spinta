# Architektūriniai sprendimai (ADR)

Šiame kataloge surašyti architektūriniai sprendimai (*Architecture Decision
Records*): kas nuspręsta, kodėl ir kokios to pasekmės. ADR rašomas tada, kai
sprendimas nėra akivaizdus iš kodo, buvo svarstytos kelios alternatyvos arba
kas nors (žmogus ar recenzentas) vėliau gali pasiūlyti jį pakeisti.

## Taisyklės

- Vienas sprendimas – vienas failas, `NNNN-trumpas-pavadinimas.md`.
- Numeruojama iš eilės nuo `0001`; numeris niekada nenaudojamas pakartotinai.
- Priimtas ADR nekeičiamas iš esmės. Jei sprendimas keičiasi, rašomas naujas
  ADR, o senojo būsena pakeičiama į „Pakeistas ADR-NNNN“.
- Būsenos: **Siūlomas**, **Priimtas**, **Atmestas**, **Pakeistas ADR-NNNN**.

Naujam ADR nukopijuokite struktūrą iš esamo: *Kontekstas*, *Sprendimas*,
*Svarstytos alternatyvos*, *Pasekmės*.

ADR paaiškina, **kodėl** taip nuspręsta. **Kaip** funkcionalumas veikia, aprašoma
programuotojų dokumentacijoje, [`docs/dev`](../dev/README.md).

## Sąrašas

| Nr. | Pavadinimas | Būsena | Data |
|---|---|---|---|
| [0001](0001-oas-429-atviras-objektas.md) | OAS `429` atsakymas – atviras objektas, limitus taiko vartai | Priimtas | 2026-09-17 |
| [0002](0002-oas-sveikuju-skaiciu-ribos-nenurodomos.md) | OAS sveikųjų skaičių savybėms `format` ir ribos nenurodomi | Priimtas | 2026-09-17 |
| [0003](0003-oas-versija-3-0.md) | Duomenų paslaugos aprašas rašomas OpenAPI 3.0 | Priimtas | 2026-09-15 |
| [0004](0004-oas-tuscias-identifikatorius-neaprasomas.md) | Tuščias deklaruotas identifikatorius OAS apraše neaprašomas | Priimtas | 2026-09-17 |
