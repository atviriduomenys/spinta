# Tinklo konfigūravimas ir registracija

## Tinklo reikalavimai

Institucija, kuri diegiasi Spinta, turi pasirūpinti tinklo konfigūracija:

- **Vartai → Spinta**: vieta, kur sudiegta Spinta, turi būti pasiekiama iš Gravitee vartų
- **Spinta → Katalogas**: Spinta turi pasiekti duomenų katalogą
- **Spinta → Šaltiniai**: Spinta turi pasiekti duomenų šaltinius (API, DB, failus)

:::{important}
Lokalus testavimas turi būti **sėkmingai užbaigtas** prieš pradedant registraciją
ir diegimą į vartus. Gravitee veikia kaip proxy, todėl lokalus testavimas yra
pakankamas funkcionalumo patikrinimui.
:::

## Registracija kataloge

Kai lokalus testavimas sėkmingas, agentas registruojamas duomenų kataloge.

<!-- TODO: papildyti registracijos kataloge instrukcijomis -->

Dėl registracijos kataloge instrukcijų kreipkitės į VSSA.

## OAS diegimas į vartus

Spinta Agento paslauga pasiekiama per Gravitee vartus. Institucija turi nuspręsti,
per kokius vartus bus teikiami duomenys.

### Vartų tipai

| Vartų tipas | Paskirtis | Prieiga |
|-------------|-----------|---------|
| **Vidiniai vartai** | Duomenys tik tarp valdžios institucijų | Tik KVTC tinkle |
| **Išoriniai vartai** | Duomenys viešai arba autorizuotiems išoriniams gavėjams | Internetas |

:::{note}
Sprendimą dėl vartų tipo priima institucija kartu su VSSA, atsižvelgiant į
duomenų jautrumo lygį ir gavėjų ratą.
:::

<!-- TODO: papildyti OAS diegimo į vartus instrukcijomis -->

Dėl OAS diegimo į vartus instrukcijų kreipkitės į VSSA.

## Tinklo konfigūracija (KVTC/SVDPT)

Jei institucija teikia duomenis per **vidinius vartus**, reikalinga papildoma
tinklo konfigūracija — KVTC institucija turi sukonfigūruoti SVDPT sujungimą.

Ši konfigūracija yra specifinė kiekvienai institucijai ir apima konfidencialią
informaciją, todėl čia neaprašoma išsamiai.

:::{note}
Dėl KVTC/SVDPT tinklo konfigūracijos kreipkitės į atsakingą VSSA atstovą.
:::