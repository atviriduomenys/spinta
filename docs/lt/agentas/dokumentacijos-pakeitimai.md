# Dokumentacijos pakeitimai

Šiame puslapyje surašyti **šios dokumentacijos** pakeitimai: kas pataisyta, papildyta ar
patikslinta ir ar dėl to reikia ką nors padaryti jau įdiegtame Agente.

Spinta programinės įrangos pakeitimai, keičiantys diegimą ar konfigūravimą, surašyti
puslapyje [Pakeitimai pagal versijas](versiju-pakeitimai.md).

Naujausi įrašai – viršuje.

Stulpelis „Ką daryti“:

- **Nieko** – patikslintas aprašymas, įdiegto Agento keisti nereikia.
- **Patikrinti** – peržiūrėkite savo konfigūraciją ar failus pagal nurodytą puslapį.
- **Pataisyti** – jei darėte pagal ankstesnę dokumentaciją, rezultatas netinkamas, jį reikia pataisyti.

## Pakeitimai

### 2026-10-29

| Puslapis | Kas pasikeitė | Ką daryti |
|----------|---------------|-----------|
| [Apie SPINTA agentą](index.md) | Naujas poskyris „Duomenų paslaugos adresas (UDTS URL)“: kelio segmentai pagal UDTS specifikaciją, paslaugos pavadinimas `{IS}_udts`, versijos pirmai paslaugos versijai nerašyti, dažniausios klaidos. Paskelbtos vartuose paslaugos kelio keisti negalima. | **Pataisyti** iki diegimo į vartus – jei kelyje nėra paslaugos segmento arba `_udts` prijungtas prie IS segmento. |
| [Apie SPINTA agentą](index.md) | Pabrėžta, kad UDTS paslaugomis teikiamiems duomenims `access` negali būti `open`. | **Patikrinti** manifestą. |
| [OAS generavimas](oas-generavimas.md) | Nauja įžanga: kam reikalingas OAS, kuo skiriasi nuo Katalogo eksporto, kaip OAS virsta vartų API (context-path, backend, `/version`, `/health`, `/token`), patikrinimas validatoriumi, pergeneravimas pakeitus DSA. | Nieko |
| [OAS generavimas](oas-generavimas.md) | OAS generuojamas `spinta udts oas`, kuri yra nuo 1.2.0; generuoti ne senesne nei 1.3.1 versija (su ja OAS importuotas į vartus), rekomenduojama naujausia. | **Pataisyti** – sugeneruoti OAS iš naujo, jei generavote senesne nei 1.3.1 versija. |
| [Registracija ir diegimas į vartus](registracija.md), [Apie SPINTA agentą](index.md) | Patikslinta OAS diegimo eiga: OAS generuoja institucija (`spinta udts oas`), pateikia VSSA patikrai ir pati diegia paslaugą į vartus per VSSA suteiktą prieigą. | Nieko |
| [OAS generavimas](oas-generavimas.md) | Patikslinta, kad `--path` nurodomas iki paslaugos (ar versijos) segmento ir kad kiekvienai paslaugai naudojamas atskiras konfigūracijos failas. Pridėtas gaunamo `servers.url` pavyzdys. | **Patikrinti** – ar kiekvienos paslaugos `info.title` skiriasi. |
| [OAS generavimas](oas-generavimas.md) | `info.description` pažymėtas privalomu (be jo vartai OAS nepriima), `info.version` – neprivalomu. | **Patikrinti** – ar nurodytas savo paslaugos aprašas. |
| [Duomenų gavimo testavimas](duomenų-gavimo-testavimas.md) | Naujas skyrius „Pilnas patikrinimas“: užklausa be žetono, su žetonu ir į duomenų rinkinį, per HTTPS. Pavyzdžiuose kelias pakeistas į `demo/demo_udts/rctest`. | Nieko |
| [Diegimas su Docker](diegimas/docker.md) | Numatytasis paleidimas – be `command:`, atvaizde esančiu skriptu (`SPINTA_AUTO_UPGRADE`); savas paleidimo skriptas aprašytas kaip alternatyva su dažniausiomis klaidomis. | **Patikrinti** – jei naudojate savą `startup.sh`. |
| [Manifest parengimas](šaltinių-konfigūravimas.md) | Naujas skyrius apie HTTP JSON / XML šaltinius ir jų autentifikaciją (pastovios antraštės nuo 1.2.0). Demo manifeste kelias pakeistas į `demo/demo_udts/rctest`. | Nieko |

### 2026-10-21

| Puslapis | Kas pasikeitė | Ką daryti |
|----------|---------------|-----------|
| [OAS generavimas](oas-generavimas.md) | Lentelė, kokį vartų adresą nurodyti `servers.url` (vidiniai TEST/PROD, išoriniai). Pavyzdžiuose `get.data.gov.lt` pakeistas į `apigw.gov.lt`; `auth.token_url` nenurodomas. | **Pataisyti** – jei `servers.url` nurodėte `get.data.gov.lt`. |

### 2026-10-19

| Puslapis | Kas pasikeitė | Ką daryti |
|----------|---------------|-----------|
| [Pakeitimai pagal versijas](versiju-pakeitimai.md) | Naujas puslapis: instrukcijas ir konfigūraciją liečiantys Spinta pakeitimai nuo 0.2dev22. | Nieko |
| [Autentifikacija](autentifikacija.md) | Introspekcijos klientas: kada reikalingas ir kaip išsaugoti jo pavadinimą ir `secret`. | **Patikrinti** – jei publikuojate paslaugas į vartus. |

### 2026-10-16

| Puslapis | Kas pasikeitė | Ką daryti |
|----------|---------------|-----------|
| [OAS generavimas](oas-generavimas.md) | Greitoje instrukcijoje pridėtas `SPINTA_CONFIG` žingsnis. | Nieko |

### 2026-10-15

| Puslapis | Kas pasikeitė | Ką daryti |
|----------|---------------|-----------|
| [OAS generavimas](oas-generavimas.md) | Naujas puslapis – OAS generavimo gidas. | Nieko |
| [Agento atnaujinimas](atnaujinimas.md), [Diegimas OS](diegimas/os.md) | Diegiama ir atnaujinama iš stabilių 1.x versijų (`spinta-latest.txt`, ne `spinta-latest-pre.txt`). | **Pataisyti** – jei atnaujinote pagal `spinta-latest-pre.txt` (gavote 0.2dev29). |

### 2026-09-14

| Puslapis | Kas pasikeitė | Ką daryti |
|----------|---------------|-----------|
| [Autentifikacija](autentifikacija.md) | Aprašytas `contract_scopes` blokas kliento faile ir `check_contract_scopes`. | Nieko |
