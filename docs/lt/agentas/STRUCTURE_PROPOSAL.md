# Dokumentacijos struktūros pasiūlymas

> **Statusas:** Pasiūlymas aptarimui. Dar nepakeisti jokie failai.
> **Branch:** `docs-restructure-proposal`

## Problema su dabartine struktūra

Dabartinė dokumentacija yra **diegimo-centrinė** — aprašo ką daryti, bet neatsako į klausimą *kodėl* ir *ar man tai reikia*. Institucijos darbuotojas ateina ir nežino nuo ko pradėti.

Dabartinis turinys (`index.rst`):
```
agento-paruošimas
šaltinių-konfigūravimas
diegimas/ (docker, os)
web-serverio-konfigūravimas
duomenų-gavimo-testavimas
tinklo-konfigūravimas
papildoma-informacija
```

**Konkrečios problemos:**
- Nėra „landing page" kuri paaiškintų visą procesą
- Kliento kūrimas (OAuth) yra diegimo failo gale — nors tai atskira tema
- Autentifikacija (vidinis vs išorinis AM) niekur nepaaiškinta
- Testavimas ir debugging atskirti, nors tai viena tema
- Nėra aiškaus žingsnio „prieš diegiant į vartus — ištestuok lokaliai"

---

## Siūloma nauja struktūra

### Principas: sprendimų kelias

Dokumentacija turėtų vesti skaitytoją per **sprendimus**, kuriuos reikia priimti, o ne tik per techninius žingsnius.

```
1. Įvadas — kas yra agentas ir kam jis reikalingas
2. Prieš diegiant — sprendimai
3. Aplinkos paruošimas → Docker / OS
4. Konfigūracija — config.yml ir manifest
5. Autentifikacija ir autorizacija
6. Lokalus testavimas ir derinimas
7. Registracija kataloge
8. Diegimas į vartus (OAS)
9. Tinklo konfigūracija (KVTC/SVDPT)
10. Papildoma informacija ir atnaujinimai
```

---

## Detalus aprašas

### 1. Įvadas (naujas failas: `įvadas.md`)

**Dabartinė situacija:** Nėra. Skaitytojas iškart patenka į techninius reikalavimus.

**Siūlomas turinys:**
- Kas yra Spinta Agentas (viena pastraipa)
- Diagrama: Agentas kaip tarpininkas tarp šaltinio ir vartų (Gravitee)
- Kada reikia agento (SOAP/WSDL šaltinis, DB, failai)
- Kiek agentų reikia institucijai (1 per šaltinį? 1 visiems?)

---

### 2. Prieš diegiant — sprendimai (pakeisti `agento-paruošimas.md`)

**Siūlomas turinys:**
- Techniniai reikalavimai (lieka kaip yra)
- **Naujas skyrius:** Diegimo būdo pasirinkimas

| Situacija | Rekomenduojamas būdas |
|-----------|----------------------|
| Turite Linux serverį | Diegimas į OS |
| Turite tik Docker aplinką | Diegimas į Docker |
| Windows serveris | Kreiptis į VSSA |

- **Naujas skyrius:** Komponentų apžvalga — kodėl reikia nginx, Redis, ir kaip jie susiję

---

### 3. Aplinkos paruošimas (lieka `diegimas/` katalogas)

Beveik nesikeičia, tik:
- Kliento kūrimo skyrius **iškeliamas** iš čia į atskirą skyrių (žr. 5 punktą)
- `/version` patikrinimas po paleidimo (jau pridėta)

---

### 4. Konfigūracija (išskaidyti į atskirus failus)

Dabartinis `šaltinių-konfigūravimas.md` gali likti, bet pridėti:
- `config.yml` parametrų aprašas (jau yra, geras)
- Manifest failo ryšys su šaltiniu — ką reiškia kiekvienas stulpelis

---

### 5. Autentifikacija ir autorizacija (naujas failas: `autentifikacija.md`)

**Dabartinė situacija:** Kliento kūrimas yra diegimo failo gale, be konteksto.

**Siūlomas turinys:**

#### Kas yra OAuth2 klientas
- Integruotas AM (Spinta) — lokaliam testavimui
- Išorinis AM (Gravitee Access Manager) — produkcinėje aplinkoje

#### Kliento kūrimas (lokaliam testavimui)
*(Perkelti iš `os.md`)*
- `spinta client add -n vardas`
- Kliento pavadinimas vs UUID — kodėl naudojamas pavadinimas
- Secret išsaugojimas

#### Leidimai (scopes)
- Ką reiškia `uapi:/:getall` ir kiti
- Kaip susieti klientą su šaltiniu (`backends` sekcija)

---

### 6. Lokalus testavimas ir derinimas (sujungti `duomenų-gavimo-testavimas.md` + dalis `papildoma-informacija.md`)

**Principas:** Lokalus testavimas yra **privalomas žingsnis** prieš diegiant į vartus.

**Siūlomas turinys:**
- Kaip gauti OAuth token
- Pirminis patikrinimas: `/version` endpoint
- Duomenų gavimo testavimas (lieka kaip yra)
- Logai: kur jie, kaip žiūrėti (`journalctl`, `access.log`)
- Dažniausios klaidos ir sprendimai

---

### 7. Registracija kataloge (naujas failas arba išplėstas `tinklo-konfigūravimas.md`)

Šiuo metu `tinklo-konfigūravimas.md` yra labai trumpas (3 eilutės). Galima išplėsti:
- Kada registruoti (tik kai lokalus testavimas sėkmingas)
- Kaip registruoti kataloge
- OAS (Open API Spec) diegimas į vartus — vidiniai vs išoriniai

---

### 8. Tinklo konfigūracija — KVTC/SVDPT (išplėstas `tinklo-konfigūravimas.md`)

- Bendra informacija apie KVTC institucijų tinklo konfigūraciją
- Konfidenciali dalis: „Dėl SVDPT/KVTC sujungimo konfigūracijos kreipkitės į [kontaktą]"

---

### 9. Papildoma informacija (lieka `papildoma-informacija.md`)

- Spinta atnaujinimas (lieka)
- Manifest atnaujinimas (lieka)
- Kelių manifestų naudojimas (lieka)

---

## Siūlomas naujas `index.rst`

```rst
Agentas
====================

.. toctree::
   :maxdepth: 2
   :caption: Agentas

   įvadas
   agento-paruošimas
   diegimas/index
   šaltinių-konfigūravimas
   autentifikacija
   duomenų-gavimo-testavimas
   tinklo-konfigūravimas
   papildoma-informacija
```

---

## Klausimai aptarimui

1. **Kliento kūrimas** — ar tikrai jis priklauso autentifikacijos skyriuje, ar geriau palikti diegimo eigoje?
2. **Gravitee AM** — kiek detaliai aprašyti išorinį AM? Ar tai atskiroje dokumentacijoje?
3. **Registracija kataloge** — ar ši informacija jau yra kažkur kitur dokumentuota?
4. **KVTC/SVDPT** — kas yra tinkamas kontaktas, į kurį nukreipti?
