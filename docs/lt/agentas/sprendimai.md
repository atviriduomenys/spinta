# Sprendimai prieš diegimą

Prieš pradedant techninius diegimo darbus, reikia priimti tris sprendimus. Jie lemia, kokie infrastruktūros darbai reikalingi ir kurią diegimo instrukciją naudoti.

## 1. Kiek aplinkų reikia?

Visoms institucijoms reikalingos **dvi aplinkos** — TEST ir PROD — nepriklausomai nuo to, kiek IS aplinkų turi institucija:

| Aplinka | Vartai | Paskirtis |
|---------|--------|-----------|
| **TEST** | `test-apigw.gov.lt` | DSA pakeitimų tikrinimas prieš perkeliant į PROD |
| **PROD** | `apigw.gov.lt` | Duomenų teikimas galutiniams vartotojams |

**Kodėl TEST reikia net be TEST IS aplinkos?** TEST vartai skirti ne IS aplinkai testuoti, o DSA pakeitimams patikrinti: nauja DSA aprašo versija pirmiausia įkeliama į TEST, ten patikrinama, ar duomenys grąžinami teisingai, ir tik tada perkeliama į PROD.

## 2. Kaip diegti TEST agentą?

TEST agentą galima įdiegti dviem būdais:

**Atskira VM (rekomenduojama)**

| Privalumai | Trūkumai |
|------------|----------|
| Pilna izoliacija — TEST ir PROD nesusiję | Reikia papildomos VM |
| PROD stabilumas neveikiamas TEST problemų | Papildomi tinklo derinimo darbai |
| Aiški resursų atskirtis | Daugiau laiko diegimui |

**Docker konteineris PROD VM**

| Privalumai | Trūkumai |
|------------|----------|
| Nereikia naujos VM | PROD VM — vienas gedimo taškas abiem agentams |
| Greičiau įdiegiama | TEST ir PROD dalinasi resursais (RAM, CPU) |
| Mažiau tinklo derinimo | Sudėtingesnė priežiūra |

## 3. Kokie vartai bus naudojami?

:::{important}
Tinklo konfigūravimas trunka ilgiausiai — **užsisakykite kuo anksčiau**, nelaukdami kol bus baigtas techninis diegimas.
:::

| Vartų tipas | Tinka kai | Tinklo darbai |
|-------------|-----------|---------------|
| **Vidiniai vartai** | Institucija yra KVTC klientė ir įtraukta į [SVDPT naudotojų sąrašą](https://www.e-tar.lt/portal/lt/legalAct/aea15050a53411e8acb39f2e6db7935b/asr) | Reikalingas papildomas tinklo sujungimas per SVDPT (KVTC forma → VSSA) |
| **Išoriniai vartai** | IS nėra SVDPT tinkle | Papildomos tinklo konfigūracijos nereikia |

Plačiau: [Tinklo konfigūravimas → Vartų parinkimas](tinklo-konfigūravimas.md#vartu-parinkimas)
