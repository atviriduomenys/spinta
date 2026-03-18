# Sprendimai prieš diegimą

Prieš pradedant techninius diegimo darbus reikia priimti penkis infrastruktūros sprendimus. Žemiau pavaizduotas sprendimų medis — kiekvienas pasirinkimas lemia tolesnę diegimo eigą ir reikalingų darbų apimtį.

```{mermaid}
flowchart TD
    Q1["1️⃣ Kiek agentų vienai IS?"]
    Q1 -->|"Rekomenduojama"| A1["Dedikuotas agentas<br/>kiekvienai IS"]
    Q1 -->|"Galima"| A2["Apjungtas<br/>(kelios IS — vienas agentas)"]
    Q1 -->|"Galima"| A3["Mišrus<br/>(dalis dedikuota,<br/>dalis apjungta)"]

    A1 & A2 & A3 --> Q2

    Q2["2️⃣ Kaip diegti aplinkas?"]
    Q2 -->|"Rekomenduojama"| B1["Atskira VM<br/>kiekvienai aplinkai<br/>(TEST + PROD)"]
    Q2 -->|"Greitesnis būdas"| B2["Viena VM<br/>PROD + TEST per Docker"]

    B1 --> Q3A["3️⃣ Diegimo būdas?"]
    Q3A -->|"Rekomenduojama"| C1["OS diegimas<br/>(systemd servisas)"]
    Q3A -->|"Galima"| C2["Docker"]

    B2 --> C3["OS + Docker<br/>(PROD — OS,<br/>TEST — Docker)"]

    C1 & C2 & C3 --> Q4

    Q4["4️⃣ Vartai?"]
    Q4 -->|"KVTC klientai<br/>SVDPT sąraše"| D1["Vidiniai vartai<br/>Reikia KVTC<br/>tinklo paraiškos"]
    Q4 -->|"Visi kiti"| D2["Išoriniai vartai<br/>Papildomos tinklo<br/>konfig. nereikia"]

    D1 & D2 --> Q5

    Q5["5️⃣ Kas diegia?"]
    Q5 -->|"Savarankiškai"| E1["Sekti šią<br/>dokumentaciją"]
    Q5 -->|"Su pagalba"| E2["Kreiptis į VSSA paslaugų teikėją,<br/>nurodyti pasirinktus variantus"]

    style A1 fill:#2d6a2d,color:#fff
    style B1 fill:#2d6a2d,color:#fff
    style C1 fill:#2d6a2d,color:#fff
    style A2 fill:#5a5a00,color:#fff
    style A3 fill:#5a5a00,color:#fff
    style B2 fill:#5a5a00,color:#fff
    style C2 fill:#5a5a00,color:#fff
    style C3 fill:#5a5a00,color:#fff
    style D1 fill:#1a4a7a,color:#fff
    style D2 fill:#1a4a7a,color:#fff
    style E1 fill:#4a4a4a,color:#fff
    style E2 fill:#4a4a4a,color:#fff
```

Plačiau apie kiekvieną sprendimą — žemiau.

## 1. Kiek agentų vienai IS?

Vienas Spinta agentas gali aptarnauti vieną arba kelias informacines sistemas:

| Variantas | Aprašas | Tinka kai |
|-----------|---------|-----------|
| **Dedikuotas** (rekomenduojama) | Atskiras agentas kiekvienai IS | IS skiriasi struktūra, apkrova ar atsakomybe |
| **Apjungtas** | Kelios IS — vienas agentas | IS paprastos, mažos apkrovos, administruoja ta pati komanda |
| **Mišrus** | Dalis dedikuota, dalis apjungta | Didelės IS — atskirai, mažos — grupuojamos |

## 2. Kiek aplinkų reikia?

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

## 5. Kas diegia?

| Variantas | Ką reikia daryti |
|-----------|-----------------|
| **Savarankiškai** | Sekti šią dokumentaciją žingsnis po žingsnio |
| **Su VSSA paslaugų teikėjo pagalba** | Kreiptis į VSSA paslaugų teikėją, aiškiai nurodyti pasirinktus variantus (agentų skaičių, VM ar Docker, vartų tipą) — tai lemia jų darbų apimtį |
