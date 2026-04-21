# Sprendimai prieš diegimą

Prieš pradedant techninius diegimo darbus reikia priimti penkis infrastruktūros sprendimus. Žemiau pavaizduotas sprendimų medis — kiekvienas pasirinkimas lemia tolesnę diegimo eigą ir reikalingų darbų apimtį.

```{mermaid}
flowchart TD
    Q1["1️⃣ Kiek agentų vienai IS?"]
    Q1 -->|"Rekomenduojama"| A1["Dedikuotas agentas<br/>kiekvienai IS"]
    Q1 -->|"Galima"| A2["Apjungtas<br/>(kelios IS — vienas agentas)"]
    Q1 -->|"Galima"| A3["Mišrus<br/>(dalis dedikuota,<br/>dalis apjungta)"]

    A1 & A2 & A3 --> Q2

    Q2["2️⃣ Kaip diegti DSA testavimo agentą?"]
    Q2 -->|"Rekomenduojama"| B1["Atskira VM<br/>kiekvienam agentui"]
    Q2 -->|"Greitesnis būdas"| B2["Viena VM<br/>PROD + DSA testavimo<br/>per Docker"]

    B1 --> Q3A["3️⃣ Diegimo būdas?"]
    Q3A -->|"Rekomenduojama"| C1["OS diegimas<br/>(systemd servisas)"]
    Q3A -->|"Galima"| C2["Docker"]

    B2 --> C3["OS + Docker<br/>(PROD — OS,<br/>DSA testavimo — Docker)"]

    C1 & C2 & C3 --> Q4

    Q4["4️⃣ Vartai?"]
    Q4 -->|"KVTC klientai<br/>SVDPT sąraše"| D1["Vidiniai vartai<br/>Reikia KVTC<br/>tinklo paraiškos"]
    Q4 -->|"Visi kiti"| D2["Išoriniai vartai<br/>Papildomos tinklo<br/>konfig. nereikia"]

    D1 & D2 --> Q5

    Q5["5️⃣ Kas diegia?"]
    Q5 -->|"Savarankiškai"| E1["Sekti šią<br/>dokumentaciją"]
    Q5 -->|"Su pagalba"| E2["Kreiptis į<br/>VSSA paslaugų teikėją,<br/>nurodyti pasirinktus<br/>variantus"]

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

Kiekviena institucija turi bent **du agentus** (PROD agentas ir DSA testavimo agentas).
Jei kataloge skelbiami ir DEMO duomenys — atitinkamai daugiau. Žr. {ref}`agentų tipų lentelę <kiek-agentu-reikia>`.

Vienas agentas gali aptarnauti vieną arba kelias informacines sistemas:

| Variantas | Aprašas | Tinka kai |
|-----------|---------|-----------|
| **Dedikuotas** (rekomenduojama) | Atskiras agentas kiekvienai IS | IS skiriasi struktūra, apkrova ar atsakomybe |
| **Apjungtas** | Kelios IS — vienas agentas | IS paprastos, mažos apkrovos, administruoja ta pati komanda |
| **Mišrus** | Dalis dedikuota, dalis apjungta | Didelės IS — atskirai, mažos — grupuojamos |

:::{warning}
**Privaloma taisyklė** (taikoma kai vienas agentas aptarnauja kelias IS): vienas agentas
negali teikti skirtingo tipo duomenų. Jei kelios IS jungiamos per vieną agentą, visos
privalo naudoti to paties tipo duomenis — PROD ir DEMO duomenys privalo būti atskirti
skirtinguose agentuose.
:::

## 2. Kiek agentų reikia pagal tipą?

Minimalus agentų skaičius priklauso nuo to, kokie duomenys bus skelbiami kataloge.
Kiekviena institucija privalo turėti bent:

- **PROD agentą (duomenų teikimui)** — jungiamas prie `apigw.gov.lt`
- **DSA testavimo agentą** — jungiamas prie `test-apigw.gov.lt`, skirtas DSA
  pakeitimams tikrinti prieš perkeliant į PROD. Rekomenduojama naudoti tik vidinį
  Spinta AM — kitos institucijos neturi turėti prieigos.

Jei kataloge skelbiami ir DEMO duomenys — kiekvienam duomenų tipui reikia atskiro
PROD agento ir DSA testavimo agento.

**Kodėl DSA testavimo agentas reikalingas net be TEST IS aplinkos?** Jis skirtas ne
IS aplinkai testuoti, o DSA pakeitimams patikrinti: nauja DSA versija pirmiausia
įkeliama į DSA testavimo agentą, ten patikrinama, ir tik tada perkeliama į PROD agentą.

## 3. Kaip diegti DSA testavimo agentą?

DSA testavimo agentą galima įdiegti dviem būdais:

**Atskira VM (rekomenduojama)**

| Privalumai | Trūkumai |
|------------|----------|
| Pilna izoliacija — PROD ir DSA testavimo agentai nesusiję | Reikia papildomos VM |
| PROD stabilumas neveikiamas DSA testavimo problemų | Papildomi tinklo derinimo darbai |
| Aiški resursų atskirtis | Daugiau laiko diegimui |

**Docker konteineris PROD VM**

| Privalumai | Trūkumai |
|------------|----------|
| Nereikia naujos VM | PROD VM — vienas gedimo taškas abiem agentams |
| Greičiau įdiegiama | PROD ir DSA testavimo agentai dalinasi resursais (RAM, CPU) |
| Mažiau tinklo derinimo | Sudėtingesnė priežiūra |

## 4. Kokie vartai bus naudojami?

:::{important}
Tinklo konfigūravimas trunka ilgiausiai — **užsisakykite kuo anksčiau**, nelaukdami kol bus baigtas techninis diegimas.
:::

| Vartų tipas | Tinka kai | Tinklo darbai |
|-------------|-----------|---------------|
| **Vidiniai vartai** | Institucija yra KVTC klientė ir įtraukta į [SVDPT naudotojų sąrašą](https://www.e-tar.lt/portal/lt/legalAct/aea15050a53411e8acb39f2e6db7935b/asr) | Reikalingas papildomas tinklo sujungimas per SVDPT (KVTC forma → VSSA) |
| **Išoriniai vartai** | IS nėra SVDPT tinkle | Papildomos tinklo konfigūracijos nereikia |

Plačiau: {ref}`Tinklo konfigūravimas → Vartų tipai <vartu-tipai>`

## 5. Kas diegia?

| Variantas | Ką reikia daryti |
|-----------|-----------------|
| **Savarankiškai** | Sekti šią dokumentaciją žingsnis po žingsnio |
| **Su VSSA paslaugų teikėjo pagalba** | Kreiptis į VSSA paslaugų teikėją, aiškiai nurodyti pasirinktus variantus (agentų skaičių, VM ar Docker, vartų tipą) — tai lemia jų darbų apimtį |
