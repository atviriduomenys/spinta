# Agento paruošimas

## Techniniai reikalavimai

Agentas veikia ir yra testuotas Linux operacinėse sistemose, konkrečiai naudojant Debian/Ubuntu distribucijas, todėl instrukcijos, kaip pavyzdys bus pateiktos būtent Debian/Ubuntu aplinkai. Diegimą galima atlikti ir kitose Linux distribucijose, tačiau tam tikros vietos, nurodytos šioje dokumentacijoje, turėtu būti priderintos taip, kad veiktų kitoje distribucijoje.

:::{note}
Pateikiama instrukcija yra kaip pavyzdys, kai naudojama Debian/Ubuntu OS
:::

Spinta yra sukurta naudojant Python programavimo kalbą ir veikia su Python versijomis 3.10-3.14. Naujose Agento versijose reikalavimas Python versijai gali keistis.

Dėl serverio resursų, tokių kaip CPU, RAM ir HDD, reikalingi resursai tiesiogiai priklauso nuo publikuojamų duomenų kiekio ir naudotojų srauto, kurie naudosis duomenų publikavimo paslauga.

Minimalūs Agento reikalavimai:

- 1 CPU
- 1 GB RAM
- 5 GB laisvos vietos diske

_Aplinka:_ pilnai įdiegta operacinė sistema su visomis būtinomis priklausomybėmis, be duomenų, iki 5 vienu metu besinaudojančių vartotojų.

:::{note}
Taip pat rekomenduojama bent 10 GB HDD laisvos vietos, kuri lieka pilnai įdiegus operacinę sistemą ir visas reikalingas priklausomybes. Ši vieta gali būti reikalinga log’ų įrašams.
:::

Pats savaime Agentas su visomis Python priklausomybėmis diske užima apie 2 GB vietos, tačiau sunaudojamos vietos skaičius gali skirtis, skirtingose distribucijose.

Agento veikimas turėtu būti nuolat stebimas ir reikiami resursai didinami, pagal poreikį.

Disko vietos ir atminties likutį Agentas stebi ir pats — `/health` adresu jis pateikia savo būsenos suvestinę, kurią galima naudoti stebėsenos sistemoje ar konteinerio būsenos tikrinime. Pagal nutylėjimą būsena tampa nesveika, kai lieka mažiau nei 2 GB laisvos vietos diske arba mažiau nei 256 MB prieinamos atminties. Šios ribos parinktos taip, kad aukščiau nurodytus minimalius reikalavimus atitinkanti sistema būtų sveika, o įspėjimas ateitų dar likus laiko sureaguoti.

:::{note}
Nesveika būsena nurodoma atsakymo `healthy` lauke, o ne HTTP statuso kode — endpoint'as visada grąžina `200`. Stebėsenos sistemą reikia konfigūruoti tikrinti būtent šį lauką.
:::

Ribas galima keisti `health.min_free_disk_space` ir `health.min_free_memory` parametrais; jas verta didinti kartu su Agentui skiriamais resursais. Plačiau apie šiuos parametrus rašoma angliškos dokumentacijos skyriuje „Configuration → Health probe configuration“.

## Operacinės sistemos paruošimas

### Papildomų OS paketų diegimas

:::{note}
Docker yra reikalingas tiek diegiant Spintą virtualioje mašinoje, tiek ir naudojant patį dockerį.
:::

```bash
sudo apt update
sudo apt upgrade
sudo apt install curl docker.io docker-compose-v2
```

### Spinta vartotojo sukūrimas

:::{note}
Agentas turėtu būti diegiamas ir leidžiamas spinta naudotojo teisėmis (ar kito ne root naudotojo teisėmis), todėl reikia sukurti sisteminį naudotoją:
:::

```bash
sudo useradd --system -g www-data --create-home --home-dir /opt/spinta spinta
```

Suteikiame docker teises Spinta sisteminiam naudotojui:

```bash
sudo usermod -aG docker spinta
```

Atkreipkite dėmesį, kad visose komandose, kurios prasideda sudo, komanda turi būti vykdoma administratoriaus teisėmis, tačiau visur kur nėra sudo, komanda turi būti vykdoma spinta naudotojo teisėmis. Tai yra svarbu, todėl nesupainiokite kokio naudotojo teisėmis vykdote komandas, priešingu atveju susidursite su sunkumais susijusiais su failų teisėmis.
