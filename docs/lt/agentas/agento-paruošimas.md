# Agento paruošimas

## Techniniai reikalavimai

Agentas veikia ir yra testuotas Linux operacinėse sistemose, konkrečiai naudojant Debian/Ubuntu distribucijas, todėl instrukcijos, kaip pavyzdys bus pateiktos būtent Debian/Ubuntu aplinkai. Diegimą galima atlikti ir kitose Linux distribucijose, tačiau tam tikros vietos, nurodytos šioje dokumentacijoje, turėtu būti priderintos taip, kad veiktų kitoje distribucijoje.

:::{note}
Pateikiama instrukcija yra kaip pavyzdys, kai naudojama Debian/Ubuntu OS
:::

Spinta yra sukurta naudojant Python programavimo kalbą ir veikia su Python versijomis 3.10-3.13. Naujose Agento versijose reikalavimas Python versijai gali keistis.

Dėl serverio resursų, tokių kaip CPU, RAM ir HDD, reikalingi resursai tiesiogiai priklauso nuo publikuojamų duomenų kiekio ir naudotojų srauto, kurie naudosis duomenų publikavimo paslauga.

**Rekomenduojami reikalavimai** (CCT standartinė VM konfigūracija):

- 2 vCPU
- 4 GB RAM
- 40 GB laisvos vietos diske
- Interneto prieiga (443 portas į `data.gov.lt`)

**Minimalūs reikalavimai** (testavimui / nedidelei apkrovai):

- 1 CPU
- 1 GB RAM
- 10 GB laisvos vietos diske

_Aplinka:_ pilnai įdiegta operacinė sistema su visomis būtinomis priklausomybėmis, be duomenų, iki 5 vienu metu besinaudojančių vartotojų.

:::{note}
Pats Agentas su visomis Python priklausomybėmis diske užima apie 2 GB vietos. Likusi vieta reikalinga OS, log'ams ir laikiems failams.
:::

Agento veikimas turėtu būti nuolat stebimas ir reikiami resursai didinami, pagal poreikį.

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
