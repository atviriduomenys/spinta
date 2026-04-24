# Aplinkos paruošimas

:::{note}
Informacija apie reikalingų aplinkų skaičių (TEST ir PROD) bei diegimo būdo pasirinkimą pateikta skyriuje [Sprendimai prieš diegimą](sprendimai.md).
:::

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

## Papildomos duomenų bazių tvarkyklės

Priklausomai nuo to, prie kokių duomenų šaltinių bus jungiamas Spinta agentas,
gali reikėti įdiegti papildomus Python paketus ir sisteminius komponentus.

| Duomenų bazė | Python paketas | Papildomi sisteminiai reikalavimai |
|--------------|----------------|-----------------------------------|
| PostgreSQL | `psycopg2-binary` | — |
| MySQL (≥ 5.6) | `pymysql` | — |
| MySQL (< 5.6) | `mysqlclient` | `build-essential python3-dev default-libmysqlclient-dev` |
| Microsoft SQL Server | `pymssql` | `freetds-bin` |
| Oracle | `cx_Oracle` | Oracle Instant Client (žr. žemiau) |
| SAS Datasets | `JayDeBeApi` | Java 11, SAS JDBC tvarkyklė |

:::{note}
Dalis paketų (pvz., `psycopg2-binary`) jau įtraukta į standartinį Spinta
requirements failą ir bus sudiegta automatiškai. Prieš diegiant papildomai,
patikrinkite, ar paketas nėra jau sudiegtas:

```bash
env/bin/pip show <paketo-pavadinimas>
```
:::

### PostgreSQL

```bash
sudo -Hsu spinta && cd
env/bin/pip install psycopg2-binary
```

### MySQL

MySQL versijai ≥ 5.6:

```bash
sudo -Hsu spinta && cd
env/bin/pip install pymysql
```

Senesnei MySQL versijai (< 5.6) naudojamas `mysqlclient`. Pirmiausiai įdiekite
sisteminius paketus, tada Python paketą:

```bash
sudo apt install build-essential python3-dev default-libmysqlclient-dev
sudo -Hsu spinta && cd
env/bin/pip install mysqlclient
```

### Microsoft SQL Server

MS SQL Server tvarkyklei reikalinga [FreeTDS](http://www.freetds.org/) biblioteka:

```bash
sudo apt install freetds-bin
sudo -Hsu spinta && cd
env/bin/pip install pymssql
```

### Oracle

Oracle duomenų bazei, be `cx_Oracle` Python paketo, privaloma įdiegti
**Oracle Instant Client** — tai natyviai sukompiliuota biblioteka, kurią turi
rasti sistema. Be šios bibliotekos `cx_Oracle` neveiks (klaida `DPI-1047`).

```bash
sudo -Hsu spinta && cd
env/bin/pip install cx_Oracle
```

Išsamią Oracle Instant Client diegimo instrukciją rasite čia:
[Oracle driver installation guide](https://github.com/atviriduomenys/spinta/issues/1881)

Žinomų Oracle problemų sąrašas:
[Oracle žinomų problemų sąrašas](https://github.com/atviriduomenys/spinta/issues/1768)

### SAS Datasets

SAS Datasets tvarkyklei reikalingi trys komponentai:

1. **Java 11** — įdiekite sistemoje:

   ```bash
   sudo apt install openjdk-11-jdk
   ```

2. **SAS JDBC tvarkyklė** — `sas.core.jar` ir `sas.intrnet.javatools.jar` failai,
   kuriuos gausite iš SAS licencijos turėtojo arba SAS palaikymo.

3. **JayDeBeApi** Python paketas:

   ```bash
   sudo -Hsu spinta && cd
   env/bin/pip install JayDeBeApi
   ```
