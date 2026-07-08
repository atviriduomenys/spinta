# Spinta Agento atnaujinimas

Šiame puslapyje aprašyta, kaip atnaujinti jau įdiegtą Spinta Agentą iki naujausios
versijos. Atnaujinimo būdas priklauso nuo to, kaip Agentas įdiegtas —
[naudojant Docker](diegimas/docker.md) ar [tiesiai operacinėje sistemoje](diegimas/os.md).

## Naujienos ir versijos

Naujienos apie Spinta Agento atnaujinimus skelbiamos [data.gov.lt](https://data.gov.lt/) portale:

- [Programinės įrangos atnaujinimai](https://data.gov.lt/more/programines-irangos-atnaujinimai/)
  — bendros naujienos apie Spinta ir Katalogo atnaujinimus. Verta sekti abu.
- [Spintos atnaujinimai](https://data.gov.lt/more/programines-irangos-atnaujinimai/spintos-atnaujinimai/)
  — konkrečiai Spinta atnaujinimų naujienos.

Detalus visų versijų pakeitimų sąrašas —
[`CHANGES.rst`](https://github.com/atviriduomenys/spinta/blob/master/CHANGES.rst).

:::{warning}
Prieš atnaujinant **peržiūrėkite pakeitimus** — naujoje versijoje gali būti nesuderinamų
(*backwards incompatible*) pakeitimų. Pavyzdžiui, nuo `0.2dev22` pasikeitė numatytoji
`access` reikšmė (`protected` → `open`) — žr. [Docker diegimas](diegimas/docker.md).
:::

## Prieš pradedant

- Pasidarykite **atsargines kopijas** (konfigūracijos, o jei naudojamas vidinis
  backend'as — ir duomenų bazės).
- Planuokite **trumpą prastovą** — atnaujinimo metu Agentas bus perkrautas, todėl
  darbus atlikite ne piko metu.
- Pasižymėkite dabartinę versiją, kad galėtumėte palyginti po atnaujinimo.

## Atnaujinimas naudojant Docker

Jei Agentas paleistas per `docker compose` (žr. [Docker diegimas](diegimas/docker.md)),
iš katalogo su `docker-compose.yml` paleiskite:

```bash
# 1. Parsisiųskite naują atvaizdą iš Docker Hub
docker compose pull spinta

# 2. Perkurkite konteinerį su nauju atvaizdu
docker compose up -d

# 3. Paleiskite atnaujinimo skriptus ir duomenų bazės migracijas
docker exec spinta spinta upgrade
docker exec spinta spinta migrate

# 4. Patikrinkite versiją ir žurnalą
docker exec spinta spinta --version
docker logs spinta
```

:::{note}
Oficialiame `vssadevops/spinta` atvaizde komanda `spinta` yra tiesiai `PATH`, todėl
`docker exec spinta spinta ...` veikia be papildomų priešdėlių.

`docker compose pull` parsisiunčia tą versijos žymą, kuri nurodyta jūsų
`docker-compose.yml` (`latest` = naujausia). Norėdami pereiti prie konkrečios versijos,
faile pakeiskite žymą, pvz. `vssadevops/spinta:0.2dev26`, ir tik tada vykdykite `pull`.
:::

:::{caution}
Jei jūsų `docker-compose.yml` `command:` naudoja `poetry run` (senesni, nestandartiniai
diegimai) — nuo `0.2dev24` atvaizdas nebeturi Poetry, tad prieš `docker compose up`
pašalinkite `poetry run` priešdėlius (pvz. `poetry run uvicorn ...` → `uvicorn ...`).
Kitaip konteineris nepasileis su klaida `poetry: command not found`. Standartiniam
diegimui (su `startup.sh`) tai netaikoma.
:::

## Atnaujinimas operacinėje sistemoje

Jei Agentas įdiegtas tiesiai operacinėje sistemoje (Python virtualioje aplinkoje `env`,
žr. [Diegimas OS](diegimas/os.md)):

```bash
# 1. Atnaujinkite paketus iš to paties pinned requirements šaltinio, kaip diegiant
env/bin/pip install --require-hashes -r https://raw.githubusercontent.com/atviriduomenys/spinta/refs/heads/master/requirements/spinta-latest-pre.txt

# 2. Paleiskite atnaujinimo skriptus ir duomenų bazės migracijas
env/bin/spinta upgrade
env/bin/spinta migrate

# 3. Perkraukite Spinta servisą
sudo systemctl restart spinta

# 4. Patikrinkite būseną
systemctl status spinta
```

Kai servisas pasileidžia, versiją galima patikrinti per `/version` endpoint (žr.
[Diegimas OS](diegimas/os.md)).

## Ką daro `upgrade` ir `migrate`

Abu žingsniai svarbūs — naujoje versijoje gali keistis tiek vidinė sandara, tiek
duomenų bazės schema:

- `spinta upgrade` — paleidžia atnaujinimo skriptus, pritaikančius vidinius pakeitimus
  tarp versijų. Ką reikės atnaujinti, galima pamatyti iš anksto komanda
  `spinta upgrade --check`.
- `spinta migrate` — pritaiko duomenų struktūros (schemos) pakeitimus **vidiniam**
  backend'ui (pvz. PostgreSQL), kuriame Spinta saugo duomenis.

:::{note}
`spinta migrate` taikomas diegimams su vidiniu backend'u. Jei Agentas duomenis teikia
tiesiai iš išorinio šaltinio (pvz. išorinis SQL su DSA manifestu), `migrate` netaikomas
ir gali pranešti `NotImplementedError` — tokiu atveju šį žingsnį praleiskite. Kilus
abejonių, pirmiausia paleiskite `spinta upgrade --check`.
:::
