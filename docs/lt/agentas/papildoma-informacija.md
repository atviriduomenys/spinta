# Papildoma informacija

Norint pasitikrinti, ar Spinta versija yra naujausia, galima paleisti šias dvi eilutes:

```bash
spinta --version
pip index versions --pre spinta
```

Jei versijos sutampa - naujinti nereikia.

Jei versijos skiriasi - reikėtų atnaujinti Spintą.

Visus naujausius ir kitoje versijoje numatomus pakeitimus galima rasti čia: [Spinta - Github](https://github.com/atviriduomenys/spinta/blob/master/CHANGES.rst "Spinta - Github")

Norint atnaujinti Spinta versiją, reikia įvykdyti tokias komandas:

```bash
sudo -Hsu spinta
cd
env/bin/pip install --require-hashes -r https://raw.githubusercontent.com/atviriduomenys/spinta/refs/heads/master/requirements/spinta-latest-pre.txt
```

## Struktūros aprašo naujinimas

Pasikeitus struktūros aprašui jį atnaujinti galima šiomis komandomis:

```bash
sudo cp manifest.csv /opt/spinta/manifest-new.csv

sudo -Hsu spinta
cd

env/bin/spinta check manifest-new.csv

cp manifest.csv manifest-old.csv
mv manifest-new.csv manifest.csv

diff -y --suppress-common-lines manifest-old.csv manifest.csv

exit

sudo systemctl restart spinta
sudo systemctl status spinta
```

## Kelių struktūros aprašų naudojimas

Jei naudojate daugiau, nei vieną manifestą, ir norite, kad Agentas pasiektų juos visus, galima tai padaryti operatoriaus sync pagalba (konfigūracijos faile) (neaktualu su SOAP/WSDL):

```bash
manifests: 
  default: 
    type: tabular 
    path: /opt/spinta/manifest.csv 
    backend: default 
    mode: external 
    sync: additional 
  additional: 
    type: tabular 
    path: /opt/spinta/manifest2.csv 
    backend: default 
    mode: external 
```

## Problemos ir sprendimai

Jei kažkas neveikia, pirmiausiai reikėtų žiūrėti servisų žurnalus, pavyzdžiui:

```bash
journalctl -u spinta -xe 
journalctl -u nginx -xe
```

Žurnaluose dažniausiai būna pateikta informacija, leidžianti suprasti kas ir kodėl neveikia.
