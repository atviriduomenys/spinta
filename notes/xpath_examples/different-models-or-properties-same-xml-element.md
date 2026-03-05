## Problema

Kartais reikia, kad iš vieno XML lauko susikurtų du skirtingi modeliai arba dvi skirtingos properties. 

Pavyzdžiui - ILTU kodas, kuris kartais pateikiamas tame pačiame lauke, kaip ir asmens kodas. 
ILTU kodas skirtas identifikuoti užsienio šalių gyvventojus, panašiai, kaip asmens kodas skirtas Lietuvos gyventojus. 

## XML pavyzdys

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Asmenys>
  <Asmuo>
    <AsmensKodas>91234567890</AsmensKodas>
    <Vardas>John</Vardas>
    <Pavardė>Smith</Pavardė>
  </Asmuo>
  <Asmuo>
    <AsmensKodas>41234567890</AsmensKodas>
    <Vardas>Ona</Vardas>
    <Pavardė>Onaitė</Pavardė>
  </Asmuo>
  <Asmuo>
    <AsmensKodas>31234567890</AsmensKodas>
    <Vardas>Petras</Vardas>
    <Pavardė>Petraitis</Pavardė>
  </Asmuo>
</Asmenys>
```

## DSA pavyzdžiai

Tokiu atveju rekomenduojame atlikti filtravimą, naudojant skirtingus kondicinius XPath. 
`source` reikia įrašyti XPath kelią iki elemento ir filtrą:

### 1. Kai norima dviejų modelių, vieno - Lietuvos gyventojams, kito - užsienio šalių gyventojams. 
   XPath filtras rašomas prie modelio `source`:


```csv
id,dataset,resource,base,model,property,type,ref,source,source.type,prepare,origin,count,level,status,visibility,access,uri,eli,title,description
,dataset,,,,,,,,,,,,,,,,,,,
,,resource,,,,dask/xml,,demo/asmenys.xml,,,,,,,,,,,,
,,,,,,,,,,,,,,,,,,,,
,,,,Asmuo,,,,"/Asmenys/Asmuo[not(starts-with(AsmensKodas, '9'))]",,,,,,develop,private,open,,,,
,,,,,asmens_kodas,integer unique,,AsmensKodas,,,,,,develop,private,open,,,,
,,,,,vardas,string required unique,,Vardas,,,,,,develop,private,open,,,,
,,,,,pavarde,string required unique,,Pavardė,,,,,,develop,private,open,,,,
,,,,UzsienioAsmuo,,,,"/Asmenys/Asmuo[starts-with(AsmensKodas, '9')]",,,,,,develop,private,open,,,,
,,,,,iltu_kodas,integer unique,,AsmensKodas,,,,,,develop,private,open,,,,
,,,,,vardas,string required unique,,Vardas,,,,,,develop,private,open,,,,
,,,,,pavarde,string required unique,,Pavardė,,,,,,develop,private,open,,,,
```

### 2. Kai norima tame pačiame modelyje turėti dvi properties, vieną asmens kodui, kitą - ILTU kodui.
   XPath filtras rašomas prie `property` `source`:

```csv
id,dataset,resource,base,model,property,type,ref,source,source.type,prepare,origin,count,level,status,visibility,access,uri,eli,title,description
,dataset,,,,,,,,,,,,,,,,,,,
,,resource,,,,dask/xml,,demo/asmenys.xml,,,,,,,,,,,,
,,,,,,,,,,,,,,,,,,,,
,,,,Asmuo,,,,/Asmenys/Asmuo,,,,,,develop,public,open,,,,
,,,,,asmens_kodas,string unique,,"AsmensKodas[not(starts-with(., '9'))]",,,,,,develop,public,open,,,,
,,,,,iltu_kodas,string unique,,"AsmensKodas[starts-with(., '9')]",,,,,,develop,public,open,,,,
,,,,,vardas,string required unique,,Vardas,,,,,,develop,public,open,,,,
,,,,,pavarde,string required unique,,Pavardė,,,,,,develop,public,open,,,,
```

## UDTS rezultatų pavyzdžiai

### 1. Su dviem modeliais, paprašius duomenų, gaunamas toks rezultatas:

Modelis `Asmuo`:

```json
http http://localhost:8000/dataset/Asmuo

{
    "_data": [
        {
            "_id": "2ffa03d1-b348-4842-b040-edadcc674361",
            "_revision": null,
            "_type": "dataset/Asmuo",
            "asmens_kodas": 41234567890,
            "pavarde": "Onaitė",
            "vardas": "Ona"
        },
        {
            "_id": "c488030e-09a9-4ea5-b2cc-7d33a59e2bf8",
            "_revision": null,
            "_type": "dataset/Asmuo",
            "asmens_kodas": 31234567890,
            "pavarde": "Petraitis",
            "vardas": "Petras"
        }
    ]
}
```

Modelis `UzsienioAsmuo`:

```json
http http://localhost:8000/dataset/UzsienioAsmuo

{
    "_data": [
        {
            "_id": "8d2e9a7d-295c-43f1-838c-925adc374c52",
            "_revision": null,
            "_type": "dataset/UzsienioAsmuo",
            "iltu_kodas": 91234567890,
            "pavarde": "Smith",
            "vardas": "John"
        }
    ]
}

```

### 2. Su vienu modeliu, skirtingomis properties, gaunamas toks rezultatas:

```json
 http http://localhost:8000/dataset/Asmuo

{
    "_data": [
        {
            "_id": "c299151e-4168-4845-a5dc-d3555badfeae",
            "_revision": null,
            "_type": "dataset/Asmuo",
            "asmens_kodas": null,
            "iltu_kodas": "91234567890",
            "pavarde": "Smith",
            "vardas": "John"
        },
        {
            "_id": "b5dcd297-7e07-41e5-b994-5c53af158414",
            "_revision": null,
            "_type": "dataset/Asmuo",
            "asmens_kodas": "41234567890",
            "iltu_kodas": null,
            "pavarde": "Onaitė",
            "vardas": "Ona"
        },
        {
            "_id": "915557f6-290d-4dc2-93e3-88877a3acb16",
            "_revision": null,
            "_type": "dataset/Asmuo",
            "asmens_kodas": "31234567890",
            "iltu_kodas": null,
            "pavarde": "Petraitis",
            "vardas": "Petras"
        }
    ]
}
```

