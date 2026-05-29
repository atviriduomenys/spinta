# DroneCI procedūros

> Pastaba: daroma prielaida, kad git šaka (branch) arba žyma (tag) jau yra git repozitorijoje.

## Laikinoji testavimo aplinka

Iš Drone (https://drone.data.gov.lt/):

* pasirinkti repozitoriją;
* spausti „New build";
* įvesti šakos pavadinimą ir spausti „Create";
* aplinka pasikels ir bus pasiekiama iš subdomeno su identišku šakos pavadinimu.

## Development aplinka

Iš Drone (https://drone.data.gov.lt/):

* pasirinkti repozitoriją;
* spausti „New build";
* įvesti pagrindinės šakos pavadinimą ir spausti „Create" (diegimas vyksta tik iš pagrindinės šakos, kitos šakos bus diegiamos kaip laikinos aplinkos).

## Docker publikavimas

Iš Drone (https://drone.data.gov.lt/):

* pasirinkti spintos repozitoriją;
* spausti „New build";
* įvesti pagrindinės žymos (tag) pavadinimą formatu `refs/tags/<git tag>`.
* Docker image bus publikuotas: https://hub.docker.com/r/vssadevops/spinta/tags
