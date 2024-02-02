# vdl-airflow

## Miljøer

### prod 
- AirFlow URL: https://vdl.airflow.knada.io
- Git branch: `main`

 ### dev
- AirFlow URL: https://vdl-dev.airflow.knada.io
- Git branch: `dev`

## Git-instrukser

### Før utvikling

All utvikling skjer i `dev`, og ikke i egne brancher. Før man starter med utvikling må man sørge for at `dev` branchen er in sync med `main` ved å gjøre følgende:

Stå i `main` branchen.

```
git checkout main 
```

Pull endringer i `main`

```
git pull 
```

Bytt til `dev` branch 

```
git checkout dev
```
Pull endringer fra `main` til `dev`

```
git pull main
```
Nå er koden in-sync med `main` og utviklingen kan starte. 

### Deploye til prod

Når du ønsker å deploye kode til prod, må koden pushes til main-branchen. 

```
git checkout main 
```

```
git pull 
```

```
git merge dev
```

```
git push
```

```
git checkout dev
```

```
git pull main
```

```
git push
```