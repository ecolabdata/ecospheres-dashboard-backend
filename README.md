# ecospheres-dashboard-backend

A script that synchronizes data.gouv.fr's Ecospheres-related data to a Postgres database. This database is used to build dashboards.

## Getting started

### Spin off the database

```shell
docker compose up
```

### Use the script

Install the required dependencies through `requirements.txt`.

Export the env var needed for the script to find the database:

```shell
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/demo
```

Launch the main script:

```shell
python cli.py load
```

It will download the catalog from data.gouv.fr and update or create the rows in the various tables. Metrics will be computed for the current day (run it multiple days in a row to have historical depth).

## Dokku

Published on http://ecospheres-catalog-scripts.app.france.sh (dummy page).

Manages the `ecospheres-catalog` database, also used by `ecospheres-catalog-dokku` (Metabase) as a secondary database.

```shell
dokku config:set --no-restart POSTGRES_DATABASE_SCHEME=postgresql
dokku postgres:link ecospheres-catalog ecospheres-catalog-scripts
```
