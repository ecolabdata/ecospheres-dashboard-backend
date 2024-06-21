# ecopsheres-catalog

## Dokku

Published on http://ecospheres-catalog-scripts.app.france.sh (dummy page).

Manages the `ecospheres-catalog` database, also used by `ecospheres-catalog-dokku` (Metabase) as a secondary database.

```shell
dokku config:set --no-restart POSTGRES_DATABASE_SCHEME=postgresql
dokku postgres:link ecospheres-catalog ecospheres-catalog-scripts
```
