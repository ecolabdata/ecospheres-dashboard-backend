# ecospheres-dashboard-backend

A script that synchronizes data.gouv.fr's Ecospheres-related data to a Postgres database. This database is used to build dashboards.

## Getting started

### Spin off the database

```shell
docker compose up
```

### Use the script

Install the required dependencies through `requirements.txt` or `requirements-dev.txt`

Export the env var needed for the script to find the database:

```shell
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/dashboard_backend
```

Bootstrap the database:

```shell
python cli.py init-db
```

Launch the main script:

```shell
python cli.py load --env=(demo|prod)
```

It will download the catalog from data.gouv.fr and update or create the rows in the various tables. Metrics will be computed for the current day (run it multiple days in a row to have historical depth).

## Schema changes

### Using alembic

Apply pending migrations:

```shell
ALEMBIC_ENV=(demo|prod) alembic upgrade head
```

`demo` env will use `DATABASE_URL` env var, `prod` env will use `DATABASE_URL_PROD` env var (same as the load script).

Create a new migration (diff code schema and database schema):

```shell
ALEMBIC_ENV=(demo|prod) alembic revision --autogenerate -m "message"
```

### Legacy

- 2024-10-08: `catalog.harvest_extras` has been deprecated, `catalog.harvest` is now used. Quick migration: `ALTER TABLE catalog DROP COLUMN IF EXISTS harvest_extras;`
- 2024-11-02: the migration to SQLAlchemy [#19](https://github.com/ecolabdata/ecospheres-dashboard-backend/pull/19) introduces migrations support.

## Linting

Linting, formatting and import sorting are done automatically by [Ruff](https://docs.astral.sh/ruff/) launched by a pre-commit hook. So, before contributing to the repository, it is necessary to initialize the pre-commit hooks:

```bash
pre-commit install
```
Once this is done, code formatting and linting, as well as import sorting, will be automatically checked before each commit.

If you cannot use pre-commit, it is necessary to format, lint, and sort imports with [Ruff](https://docs.astral.sh/ruff/) before committing:

```bash
ruff check --fix .
ruff format .
```

> WARNING: running `ruff` on the codebase will lint and format all of it, whereas using `pre-commit` will only be done on the staged files

## Dokku

Published on http://ecospheres-catalog-scripts.app.france.sh (dummy page).

Manages the `ecospheres-catalog` database, also used by `ecospheres-catalog-dokku` (Metabase) as a secondary database.

```shell
dokku config:set --no-restart POSTGRES_DATABASE_SCHEME=postgresql
dokku postgres:link ecospheres-catalog ecospheres-catalog-scripts
```
