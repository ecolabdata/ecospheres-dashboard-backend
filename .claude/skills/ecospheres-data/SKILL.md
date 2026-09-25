---
name: ecospheres-data
description: Query the Ecosphères dashboard database (prod) and turn the results into charts and reports. Use when the user asks about traffic, visits, downloads, long tail / concentration, Matomo stats for ecologie.data.gouv.fr, dataset or organization metrics, catalog completeness or quality over time, bouquets — or wants a graph, dashboard or report from this data.
---

# Ecosphères data analysis

This repo's `cli.py load --env prod` fills a Postgres database from data.gouv.fr, its metric API and
Matomo. This skill reads a **local copy** of that prod database and presents the results.

## 1. Connect to the local copy

Query the local restore of the prod database (`dashboard_backend_prod` in the repo's `docker compose`
Postgres, filled by `make restore_prod`). Never query the remote prod database.

Run queries with the bundled runner and the repo's venv (psycopg2 is installed there, `psql` may not be):

```sh
.venv/bin/python .claude/skills/ecospheres-data/query.py "SELECT count(*) FROM catalog WHERE NOT deleted"
.venv/bin/python .claude/skills/ecospheres-data/query.py -f <scratchpad>/my.sql
.venv/bin/python .claude/skills/ecospheres-data/query.py -f my.sql -o <scratchpad>/result.json   # full result for charts
```

It prints at most 50 rows (`-n` to change) and runs one statement per call. It connects to
`postgresql://postgres:postgres@localhost:5432/dashboard_backend_prod` unless the user has set
`ANALYTICS_DATABASE_URL`, for example because the database runs on another port.

**Don't manage the database yourself.** Don't start, stop or reconfigure containers, and don't run
`make restore_prod` or any other restore: they're the user's to run. If the connection fails, or a table is
missing or empty, say what happened and ask the user to start the database or restore it.

**Check the copy is current before any analysis.** Run:

```sql
SELECT 'datasets_metrics' AS source, max(date) FROM datasets_metrics
UNION ALL SELECT 'metrics', max(date) FROM metrics
UNION ALL SELECT 'stats', max(date) FROM stats
```

Tell the user what the latest dates mean. The latest `datasets_metrics` date is the latest traffic month;
a month's data.gouv traffic arrives on the 2nd of the next month, and `metrics` / `stats` daily. Then ask whether
this copy is recent enough for their question, or whether they want to run `make restore_prod` first. Don't
query further until they answer. Ask once per session unless they restore in between.

Only **prod** has traffic data (`datasets_metrics`, monthly org metrics, `stats`); the demo database has none.

## 2. Traps (read before writing any SQL)

- **Monthly rows are dated the 1st of the month they cover**: `datasets_metrics`, the `*_monthly` org
  traffic in `metrics`, and `stats` with `period = 'month'`. `2026-08-01` = August 2026.
- **Always filter `stats` on `period`**; `segment IS NULL` is the whole site and segments overlap (never sum
  them). Monthly unique visitors: use `period = 'month'` rows, never sum daily ones.
- **Zero traffic has no row** in `datasets_metrics` (and org traffic in `metrics`). For per-dataset
  distributions, start from `catalog WHERE NOT deleted` and `LEFT JOIN … COALESCE(value, 0)`.
- **A missing month or day is a gap, not a zero** (the cron didn't run). Detect gaps with
  `generate_series` and show them as gaps in charts; say so in the report.
- **`catalog`, `resources`, `bouquets` are current state only**, no history. Time series come from
  `metrics` (daily snapshots) and `datasets_metrics` / `stats`.
- `metrics` rows with `organization IS NULL` are global aggregates; global `nb_datasets` is a sum over
  organizations and excludes datasets without one.
- data.gouv.fr traffic (`datasets_metrics`, org traffic) covers **all of data.gouv.fr**, Matomo `stats` covers
  **ecologie.data.gouv.fr** only. Don't mix them in one number.
- `catalog.url_data_gouv` is an HTML anchor. Build links as `https://www.data.gouv.fr/fr/datasets/<dataset_id>/`.

Table-by-table semantics: [schema.md](schema.md). Exact columns: `models.py`, or `information_schema.columns`.

## 3. Query patterns

Write queries for the question at hand, not from a template. A few patterns come up in most of them:

**Per-dataset distribution for one month.** Zero-fill from `catalog`:

```sql
WITH traffic AS (
  SELECT c.dataset_id, COALESCE(m.value, 0) AS v
  FROM catalog c
  LEFT JOIN datasets_metrics m
    ON m.dataset = c.dataset_id
   AND m.measurement = 'nb_visits_monthly'
   AND m.date = date '2026-08-01'          -- August 2026
  WHERE NOT c.deleted
)
SELECT ... FROM traffic;
```

`catalog` is today's catalog, so for an older month this counts datasets added since then as zeros and leaves out
removed ones. Say so when that matters.

**Time series.** Build the calendar with `generate_series(min(date), max(date), interval '1 month')`
(or `'1 day'` for `metrics`), then `LEFT JOIN` the data to it. Months with no row at all weren't loaded:
show them as gaps and don't count them as zeros.

**Distribution shape.** Don't assume the shape; measure it and report what you find. Use ranks
(`row_number() OVER (ORDER BY v DESC)`), cumulative shares (`sum(v) OVER (ORDER BY rank) / total`), top-N and
top-X% shares, the median against the mean, order-of-magnitude buckets (`floor(log(v))`) and optionally a Gini
coefficient. A mean alone hides the shape, whatever it is.

**Month to month.** When comparing rankings across months, make sure the previous month exists. Otherwise
the result is "unknown", not "nothing in common".

Check the data range first (`SELECT min(date), max(date) FROM …`) and state it in the answer.

## 4. Present results

- Quick question → answer in the terminal with the key numbers and the traffic month(s) they cover.
- Chart or report → if the Artifact tool is available, publish an HTML artifact (load the `dataviz` and
  `artifact-design` skills first when available). Otherwise write a self-contained HTML file to the
  scratchpad and give its path.
- Export the query result with `-o …json` and embed the rows inline in the page; don't hand-copy numbers.
- Traffic values span several orders of magnitude, so log scales, ranked/cumulative (Lorenz) curves and
  order-of-magnitude buckets usually read better than linear histograms. Pick the chart for the shape you
  measured, not the one you expected.
- In every report, state: data source (data.gouv.fr metric API vs Matomo), period covered, known gaps,
  and put the SQL used in a collapsed `<details>` section so numbers can be re-checked.
- Write in the user's language (the team works in French).
