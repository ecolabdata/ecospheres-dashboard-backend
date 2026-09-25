# Schema reference

What each table *means*. For exact columns, see `models.py` or query
`information_schema.columns`. Everything is loaded by `cli.py load --env prod`, which a
dokku cron runs daily at 06:32.

## catalog: datasets in the universe (current state only)

One row per dataset that is, or was, an element of the `univers-ecospheres` topic on
data.gouv.fr. There is **no history**: each load marks every row `deleted = true`, then
upserts the datasets still in the topic. Private datasets are skipped.

- `dataset_id` is the data.gouv.fr id, and the join key everywhere (`datasets_metrics.dataset`,
  `resources.dataset_id`, `datasets_bouquets.dataset_id`).
- `organization` is a FK to `organizations.organization_id`. It can be NULL for datasets owned by a user (`owner`).
- `url_data_gouv` is an **HTML `<a>` tag**, not a URL. Build links as
  `https://www.data.gouv.fr/fr/datasets/<dataset_id>/`.
- `has_*` columns are completeness indicators, such as `has_license`, `has_spatial__zones`,
  `has_temporal_coverage`, `has_contact_points`, `has_harvest__remote_url`.
- `quality__score` is data.gouv's quality score (0–1). `quality__score__bin` / `_bin_label` are
  pre-bucketed (<0.2, <0.4, …).
- `description__length__bin` / `_bin_label` hold the description length (<200, <1000, <5000, ≥5000).
- `harvest__*` hold harvest metadata. `prefix_harvest_remote_url` is the URL minus its last
  segment, which is useful to group datasets by source catalogue.
- `tags` is a text[].
- `frequency`, `license`, `license__title`, `created_at`, `last_modified` come straight from the data.gouv.fr API.

## resources: files attached to catalog datasets

The table is **fully deleted and reloaded** on each load, so it holds the current state only. `available`
comes from data.gouv's `check:available`. `format`, `type` and `schema__name` are useful for
breakdowns. `*__exists` columns are completeness indicators.

## organizations

`organization_id`, `name`, `acronym`, `type` (the universe's own classification of the org, from the
ecospheres-universe config), and `service_public` (true when the org has both the `public-service` and `certified` badges).

## bouquets / datasets_bouquets

Bouquets are data.gouv "topics" tagged with the universe name. `theme` is resolved from tags, and
`deleted` / `private` are flags. `nb_datasets` (universe datasets and other data.gouv datasets),
`nb_datasets_external` (a URI outside data.gouv), `nb_factors_missing`, `nb_factors_not_available`
and `nb_factors` (the total) describe each bouquet's contents.
`datasets_bouquets` is rebuilt on each load. It only links bouquets to datasets that are in `catalog`.

## metrics: time series (organization level and global)

`(date, measurement, value, organization)`. `organization IS NULL` marks a global row.
**It mixes two kinds of rows:**

1. **Daily catalog snapshots** from `compute_metrics`, dated the day of the run:
   - `nb_organizations` (global).
   - `nb_datasets` per org, plus a global one that is **the sum over orgs**, so it leaves out datasets with no org.
   - `nb_<indicator>` per org and global, where the indicator is any `has_*` field without the `has_` prefix: `nb_license`, `nb_harvest`,
     `nb_harvest__created_at`, `nb_harvest__issued_at`, `nb_harvest__modified_at`,
     `nb_harvest__remote_id`, `nb_harvest__remote_url`, `nb_resources__total`, `nb_spatial__zones`,
     `nb_spatial__geom`, `nb_temporal_coverage`, `nb_frequency`, `nb_contact_points`.
   - `avg_quality__score` per org and global.
   - Global bouquet counts: `nb_datasets_from_universe_in_bouquets`, `nb_bouquets_public`,
     `nb_datasets_in_bouquets_public`, `nb_datasets_external_in_bouquets_public`,
     `nb_factors_in_bouquets_public`, `nb_factors_missing_in_bouquets_public`,
     `nb_factors_not_available_in_bouquets_public`.
2. **Monthly data.gouv.fr traffic per organization** (prod only, loaded on the 2nd of the next month):
   `nb_visits_datasets_monthly` and `nb_downloads_resources_monthly`, org rows only (no global total).
   These count **all of the org's datasets on data.gouv.fr**, not only those in the universe. They follow
   the date and zero conventions of `datasets_metrics` described below.

Days when the cron did not run are simply missing. For example, it was disabled for a while during a data.gouv IP block.

## datasets_metrics: monthly data.gouv.fr traffic per dataset (prod only)

`(date, measurement, value, dataset)`. The measurements are `nb_visits_monthly` (dataset page visits) and
`nb_downloads_resources_monthly` (downloads of the dataset's resources). The source is data.gouv.fr's metric API,
which is **all of data.gouv.fr**, not only ecologie.data.gouv.fr.

- `date` is the **1st of the traffic month**, so `2026-08-01` holds August 2026.
- There are **no zero rows**. A dataset with no traffic that month has no row, so start from `catalog` and
  `LEFT JOIN ... COALESCE(value, 0)`.
- The data is loaded only if the cron ran on the 2nd of the next month, so a whole month can be missing. That is a
  gap, not zero traffic. The user can backfill a month with `cli.py load-datagouvfr-metrics --env prod --month YYYY-MM`
  (it only covers datasets currently in the universe).

## stats: Matomo analytics for ecologie.data.gouv.fr (prod only)

One row per `(date, segment, period)`. The columns are mapped from Matomo: `nb_visits`, `nb_uniq_visitors`,
`nb_actions`, `nb_pageviews`, `nb_downloads`, `bounce_rate` (0–1), `avg_time_on_site` (seconds),
`nb_uniq_visitors_new` / `_returning`, and others.

- `period = 'day'`: `date` is that day. `period = 'month'`: `date` is the **1st of that same month**
  (same convention as `datasets_metrics`). Rows that existed before the column was added defaulted to `'day'`.
- **Always filter on `period`.** For monthly unique visitors, averages or rates, use the `'month'` rows. Summing
  daily `nb_uniq_visitors` overcounts.
- `segment IS NULL` is the whole site. The other segments are `/datasets`, `/bouquets`, `/indicators` and `/dataservices`,
  each a "page URL contains" match. They **overlap and are not additive** (one visit can appear in several).
- The current month's `'month'` row is only written once the month is over.
