# dynamic_abac

Synthetic **patient demographics** (10 rows) and **patient claims** (100 rows) are shipped inside the `dynamic_abac_demo` Python wheel. A **Databricks Asset Bundle (DAB)** deploys a serverless job that writes them as **Unity Catalog Delta** tables:

`{catalog}.{schema}.patient_demographics`  
`{catalog}.{schema}.patient_claims`  

The same job then creates Delta copies in that schema (same data and columns): **`patient_demographics_with_abac`** and **`patient_claims_with_abac`**.

The schema defaults to **`demo_dynamic_abac`**. The **catalog** is a bundle variable you set for your workspace.

## Prerequisites

- Databricks CLI v0.200+ with auth to your workspace (`databricks auth login` or a profile in `~/.databrickscfg`).
- A Unity Catalog **catalog** your user can write to (the bundle does not create the catalog).
- Network access during `databricks bundle deploy` so the wheel build can `pip install build hatchling` into a local `.bundle-venv/`.

## Configure catalog (and optional schema)

Edit defaults in `databricks.yml` under `variables`, or override at deploy time:

```bash
databricks bundle deploy -t dev --var catalog=my_catalog --var schema=demo_dynamic_abac
```

## Deploy and run

```bash
databricks bundle deploy -t dev
databricks bundle run seed_patient_tables -t dev
```

After a successful run, query e.g. `` SELECT * FROM my_catalog.demo_dynamic_abac.patient_claims LIMIT 5 `` (replace with your catalog).

## Layout

| Path | Purpose |
|------|---------|
| `databricks.yml` | Bundle metadata, variables, wheel artifact build |
| `resources/seed_patient_tables.job.yml` | Serverless job running the `seed` console script |
| `src/dynamic_abac_demo/` | Wheel package: `seed_tables.py` + `bootstrap_data/*.csv` |
| `sql/patient_tables.sql` | Reference DDL (logical shape); actual tables are Delta from the job |

## Local wheel build (optional)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install build hatchling && python -m build --wheel .
```
