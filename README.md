# dynamic_abac

Synthetic **patient demographics** (10 rows) and **patient claims** (100 rows) are shipped inside the `dynamic_abac_demo` Python wheel. A **Databricks Asset Bundle (DAB)** deploys a serverless job that writes them as **Unity Catalog Delta** tables:

`{catalog}.{schema}.patient_demographics`  
`{catalog}.{schema}.patient_claims`  

The same job then creates Delta copies in that schema (same data and columns): **`patient_demographics_with_abac`** and **`patient_claims_with_abac`**.

Those two tables get a **Unity Catalog row filter** via scalar function **`check_patient_access`** (argument is each row’s **`patient_id`**): if `CURRENT_USER()` matches **`staff_patient_crosswalk.staff_email`** for a row where **`is_excluded = 1`** for that patient, that patient’s demographics and claims rows are hidden (for example, staff on **S003** with **P0007** excluded will not see **P0007**). Anyone not matching such a crosswalk row still sees all patients. Query users need **SELECT** on `staff_patient_crosswalk` and **EXECUTE** on `check_patient_access` (see comments in `sql/abac_row_filter.sql` for example grants).

The **`seed_patient_tables`** job runs end to end: **(1)** wheel task seeds base tables, crosswalk, `_with_abac` copies, and attaches the row filter; **(2)** notebook task **`verify_abac_policies.ipynb`** runs with the same **`catalog`** / **`schema`** widgets so you can see counts and `EXCEPT` proofs in the job run output.

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

Or run deploy + job in one step from the repo root (requires ``databricks`` on your PATH):

```bash
python set_up.py --catalog YOUR_CATALOG --profile YOUR_CLI_PROFILE
```

After a successful run, open the **`verify_abac_notebook`** task run in the job UI to inspect **`verify_abac_policies`** outputs (counts and `EXCEPT` lists), or query tables directly, e.g. `` SELECT * FROM my_catalog.demo_dynamic_abac.patient_claims LIMIT 5 ``.

## Layout

| Path | Purpose |
|------|---------|
| `set_up.py` | CLI wrapper: ``bundle deploy`` then ``bundle run seed_patient_tables`` with ``--catalog`` / ``--schema`` |
| `databricks.yml` | Bundle metadata, variables, wheel artifact build |
| `resources/seed_patient_tables.job.yml` | Serverless job running the `seed` console script |
| `src/dynamic_abac_demo/` | Wheel package: `seed_tables.py` + `bootstrap_data/*.csv` |
| `notebooks/verify_abac_policies.ipynb` | Run in a workspace to compare base vs `*_with_abac` visibility for `CURRENT_USER()` |
| `sql/patient_tables.sql` | Reference DDL (logical shape); actual tables are Delta from the job |
| `sql/abac_row_filter.sql` | Example UC row-filter DDL (catalog/schema placeholders); seed job applies the same logic with your variables |

## Local wheel build (optional)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install build hatchling && python -m build --wheel .
```
