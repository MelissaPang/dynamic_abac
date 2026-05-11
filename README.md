# dynamic_abac

## Use case

This project demonstrates **dynamic row-level security (RLS)** in Unity Catalog: **which patient rows a user may see depends on who they are**, not on a static role-to-table grant.

For **each user**, the set of patients they are allowed to work with is modeled in **`staff_patient_crosswalk`**: each row ties a staff identity (for example `staff_email`) to a `patient_id`, with optional **`is_excluded`** to hide specific patients from that staff member even when a relationship exists. At query time, a scalar function **`check_patient_access`** evaluates each row’s **`patient_id`** against `CURRENT_USER()` and the crosswalk so excluded combinations drop out of the result.

The same idea extends **across a whole database (catalog)**: you attach the row filter (or, where your metastore supports it, an ABAC row-filter policy) on every patient-facing table that should honor that crosswalk—typically at **catalog or schema scope** with conditions on tags—so demographics, claims, and other PHI tables all enforce the same per-user patient list. This repo ships a **minimal schema** (`demo_dynamic_abac` by default) so you can deploy and validate the pattern end to end before rolling it out broadly.

## Tables in this schema

| Table | Role |
|--------|------|
| **`patient_demographics`** | Base **Delta** table: synthetic patient demographics (10 rows). **No** row filter; use as “full copy” reference. |
| **`patient_claims`** | Base **Delta** table: synthetic claims (100 rows). **No** row filter. |
| **`staff_patient_crosswalk`** | **Delta** table: which staff may see which `patient_id`, and whether that link is **`is_excluded`**. The row-filter UDF **reads this table**; grant analysts **SELECT** here if they should be subject to the filter. |
| **`patient_demographics_with_abac`** | Copy of demographics with the same columns as the base table; **`check_patient_access`** is applied on **`patient_id`** so visibility matches the crosswalk rules. |
| **`patient_claims_with_abac`** | Copy of claims with the row filter on **`patient_id`**, aligned with the same rules. |

The **`seed_patient_tables`** job (via `set_up.py`) materializes these tables from bundled CSVs in the `dynamic_abac_demo` wheel, then applies **`check_patient_access`** (Unity Catalog row filter, with optional ABAC policy attempt where tags are policy-enabled—see `sql/abac_row_filter.sql`). A follow-on notebook task **`verify_abac_policies.ipynb`** compares base vs `*_with_abac` counts for `CURRENT_USER()`.

**Catalog** and **schema** are bundle variables (`catalog` required for your workspace; schema defaults to **`demo_dynamic_abac`**).

## Prerequisites

- Databricks CLI v0.200+ with auth to your workspace (`databricks auth login` or a profile in `~/.databrickscfg`).
- A Unity Catalog **catalog** your user can write to (the bundle does not create the catalog).
- Network access during `databricks bundle deploy` so the wheel build can `pip install build hatchling` into a local `.bundle-venv/`.

## Deploy and run

From the repo root (requires `databricks` on your `PATH`):

```bash
python set_up.py --catalog <catalog> [--profile <profile name>]
```

`--profile` is optional; use it when you have several entries in `~/.databrickscfg` and need to pick a workspace. If `python` is not available, use `python3` the same way.

After a successful run, open the **`verify_abac_notebook`** task in the job UI to inspect **`verify_abac_policies`** outputs, or query tables directly, for example `SELECT * FROM <catalog>.demo_dynamic_abac.patient_claims LIMIT 5`.

## Layout

| Path | Purpose |
|------|---------|
| `set_up.py` | Runs `bundle deploy` then `bundle run seed_patient_tables` with your `--catalog` / `--schema` / optional `--profile` |
| `databricks.yml` | Bundle metadata, variables, wheel artifact build |
| `resources/seed_patient_tables.job.yml` | Serverless job running the `seed` console script |
| `src/dynamic_abac_demo/` | Wheel package: `seed_tables.py` + `bootstrap_data/*.csv` |
| `notebooks/verify_abac_policies.ipynb` | Compare base vs `*_with_abac` visibility for `CURRENT_USER()` |
| `sql/patient_tables.sql` | Reference DDL (logical shape); actual tables are Delta from the job |
| `sql/abac_row_filter.sql` | Example UC row-filter / optional ABAC policy DDL |

## Local wheel build (optional)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install build hatchling && python -m build --wheel .
```
