# dynamic_abac

## Use case

This project demonstrates **dynamic row-level security (RLS)** in Unity Catalog: **which patient rows a user may see depends on who they are**, not on a static role-to-table grant.

For **each user**, the set of patients they are allowed to work with is modeled in **`staff_patient_crosswalk`**: each row ties a staff identity (for example `staff_email`) to a `patient_id`, with optional **`is_excluded`** to hide specific patients from that staff member even when a relationship exists. At query time, a scalar function **`check_patient_access`** evaluates each row’s **`patient_id`** against `CURRENT_USER()` and the crosswalk so excluded combinations drop out of the result.

The same idea extends **across a whole database (catalog)**: you attach the same **ABAC row-filter policy pattern** on every patient-facing table that should honor that crosswalk (this demo uses **one policy per table**, principals **`All account users`** then **`account users`**, **existing** Unity Catalog **governed** tags—**`abac_tag_key`** + **`abac_tag_value`** (default **`true`**) on each `*_with_abac` table, and **`abac_tag_key2`** with optional **`abac_tag_key2_value`** on **`patient_id`** (empty = key-only + `has_tag` in `MATCH COLUMNS`), plus UDF **`check_patient_access`**). Most teams **reuse tags already defined** on the account rather than inventing new ones. At catalog or schema scale you repeat that pattern with shared tags and UDFs. This repo ships a **minimal schema** (`demo_dynamic_abac` by default) so you can deploy and validate the pattern end to end before rolling it out broadly.

## Tables in this schema

| Table | Role |
|--------|------|
| **`patient_demographics`** | Base **Delta** table: synthetic patient demographics (10 rows). **No** row filter; use as “full copy” reference. |
| **`patient_claims`** | Base **Delta** table: synthetic claims (100 rows). **No** row filter. |
| **`staff_patient_crosswalk`** | **Delta** table: which staff may see which `patient_id`, and whether that link is **`is_excluded`**. The row-filter UDF **reads this table**; grant analysts **SELECT** here if they should be subject to the filter. |
| **`patient_demographics_with_abac`** | Copy of demographics; **SET TAG ON TABLE** using your **existing governed** **`abac_tag_key`** + **`abac_tag_value`** (or key-only if value empty), **SET TAG ON COLUMN** … **`patient_id`** with **existing governed** **`abac_tag_key2`** + optional **`abac_tag_key2_value`**. Policy **`MATCH COLUMNS`** uses **`has_tag`** or **`has_tag_value`** on that column tag to pass **`patient_id`** into **`check_patient_access`**. |
| **`patient_claims_with_abac`** | Same two-tag pattern + **`dynamic_abac_row`** policy as demographics. |

The **`seed_patient_tables`** job (via `set_up.py`) materializes these tables from bundled CSVs in the `dynamic_abac_demo` wheel, creates the UDF, applies **existing governed** tag keys (and values) to `*_with_abac` tables, and creates **per-table ABAC row-filter policies** (see `sql/abac_row_filter.sql`). A follow-on notebook task **`verify_abac_policies.ipynb`** compares base vs `*_with_abac` counts for `CURRENT_USER()`.

**Catalog** and **schema** are bundle variables (`catalog` required for your workspace; schema defaults to **`demo_dynamic_abac`**).

## Configure tags and catalog (`databricks.yml`)

**Before your first deploy**, edit **`databricks.yml`** so the bundle variables match **your** Unity Catalog account. In practice you **point the variables at governed tags that already exist** on the account (the usual approach); the job **does not create** new governed tags. It only runs **`SET TAG`** and **`CREATE POLICY`** using keys and values that **already exist** and are allowed for tag-based row policies.

Update at least:

| Variable | What to change |
|----------|------------------|
| **`catalog`** | UC catalog where tables are written (must already exist). |
| **`schema`** | Schema under that catalog (created by the job if missing). |
| **`abac_tag_key`** | **Existing governed** tag **key** on each `*_with_abac` table (already defined at account level). |
| **`abac_tag_value`** | Value your **existing** table tag allows (for example `true`), or an empty default only if key-only is valid on your account. |
| **`abac_tag_key2`** | **Existing governed** tag **key** on `patient_id`. |
| **`abac_tag_key2_value`** | Value allowed for that **existing** column tag (often an enum such as `ssn`, `address`, …). Use an empty or whitespace-only default only if your tag allows key-only on the column. |

Under **`targets`**, set **`workspace.host`** and **`workspace.profile`** (or your auth flow) for the workspace you use.

**Alternatives:** you can leave `databricks.yml` as-is and pass overrides with **`databricks bundle deploy --var abac_tag_key=…`** / **`bundle run --var …`**, or use the matching **`set_up.py`** flags (`--abac-tag-key`, `--abac-tag-value`, `--abac-tag-key2`, `--abac-tag-key2-value`); those are forwarded as the same bundle variables.

## Prerequisites

- Databricks CLI v0.200+ with auth to your workspace (`databricks auth login` or a profile in `~/.databrickscfg`).
- A Unity Catalog **catalog** your user can write to (the bundle does not create the catalog).
- **ABAC policies:** use **existing governed** tags—set **their** keys and allowed values in **`databricks.yml`** (see above) or override with **`--var`** / **`set_up.py`** flags. You need **APPLY TAG** (and related grants) on those tags and **MANAGE** on the tables as your admins require.
- Network access during `databricks bundle deploy` so the wheel build can `pip install build hatchling` into a local `.bundle-venv/`.

## Deploy and run

From the repo root (requires `databricks` on your `PATH`):

```bash
python set_up.py --catalog <catalog> [--profile <profile name>] [--abac-tag-key KEY] [--abac-tag-value VAL] [--abac-tag-key2 KEY2] [--abac-tag-key2-value VAL2]
```

`--profile` is optional when your bundle `workspace.profile` or `DATABRICKS_CONFIG_PROFILE` resolves auth. Tag flags are forwarded as bundle vars (use **`--abac-tag-value ""`** for a key-only table tag if your tag allows it). If `python` is not available, use `python3` the same way.

After a successful run, open the **`verify_abac_notebook`** task in the job UI to inspect **`verify_abac_policies`** outputs, or query tables directly, for example `SELECT * FROM <catalog>.demo_dynamic_abac.patient_claims LIMIT 5`.

## Layout

| Path | Purpose |
|------|---------|
| `set_up.py` | Runs `bundle deploy` then `bundle run seed_patient_tables` with your `--catalog` / `--schema` / optional `--profile` |
| `databricks.yml` | Bundle metadata, **tag/catalog variables you should customize**, wheel artifact build |
| `resources/seed_patient_tables.job.yml` | Serverless job running the `seed` console script |
| `src/dynamic_abac_demo/` | Wheel package: `seed_tables.py` + `bootstrap_data/*.csv` |
| `notebooks/verify_abac_policies.ipynb` | Compare base vs `*_with_abac` visibility for `CURRENT_USER()` |
| `sql/patient_tables.sql` | Reference DDL (logical shape); actual tables are Delta from the job |
| `sql/abac_row_filter.sql` | Example UDF + tags + per-table ABAC row-filter policy DDL |

## Local wheel build (optional)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install build hatchling && python -m build --wheel .
```
