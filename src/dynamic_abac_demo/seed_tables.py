"""Create UC schema and overwrite Delta tables from bundled CSV fixtures."""

from __future__ import annotations

import argparse
import csv
import subprocess
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def _bootstrap_dir() -> Path:
    return Path(__file__).resolve().parent / "bootstrap_data"


def _read_fixture_csv(spark: SparkSession, path: Path):
    """Load a small CSV from the wheel using the driver; works with Spark Connect (no cluster-local paths)."""
    with path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        raise ValueError(f"No rows in fixture CSV: {path}")
    return spark.createDataFrame(rows)


def _git_user_email_near(source_file: str) -> str | None:
    """Return `git config user.email` from the nearest ancestor directory that contains `.git`."""
    p = Path(source_file).resolve().parent
    for _ in range(12):
        if (p / ".git").exists():
            try:
                proc = subprocess.run(
                    ["git", "-C", str(p), "config", "user.email"],
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=False,
                )
                if proc.returncode == 0 and (email := proc.stdout.strip()):
                    return email
            except (OSError, subprocess.SubprocessError):
                return None
            return None
        if p.parent == p:
            break
        p = p.parent
    return None


def _set_tag_sql_allow_duplicate(spark: SparkSession, sql: str) -> None:
    """Run SET TAG …; UC raises if the same tag key is already assigned (idempotent re-seed)."""
    try:
        spark.sql(sql)
    except Exception as e:
        if "UC_DUPLICATE_TAG_ASSIGNMENT" in str(e):
            return
        raise


def _apply_abac_row_filters(spark: SparkSession, catalog: str, schema: str) -> None:
    """Hide patient rows when crosswalk marks is_excluded = 1 for the current user.

    Tries Unity Catalog ABAC ``CREATE POLICY`` (row filter UDF + tag conditions) first.
    If the metastore does not expose the tag keys for policy conditions (common on ad-hoc
    tags), falls back to ``ALTER TABLE ... SET ROW FILTER``. See sql/abac_row_filter.sql.
    """
    fq = lambda name: f"`{catalog}`.`{schema}`.`{name}`"
    fq_schema = f"`{catalog}`.`{schema}`"
    fq_fn = f"`{catalog}`.`{schema}`.`check_patient_access`"

    spark.sql(
        f"""
        CREATE OR REPLACE FUNCTION {fq("check_patient_access")}(pid STRING)
        RETURNS BOOLEAN
        RETURN ((
          SELECT CASE
            WHEN EXISTS (
              SELECT 1
              FROM {fq("staff_patient_crosswalk")} c
              WHERE c.staff_email = CURRENT_USER()
                AND c.patient_id = pid
                AND CAST(c.is_excluded AS INT) = 1
            ) THEN FALSE
            ELSE TRUE
          END
        ))
        """
    )

    abac_policy_sql = f"""
        CREATE OR REPLACE POLICY patient_crosswalk_abac
        ON SCHEMA {fq_schema}
        COMMENT 'Hide patient rows when staff crosswalk marks is_excluded = 1 for current user and patient_id'
        ROW FILTER {fq_fn}
        TO `account users`
        FOR TABLES
        WHEN has_tag_value('dynamic_abac_table', 'true')
        MATCH COLUMNS has_tag_value('dynamic_abac_table', 'true') AS pid
        USING COLUMNS (pid)
        """

    try:
        for tbl in ("patient_demographics_with_abac", "patient_claims_with_abac"):
            _set_tag_sql_allow_duplicate(
                spark, f"SET TAG ON TABLE {fq(tbl)} dynamic_abac_table = true"
            )
            _set_tag_sql_allow_duplicate(
                spark,
                f"SET TAG ON COLUMN {fq(tbl)}.patient_id dynamic_abac_table = true",
            )
        spark.sql(abac_policy_sql)
    except Exception as e:
        msg = str(e)
        if "UC_INVALID_POLICY_CONDITION" not in msg and "Unknown tag policy key" not in msg:
            raise
        for tbl in ("patient_demographics_with_abac", "patient_claims_with_abac"):
            spark.sql(
                f"ALTER TABLE {fq(tbl)} SET ROW FILTER {fq_fn} ON (patient_id)"
            )
        print(
            f"ABAC policy conditions not available for tag keys in this metastore; "
            f"applied inline row filter {catalog}.{schema}.check_patient_access "
            f"on patient_demographics_with_abac and patient_claims_with_abac."
        )
    else:
        print(
            f"Created ABAC row filter policy patient_crosswalk_abac on schema {catalog}.{schema} "
            f"using UDF {catalog}.{schema}.check_patient_access"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Seed patient demographics and claims Delta tables.")
    parser.add_argument(
        "--catalog",
        required=True,
        help="Unity Catalog name (must exist). Tables are written to {catalog}.{schema}.*",
    )
    parser.add_argument(
        "--schema",
        default="demo_dynamic_abac",
        help="Schema name within the catalog (default: demo_dynamic_abac).",
    )
    args = parser.parse_args(argv)

    catalog = args.catalog.strip()
    schema = args.schema.strip()
    if not catalog or not schema:
        print("catalog and schema must be non-empty", file=sys.stderr)
        return 1

    data_dir = _bootstrap_dir()
    demo_csv = data_dir / "patient_demographics.csv"
    claims_csv = data_dir / "patient_claims.csv"
    crosswalk_csv = data_dir / "staff_patient_crosswalk.csv"
    if not demo_csv.is_file() or not claims_csv.is_file() or not crosswalk_csv.is_file():
        print(f"Missing CSV fixtures under {data_dir}", file=sys.stderr)
        return 1

    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

    demographics = _read_fixture_csv(spark, demo_csv)
    claims = _read_fixture_csv(spark, claims_csv)
    crosswalk = _read_fixture_csv(spark, crosswalk_csv)
    repo_git_email = _git_user_email_near(__file__)
    spark_principal = spark.sql("SELECT current_user() AS u").collect()[0]["u"]
    s003_email = repo_git_email or spark_principal
    crosswalk = crosswalk.withColumn(
        "staff_email",
        F.when(F.upper(F.trim(F.col("staff_id"))) == "S003", F.lit(s003_email)).otherwise(F.col("staff_email")),
    )

    demographics.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{catalog}.{schema}.patient_demographics"
    )
    claims.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{catalog}.{schema}.patient_claims"
    )
    crosswalk.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{catalog}.{schema}.staff_patient_crosswalk"
    )

    spark.sql(
        f"CREATE OR REPLACE TABLE `{catalog}`.`{schema}`.`patient_demographics_with_abac` "
        f"AS SELECT * FROM `{catalog}`.`{schema}`.`patient_demographics`"
    )
    spark.sql(
        f"CREATE OR REPLACE TABLE `{catalog}`.`{schema}`.`patient_claims_with_abac` "
        f"AS SELECT * FROM `{catalog}`.`{schema}`.`patient_claims`"
    )

    _apply_abac_row_filters(spark, catalog, schema)

    n_demo = spark.table(f"{catalog}.{schema}.patient_demographics").count()
    n_claims = spark.table(f"{catalog}.{schema}.patient_claims").count()
    n_demo_abac = spark.table(f"{catalog}.{schema}.patient_demographics_with_abac").count()
    n_claims_abac = spark.table(f"{catalog}.{schema}.patient_claims_with_abac").count()
    n_crosswalk = spark.table(f"{catalog}.{schema}.staff_patient_crosswalk").count()
    print(
        f"Seeded {catalog}.{schema}: "
        f"patient_demographics={n_demo}, patient_claims={n_claims}, "
        f"staff_patient_crosswalk={n_crosswalk}, "
        f"patient_demographics_with_abac={n_demo_abac}, patient_claims_with_abac={n_claims_abac}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
