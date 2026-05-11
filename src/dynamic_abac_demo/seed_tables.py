"""Create UC schema and overwrite Delta tables from bundled CSV fixtures."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession


def _bootstrap_dir() -> Path:
    return Path(__file__).resolve().parent / "bootstrap_data"


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
    if not demo_csv.is_file() or not claims_csv.is_file():
        print(f"Missing CSV fixtures under {data_dir}", file=sys.stderr)
        return 1

    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

    demographics = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(str(demo_csv))
    )
    claims = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(str(claims_csv))
    )

    full_demo = f"`{catalog}`.`{schema}`.`patient_demographics`"
    full_claims = f"`{catalog}`.`{schema}`.`patient_claims`"

    demographics.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{catalog}.{schema}.patient_demographics"
    )
    claims.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{catalog}.{schema}.patient_claims"
    )

    spark.sql(f"REFRESH TABLE {full_demo}")
    spark.sql(f"REFRESH TABLE {full_claims}")

    spark.sql(
        f"CREATE OR REPLACE TABLE `{catalog}`.`{schema}`.`patient_demographics_with_abac` "
        f"AS SELECT * FROM `{catalog}`.`{schema}`.`patient_demographics`"
    )
    spark.sql(
        f"CREATE OR REPLACE TABLE `{catalog}`.`{schema}`.`patient_claims_with_abac` "
        f"AS SELECT * FROM `{catalog}`.`{schema}`.`patient_claims`"
    )

    full_demo_abac = f"`{catalog}`.`{schema}`.`patient_demographics_with_abac`"
    full_claims_abac = f"`{catalog}`.`{schema}`.`patient_claims_with_abac`"
    spark.sql(f"REFRESH TABLE {full_demo_abac}")
    spark.sql(f"REFRESH TABLE {full_claims_abac}")

    n_demo = spark.table(f"{catalog}.{schema}.patient_demographics").count()
    n_claims = spark.table(f"{catalog}.{schema}.patient_claims").count()
    n_demo_abac = spark.table(f"{catalog}.{schema}.patient_demographics_with_abac").count()
    n_claims_abac = spark.table(f"{catalog}.{schema}.patient_claims_with_abac").count()
    print(
        f"Seeded {catalog}.{schema}: "
        f"patient_demographics={n_demo}, patient_claims={n_claims}, "
        f"patient_demographics_with_abac={n_demo_abac}, patient_claims_with_abac={n_claims_abac}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
