"""Create UC schema and overwrite Delta tables from bundled CSV fixtures."""

from __future__ import annotations

import argparse
import csv
import re
import subprocess
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

_TAG_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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


def _assert_tag_identifier(name: str, what: str) -> str:
    s = name.strip()
    if not s or not _TAG_IDENT.match(s):
        raise ValueError(f"{what} must be a non-empty Unity Catalog identifier (letters, digits, underscore): {name!r}")
    return s


def _sql_string_literal(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _tag_value_sql_for_set_tag(tag_value: str) -> str:
    """UC ``SET TAG`` value: booleans as keywords; governed enums as bare identifiers (``ssn``); else a string literal."""
    s = tag_value.strip()
    low = s.lower()
    if low in ("true", "false"):
        return low
    if _TAG_IDENT.match(s):
        return s
    return _sql_string_literal(s)


def _tag_value_sql_for_policy(tag_value: str) -> str:
    """Value argument for ``has_tag_value`` in row policies (string form; bare names are not column refs)."""
    return _sql_string_literal(tag_value.strip())


def _set_tag_on_entity_clause(tag_key: str, tag_value: str) -> str:
    """Fragment for ``SET TAG ON …``: ``tag_key`` (key-only) or ``tag_key = value`` (UC requires ``=``)."""
    s = tag_value.strip()
    if not s:
        return tag_key
    return f"{tag_key} = {_tag_value_sql_for_set_tag(s)}"


def _match_columns_for_column_tag(column_tag_key: str, column_tag_value: str) -> str:
    """Policy binds UDF to ``patient_id`` via column tag: ``has_tag`` (key-only) or ``has_tag_value``."""
    k = _sql_string_literal(column_tag_key)
    s = column_tag_value.strip()
    if not s:
        return f"has_tag({k}) AS pid"
    v = _tag_value_sql_for_policy(s)
    return f"has_tag_value({k}, {v}) AS pid"


def _set_tag_sql_allow_duplicate(spark: SparkSession, sql: str) -> None:
    """Run SET TAG …; UC raises if the same tag key is already assigned (idempotent re-seed)."""
    try:
        spark.sql(sql)
    except Exception as e:
        if "UC_DUPLICATE_TAG_ASSIGNMENT" in str(e):
            return
        raise


def _drop_policy_if_exists(spark: SparkSession, policy_name: str, on_clause: str) -> None:
    """``on_clause`` is e.g. ``ON SCHEMA `cat`.`sch` `` or ``ON TABLE `cat`.`sch`.`t` ``."""
    try:
        spark.sql(f"DROP POLICY {policy_name} {on_clause}")
    except Exception:
        pass


def _apply_abac_row_filters(
    spark: SparkSession,
    catalog: str,
    schema: str,
    *,
    table_tag_key: str,
    table_tag_value: str,
    column_tag_key: str,
    column_tag_value: str,
) -> None:
    """Attach per-table ABAC row-filter policies (no ``ALTER TABLE … SET ROW FILTER``).

    ``table_tag_key`` + optional ``table_tag_value`` (default ``true``) on each ``*_with_abac``
    table; ``column_tag_key`` + optional ``column_tag_value`` on ``patient_id`` (empty =
    key-only). Policy ``MATCH COLUMNS`` uses ``has_tag`` or ``has_tag_value`` on the column tag
    so the row-filter UDF receives ``patient_id``.

    Principals: ``All account users`` then ``account users``. See ``sql/abac_row_filter.sql``.
    """
    tkey = _assert_tag_identifier(table_tag_key, "abac_tag_key (table)")
    ckey = _assert_tag_identifier(column_tag_key, "abac_tag_key2 (column)")
    table_clause = _set_tag_on_entity_clause(tkey, table_tag_value)
    column_clause = _set_tag_on_entity_clause(ckey, column_tag_value)

    fq = lambda name: f"`{catalog}`.`{schema}`.`{name}`"
    fq_schema = f"`{catalog}`.`{schema}`"
    fq_fn = f"`{catalog}`.`{schema}`.`check_patient_access`"
    match_sql = _match_columns_for_column_tag(ckey, column_tag_value)

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

    _drop_policy_if_exists(spark, "patient_crosswalk_abac", f"ON SCHEMA {fq_schema}")

    abac_tables = ("patient_demographics_with_abac", "patient_claims_with_abac")
    principal_candidates = ("`All account users`", "`account users`")

    for tbl in abac_tables:
        try:
            spark.sql(f"ALTER TABLE {fq(tbl)} DROP ROW FILTER")
        except Exception:
            pass
        _drop_policy_if_exists(spark, "dynamic_abac_row", f"ON TABLE {fq(tbl)}")

        _set_tag_sql_allow_duplicate(spark, f"SET TAG ON TABLE {fq(tbl)} {table_clause}")
        _set_tag_sql_allow_duplicate(
            spark, f"SET TAG ON COLUMN {fq(tbl)}.patient_id {column_clause}"
        )

    principal_sql: str | None = None

    for tbl in abac_tables:
        to_try = principal_candidates if principal_sql is None else (principal_sql,)
        last_err: BaseException | None = None
        for p in to_try:
            try:
                spark.sql(
                    f"""
                    CREATE OR REPLACE POLICY dynamic_abac_row
                    ON TABLE {fq(tbl)}
                    COMMENT 'ABAC row filter: check_patient_access(patient_id) for crosswalk-driven visibility'
                    ROW FILTER {fq_fn}
                    TO {p}
                    FOR TABLES
                    MATCH COLUMNS {match_sql}
                    USING COLUMNS (pid)
                    """
                )
                principal_sql = p
                break
            except Exception as e:
                last_err = e
                if "PRINCIPAL_DOES_NOT_EXIST" in str(e):
                    continue
                raise
        else:
            if last_err is not None:
                raise last_err
            raise RuntimeError("CREATE POLICY failed with no captured error")

    used = principal_sql.strip("`") if principal_sql else "unknown"
    tdesc = f"`{tkey}`" if not table_tag_value.strip() else f"`{tkey}`={_tag_value_sql_for_set_tag(table_tag_value)}"
    cdesc = f"`{ckey}`" if not column_tag_value.strip() else f"`{ckey}`={_tag_value_sql_for_set_tag(column_tag_value)}"
    print(
        f"Attached ABAC row-filter policy dynamic_abac_row on each *_with_abac table in "
        f"{catalog}.{schema} (table tag {tdesc}, column tag {cdesc}; TO `{used}`; "
        f"UDF {catalog}.{schema}.check_patient_access)"
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
    parser.add_argument(
        "--abac-tag-key",
        default="abac",
        help="Governed tag key on *_with_abac **tables** (same default as bundle ``abac_tag_key``).",
    )
    parser.add_argument(
        "--abac-tag-value",
        default="true",
        help="Tag value for ``--abac-tag-key`` on tables (SQL string literal). Use empty string for key-only ``SET TAG ON TABLE``.",
    )
    parser.add_argument(
        "--abac-tag-key2",
        default="abac_pii_types",
        help="Governed tag key on **patient_id** (same default as bundle ``abac_tag_key2``).",
    )
    parser.add_argument(
        "--abac-tag-key2-value",
        default="ssn",
        help="Tag value for ``--abac-tag-key2`` on ``patient_id`` (governed enum). Empty = key-only + ``has_tag`` in policy if UC allows. Default ``ssn`` for typical ``abac_pii_types``.",
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

    _apply_abac_row_filters(
        spark,
        catalog,
        schema,
        table_tag_key=args.abac_tag_key,
        table_tag_value=args.abac_tag_value,
        column_tag_key=args.abac_tag_key2,
        column_tag_value=args.abac_tag_key2_value,
    )

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
