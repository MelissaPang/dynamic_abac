#!/usr/bin/env python3
"""
End-to-end setup: deploy the Databricks Asset Bundle, then run the ``seed_patient_tables`` job.

That job (1) seeds Delta tables and crosswalk, attaches per-table ABAC row-filter policies
(UDF + governed tags: ``abac_tag_key`` / ``abac_tag_value`` on tables, ``abac_tag_key2`` / optional ``abac_tag_key2_value`` on ``patient_id``) on ``*_with_abac`` tables, and
(2) runs ``notebooks/verify_abac_policies.ipynb`` to validate visibility.

Requires the ``databricks`` CLI on PATH and workspace auth (see README).

Examples::

    python set_up.py --catalog melissap --profile melissapang
    python set_up.py --catalog main --schema demo_dynamic_abac --skip-deploy
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


def _repo_root() -> Path:
    root = Path(__file__).resolve().parent
    if not (root / "databricks.yml").is_file():
        print(f"Expected databricks.yml next to this script: {root}", file=sys.stderr)
        sys.exit(1)
    return root


def _run(cmd: list[str], *, cwd: Path, extra_env: dict[str, str] | None = None) -> None:
    printable = " ".join(cmd)
    print(f"\n$ {printable}\n", flush=True)
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    proc = subprocess.run(cmd, cwd=cwd, env=env)
    if proc.returncode != 0:
        sys.exit(proc.returncode)


def _databricks_prefix(profile: str | None) -> list[str]:
    cmd = ["databricks"]
    if profile:
        cmd.extend(["--profile", profile])
    return cmd


def _bundle_var_scalar(name: str, value: str) -> str:
    """Bundle ``--var`` must not be empty/null for job wheel parameters (Terraform rejects nil). Whitespace-only → single space; ``seed_tables`` strips that to key-only."""
    return f"{name}={value if value.strip() else ' '}"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--catalog",
        default="main",
        help="Unity Catalog name passed to the bundle (default: main).",
    )
    parser.add_argument(
        "--schema",
        default="demo_dynamic_abac",
        help="Schema name passed to the bundle (default: demo_dynamic_abac).",
    )
    parser.add_argument(
        "--target",
        default="dev",
        help="Bundle target from databricks.yml (default: dev).",
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="Optional ~/.databrickscfg profile when multiple workspaces match.",
    )
    parser.add_argument(
        "--skip-deploy",
        action="store_true",
        help="Only run the job; skip bundle deploy (after a successful deploy with same vars).",
    )
    parser.add_argument(
        "--abac-tag-key",
        default="abac",
        help="Bundle/job: governed tag key on *_with_abac tables (default matches databricks.yml ``abac_tag_key``).",
    )
    parser.add_argument(
        "--abac-tag-value",
        default="true",
        help="Bundle/job: tag value for table tag key (default ``true``; empty for key-only). Matches ``abac_tag_value``.",
    )
    parser.add_argument(
        "--abac-tag-key2",
        default="abac_pii_types",
        help="Bundle/job: governed tag key on patient_id (default matches databricks.yml ``abac_tag_key2``).",
    )
    parser.add_argument(
        "--abac-tag-key2-value",
        default="ssn",
        help="Bundle/job: column tag value (governed enum for your tag; default ``ssn``). Empty = key-only if UC allows. Matches ``abac_tag_key2_value``.",
    )
    args = parser.parse_args()

    root = _repo_root()
    dbx = _databricks_prefix(args.profile)
    var_catalog = f"catalog={args.catalog}"
    var_schema = f"schema={args.schema}"
    var_abac_key = f"abac_tag_key={args.abac_tag_key}"
    var_abac_val = _bundle_var_scalar("abac_tag_value", args.abac_tag_value)
    var_abac_key2 = f"abac_tag_key2={args.abac_tag_key2}"
    var_abac_key2_val = _bundle_var_scalar("abac_tag_key2_value", args.abac_tag_key2_value)

    if not args.skip_deploy:
        deploy = [
            *dbx,
            "bundle",
            "deploy",
            "-t",
            args.target,
            "--var",
            var_catalog,
            "--var",
            var_schema,
            "--var",
            var_abac_key,
            "--var",
            var_abac_val,
            "--var",
            var_abac_key2,
            "--var",
            var_abac_key2_val,
        ]
        _run(deploy, cwd=root)

    run_job = [
        *dbx,
        "bundle",
        "run",
        "seed_patient_tables",
        "-t",
        args.target,
        "--var",
        var_catalog,
        "--var",
        var_schema,
        "--var",
        var_abac_key,
        "--var",
        var_abac_val,
        "--var",
        var_abac_key2,
        "--var",
        var_abac_key2_val,
    ]
    _run(run_job, cwd=root)

    print("\nSetup pipeline completed successfully.", flush=True)


if __name__ == "__main__":
    main()
