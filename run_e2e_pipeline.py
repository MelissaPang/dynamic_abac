#!/usr/bin/env python3
"""
Run the dynamic_abac Databricks pipeline end-to-end from your laptop:

1. ``databricks bundle deploy`` — builds the wheel, syncs resources, applies bundle variables.
2. ``databricks bundle run seed_patient_tables`` — seeds Delta tables + crosswalk, applies UC row
   filters, then runs ``notebooks/verify_abac_policies.ipynb`` as the second task.

Requires the Databricks CLI (``databricks``) on PATH and auth for the workspace in ``databricks.yml``
(``databricks auth login`` or a ``~/.databrickscfg`` profile).

Examples::

    python run_e2e_pipeline.py --catalog melissap --profile melissapang
    python run_e2e_pipeline.py --catalog main --schema demo_dynamic_abac --skip-deploy
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
        print(f"Expected databricks.yml next to this script; cwd logic wrong: {root}", file=sys.stderr)
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
        help="Bundle target name from databricks.yml (default: dev).",
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="Optional ~/.databrickscfg profile (e.g. when multiple hosts match).",
    )
    parser.add_argument(
        "--skip-deploy",
        action="store_true",
        help="Only run the job; skip bundle deploy (use after a successful deploy with same vars).",
    )
    args = parser.parse_args()

    root = _repo_root()
    dbx = _databricks_prefix(args.profile)
    var_catalog = f"catalog={args.catalog}"
    var_schema = f"schema={args.schema}"

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
    ]
    _run(run_job, cwd=root)

    print("\nPipeline completed successfully.", flush=True)


if __name__ == "__main__":
    main()
