"""Import macro/labor JSON data into local MongoDB.

Defaults:
- host: localhost:27017
- db: HW3
- collection: macro_labor
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

# .env loading
try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None


PRIMARY_PATH = Path("data/macro_labor_us_monthly_1985_present.json")
ALT_PATH = Path("scripts/data/macro_labor_us_monthly_1985_present.json")


def parse_args() -> argparse.Namespace:
    if load_dotenv is not None:
        load_dotenv()
    parser = argparse.ArgumentParser(description="Import macro/labor JSON data into MongoDB.")
    parser.add_argument(
        "--host",
        default=os.getenv("MONGO_HOST"),
        help="Mongo host (env: MONGO_HOST)",
    )
    parser.add_argument(
        "--db",
        default=os.getenv("MONGO_DB"),
        help="Database name (env: MONGO_DB)",
    )
    parser.add_argument(
        "--collection",
        default=os.getenv("MONGO_COLLECTION"),
        help="Collection name (env: MONGO_COLLECTION)",
    )
    parser.add_argument(
        "--file",
        default=None,
        help="Path to JSON file (default: data/macro_labor_us_monthly_1985_present.json)",
    )
    parser.add_argument(
        "--drop",
        action="store_true",
        help="Drop the collection before import (default: false).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.host or not args.db or not args.collection:
        print("Missing MONGO_HOST, MONGO_DB, or MONGO_COLLECTION in environment.", file=sys.stderr)
        return 1
    data_path = PRIMARY_PATH if PRIMARY_PATH.exists() else ALT_PATH
    if args.file:
        data_path = Path(args.file)
    if not data_path.exists():
        print(f"Could not find data file at {PRIMARY_PATH} or {ALT_PATH}", file=sys.stderr)
        return 1

    mongoimport = "mongoimport"
    if not shutil_which(mongoimport):
        print("mongoimport not found in PATH. Install MongoDB Database Tools.", file=sys.stderr)
        return 1

    cmd = [
        mongoimport,
        "--host",
        args.host,
        "--db",
        args.db,
        "--collection",
        args.collection,
        "--file",
        str(data_path),
        "--jsonArray",
    ]
    if args.drop:
        cmd.append("--drop")

    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        return result.returncode

    print(f"Import complete: {args.db}.{args.collection} from {data_path}")
    return 0


def shutil_which(cmd: str) -> str | None:
    for path in os.environ.get("PATH", "").split(os.pathsep):
        candidate = Path(path) / cmd
        if candidate.exists() and os.access(candidate, os.X_OK):
            return str(candidate)
    return None


if __name__ == "__main__":
    raise SystemExit(main())
