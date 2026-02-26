#!/usr/bin/env python3
"""Demonstrate CRUD endpoints for the macro/labor API.

Flow:
1) GET a record by date
2) Save it locally
3) Modify one numeric field and PUT it back
4) GET and display the edited record
5) DELETE the edited record
6) POST the original record to restore it

Requires the API to be running.
"""
from __future__ import annotations

import json
import os
import sys
import urllib.request
from typing import Any, Dict, Tuple

BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:8000")
SAMPLE_DATE = os.getenv("SAMPLE_DATE", "2020-04-01")


class ApiError(RuntimeError):
    pass


def request_json(method: str, path: str, body: Dict[str, Any] | None = None) -> Tuple[int, Dict[str, Any]]:
    url = f"{BASE_URL}{path}"
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            payload = resp.read().decode("utf-8")
            return resp.status, json.loads(payload) if payload else {}
    except urllib.error.HTTPError as e:
        payload = e.read().decode("utf-8")
        raise ApiError(f"{method} {url} -> {e.code}: {payload}") from e


def tweak_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    """Return a shallow-modified copy of record with a small numeric change."""
    new_rec = json.loads(json.dumps(rec))

    # Prefer a FRED field if available
    fred = new_rec.get("fred", {})
    if isinstance(fred, dict):
        for key in ["unemployment_rate", "cpi_all_items", "fed_funds_rate", "yield_spread_10y_2y"]:
            if key in fred and isinstance(fred[key], (int, float)):
                fred[key] = float(fred[key]) + 0.01
                return new_rec

    # Fall back to a BLS field
    bls = new_rec.get("bls", {})
    if isinstance(bls, dict):
        for key in ["unemployment_rate_bls", "total_nonfarm_payrolls", "labor_force_participation_rate"]:
            if key in bls and isinstance(bls[key], (int, float)):
                bls[key] = float(bls[key]) + 0.01
                return new_rec

    # Fall back to census
    census = new_rec.get("census", {})
    if isinstance(census, dict):
        for key in ["total_population", "median_household_income"]:
            if key in census and isinstance(census[key], (int, float)):
                census[key] = float(census[key]) + 1.0
                return new_rec

    raise ApiError("No numeric field found to modify")


def main() -> int:
    print(f"API base: {BASE_URL}")
    print(f"Sample date: {SAMPLE_DATE}")

    # 1) GET
    status, record = request_json("GET", f"/records/{SAMPLE_DATE}")
    print(f"GET {SAMPLE_DATE}: status={status}")

    original = record

    # 2) Modify + PUT
    edited = tweak_record(original)
    status, updated = request_json("PUT", f"/records/{SAMPLE_DATE}", edited)
    print(f"PUT {SAMPLE_DATE}: status={status}")

    # 3) GET to show edit
    status, check = request_json("GET", f"/records/{SAMPLE_DATE}")
    print(f"GET edited {SAMPLE_DATE}: status={status}")
    print(json.dumps(check, indent=2)[:1200])

    # 4) DELETE edited
    status, deleted = request_json("DELETE", f"/records/{SAMPLE_DATE}")
    print(f"DELETE {SAMPLE_DATE}: status={status}, response={deleted}")

    # 5) POST original to restore
    status, restored = request_json("POST", "/records", original)
    print(f"POST restore {SAMPLE_DATE}: status={status}")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ApiError as e:
        print(f"API error: {e}", file=sys.stderr)
        raise SystemExit(1)
