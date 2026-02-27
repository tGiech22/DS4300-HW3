"""
Build a monthly US macro + labor dataset (1985-present) using:
- FRED API (macro series)
- BLS API (labor series)
- Census API (ACS1 national population + median household income - annual, forward-filled monthly)

Outputs: data/macro_labor_us_monthly_1985_present.json
"""
from __future__ import annotations

import json
import os
import re
import sys
import urllib.parse
import urllib.request
from datetime import date

# .env loading
try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

# ---------- Config ----------
START_DATE = date(1985, 1, 1)
END_DATE = date.today()

FRED_SERIES = [
    {"id": "UNRATE", "key": "unemployment_rate", "name": "Unemployment Rate"},
    {"id": "CPIAUCSL", "key": "cpi_all_items", "name": "Consumer Price Index for All Urban Consumers: All Items"},
    {"id": "FEDFUNDS", "key": "fed_funds_rate", "name": "Effective Federal Funds Rate"},
    {"id": "T10Y2Y", "key": "yield_spread_10y_2y", "name": "10-Year Treasury Constant Maturity Minus 2-Year"},
]

BLS_SERIES = [
    {"id": "LNS14000000", "key": "unemployment_rate_bls", "name": "Unemployment Rate"},
    {"id": "CES0000000001", "key": "total_nonfarm_payrolls", "name": "All Employees, Total Nonfarm"},
    {"id": "LNS11300000", "key": "labor_force_participation_rate", "name": "Labor Force Participation Rate"},
]

CENSUS_VARS = [
    {"id": "B01001_001E", "key": "total_population", "name": "Total Population"},
    {"id": "B19013_001E", "key": "median_household_income", "name": "Median Household Income"},
]

OUTPUT_PATH = "data/macro_labor_us_monthly_1985_present.json"
SERIES_DEFS_PATH = "data/series_definitions.json"

# ---------- Helpers ----------

def month_range(start: date, end: date):
    """Yield first day of each month between start and end inclusive."""
    y, m = start.year, start.month
    while (y < end.year) or (y == end.year and m <= end.month):
        yield date(y, m, 1)
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1


def http_get_json(url: str, params: dict | None = None) -> dict:
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(url) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        safe_url = url
        try:
            parts = urllib.parse.urlsplit(url)
            q = urllib.parse.parse_qs(parts.query, keep_blank_values=True)
            if "api_key" in q:
                q["api_key"] = ["***REDACTED***"]
            safe_query = urllib.parse.urlencode(q, doseq=True)
            safe_url = urllib.parse.urlunsplit(
                (parts.scheme, parts.netloc, parts.path, safe_query, parts.fragment)
            )
        except Exception:
            safe_url = url
        body = ""
        try:
            body = e.read().decode("utf-8")
        except Exception:
            body = "<unable to read body>"
        raise RuntimeError(f"HTTPError {e.code} for URL: {safe_url}\nResponse: {body}") from e


def http_post_json(url: str, payload: dict) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8"))


def parse_float(val: str | None) -> float | None:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    val = val.strip()
    if val in ("", "."):
        return None
    try:
        return float(val)
    except ValueError:
        return None


# ---------- Fetchers ----------

def fetch_fred_series(series_id: str, api_key: str, start: date, end: date) -> dict[str, float | None]:
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start.isoformat(),
        "observation_end": end.isoformat(),
        "frequency": "m",
        "aggregation_method": "avg",
    }
    data = http_get_json(url, params)
    out: dict[str, float | None] = {}
    for obs in data.get("observations", []):
        obs_date = obs.get("date")
        value = parse_float(obs.get("value"))
        if obs_date:
            out[obs_date] = value
    return out


def fetch_bls_series(series_ids: list[str], api_key: str | None, start_year: int, end_year: int) -> dict[str, dict[str, float | None]]:
    url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    payload = {
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
    }
    if api_key:
        payload["registrationkey"] = api_key

    data = http_post_json(url, payload)
    if data.get("status") != "REQUEST_SUCCEEDED":
        raise RuntimeError(f"BLS request failed: {data.get('message')}")

    series_map: dict[str, dict[str, float | None]] = {sid: {} for sid in series_ids}
    for series in data.get("Results", {}).get("series", []):
        sid = series.get("seriesID")
        if not sid:
            continue
        for item in series.get("data", []):
            period = item.get("period")
            if not (period and period.startswith("M") and period != "M13"):
                continue
            year = item.get("year")
            value = parse_float(item.get("value"))
            if year:
                month = period[1:]
                obs_date = f"{year}-{month}-01"
                series_map[sid][obs_date] = value
    return series_map


def fetch_census_acs1_us(api_key: str, series_ids: list[str], start_year: int, end_year: int) -> dict[int, dict[str, float | None]]:
    out: dict[int, dict[str, float | None]] = {}
    for year in range(start_year, end_year + 1):
        url = f"https://api.census.gov/data/{year}/acs/acs1"
        params = {
            "get": "NAME," + ",".join(series_ids),
            "for": "us:1",
            "key": api_key,
        }
        try:
            data = http_get_json(url, params)
        except Exception:
            # ACS1 is not available for all years; stop when we hit a non-existent year
            break
        if not data or len(data) < 2:
            continue
        header = data[0]
        row = data[1]
        values = dict(zip(header, row))
        out[year] = {k: parse_float(values.get(k)) for k in series_ids}
    return out


def weighted_median(values: list[float], weights: list[float]) -> float | None:
    if not values or not weights:
        return None
    pairs = sorted(zip(values, weights), key=lambda item: item[0])
    total = sum(w for _, w in pairs)
    if total <= 0:
        return None
    cutoff = total / 2.0
    running = 0.0
    for value, weight in pairs:
        running += weight
        if running >= cutoff:
            return value
    return pairs[-1][0]


def fetch_cps_asec_median_household_income(
    api_key: str, start_year: int, end_year: int
) -> dict[int, float | None]:
    out: dict[int, float | None] = {}
    for year in range(start_year, end_year + 1):
        url = f"https://api.census.gov/data/{year}/cps/asec/mar"
        params = {
            "get": "HTOTVAL,HSUP_WGT,A_LINENO",
            "A_LINENO": "1",
            "key": api_key,
        }
        try:
            data = http_get_json(url, params)
        except Exception:
            continue
        if not data or len(data) < 2:
            continue
        header = data[0]
        idx = {name: i for i, name in enumerate(header)}
        values: list[float] = []
        weights: list[float] = []
        for row in data[1:]:
            value = parse_float(row[idx["HTOTVAL"]]) if "HTOTVAL" in idx else None
            weight = parse_float(row[idx["HSUP_WGT"]]) if "HSUP_WGT" in idx else None
            if value is None or weight is None or weight <= 0:
                continue
            values.append(value)
            weights.append(weight)
        out[year] = weighted_median(values, weights)
    return out


def extract_year_from_text(text: str) -> int | None:
    match = re.search(r"(19|20)\\d{2}", text)
    if not match:
        return None
    return int(match.group(0))


def fetch_pep_us_population(api_key: str, start_year: int, end_year: int) -> dict[int, float | None]:
    out: dict[int, float | None] = {}

    # 1990-2000 intercensal: sum across ages for July 1 each year.
    if end_year >= 1990 and start_year <= 2000:
        url = "https://api.census.gov/data/1990/pep/int_natrespop"
        for month in ("7", "07"):
            params = {
                "get": "YEAR,MONTH,TOT_POP,AGE",
                "MONTH": month,
                "key": api_key,
            }
            try:
                data = http_get_json(url, params)
            except Exception:
                data = []
            if data and len(data) > 1:
                header = data[0]
                idx = {name: i for i, name in enumerate(header)}
                totals: dict[int, float] = {}
                for row in data[1:]:
                    year = row[idx["YEAR"]]
                    total = parse_float(row[idx["TOT_POP"]]) if "TOT_POP" in idx else None
                    if not year or total is None:
                        continue
                    y = int(year)
                    if y < start_year or y > end_year:
                        continue
                    totals[y] = totals.get(y, 0.0) + total
                out.update({y: totals.get(y) for y in totals})
                break

    # 2000-2010 intercensal: annual July 1 estimates.
    if end_year >= 2000 and start_year <= 2010:
        url = "https://api.census.gov/data/2000/pep/int_population"
        params = {
            "get": "DATE_DESC,POP",
            "for": "us:1",
            "key": api_key,
        }
        try:
            data = http_get_json(url, params)
        except Exception:
            data = []
        if data and len(data) > 1:
            header = data[0]
            idx = {name: i for i, name in enumerate(header)}
            for row in data[1:]:
                desc = row[idx["DATE_DESC"]] if "DATE_DESC" in idx else ""
                if "July 1" not in desc:
                    continue
                year = extract_year_from_text(desc)
                pop = parse_float(row[idx["POP"]]) if "POP" in idx else None
                if year is None or pop is None:
                    continue
                if start_year <= year <= end_year:
                    out[year] = pop

    # 2010-2019 postcensal (vintage 2019): annual July 1 estimates.
    if end_year >= 2010 and start_year <= 2019:
        url = "https://api.census.gov/data/2019/pep/population"
        params = {
            "get": "DATE_DESC,POP",
            "for": "us:1",
            "key": api_key,
        }
        try:
            data = http_get_json(url, params)
        except Exception:
            data = []
        if data and len(data) > 1:
            header = data[0]
            idx = {name: i for i, name in enumerate(header)}
            for row in data[1:]:
                desc = row[idx["DATE_DESC"]] if "DATE_DESC" in idx else ""
                if "July 1" not in desc:
                    continue
                year = extract_year_from_text(desc)
                pop = parse_float(row[idx["POP"]]) if "POP" in idx else None
                if year is None or pop is None:
                    continue
                if start_year <= year <= end_year:
                    out[year] = pop

    # 2020-2022 postcensal monthly (vintage 2021): use July 1.
    if end_year >= 2020 and start_year <= 2022:
        url = "https://api.census.gov/data/2021/pep/natmonthly"
        params = {
            "get": "MONTHLY_DESC,POP",
            "for": "us:1",
            "key": api_key,
        }
        try:
            data = http_get_json(url, params)
        except Exception:
            data = []
        if data and len(data) > 1:
            header = data[0]
            idx = {name: i for i, name in enumerate(header)}
            for row in data[1:]:
                desc = row[idx["MONTHLY_DESC"]] if "MONTHLY_DESC" in idx else ""
                if "July 1" not in desc:
                    continue
                year = extract_year_from_text(desc)
                pop = parse_float(row[idx["POP"]]) if "POP" in idx else None
                if year is None or pop is None:
                    continue
                if start_year <= year <= end_year:
                    out[year] = pop

    return out


# ---------- Build ----------

def main() -> int:
    if load_dotenv is not None:
        load_dotenv()

    fred_key = os.getenv("FRED_API_KEY")
    bls_key = os.getenv("BLS_API_KEY")
    census_key = os.getenv("CENSUS_API_KEY")

    if not fred_key or not census_key:
        print("Missing API keys. Please set FRED_API_KEY and CENSUS_API_KEY.")
        print("BLS_API_KEY is optional but recommended.")
        return 1

    # Initialize monthly index
    months = [d.isoformat() for d in month_range(START_DATE, END_DATE)]

    # Fetch FRED series
    fred_data: dict[str, dict[str, float | None]] = {}
    for s in FRED_SERIES:
        fred_data[s["id"]] = fetch_fred_series(s["id"], fred_key, START_DATE, END_DATE)

    # Fetch BLS series in 20-year chunks
    bls_data: dict[str, dict[str, float | None]] = {s["id"]: {} for s in BLS_SERIES}
    year = START_DATE.year
    while year <= END_DATE.year:
        chunk_start = year
        chunk_end = min(year + 19, END_DATE.year)
        chunk = fetch_bls_series([s["id"] for s in BLS_SERIES], bls_key, chunk_start, chunk_end)
        for sid, series in chunk.items():
            bls_data[sid].update(series)
        year = chunk_end + 1

    acs_data = fetch_census_acs1_us(census_key, [s["id"] for s in CENSUS_VARS], 2005, END_DATE.year)
    cps_income = fetch_cps_asec_median_household_income(
        census_key, 1992, min(2004, END_DATE.year)
    )
    pep_population = fetch_pep_us_population(census_key, START_DATE.year, END_DATE.year)

    # Assemble document
    docs = []
    for m in months:
        y = int(m[:4])
        doc = {
            "date": m,
            "fred": {s["key"]: fred_data[s["id"]].get(m) for s in FRED_SERIES},
            "bls": {s["key"]: bls_data[s["id"]].get(m) for s in BLS_SERIES},
            "census": {
                "total_population": (
                    pep_population.get(y)
                    if pep_population.get(y) is not None
                    else acs_data.get(y, {}).get("B01001_001E")
                ),
                "median_household_income": (
                    acs_data.get(y, {}).get("B19013_001E")
                    if y >= 2005
                    else cps_income.get(y)
                ),
            },
        }
        docs.append(doc)

    # Write output
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(docs, f, indent=2)

    series_defs = {
        "fred": {s["id"]: {"key": s["key"], "name": s["name"]} for s in FRED_SERIES},
        "bls": {s["id"]: {"key": s["key"], "name": s["name"]} for s in BLS_SERIES},
        "census": {s["id"]: {"key": s["key"], "name": s["name"]} for s in CENSUS_VARS},
    }
    with open(SERIES_DEFS_PATH, "w", encoding="utf-8") as f:
        json.dump(series_defs, f, indent=2)

    print(f"Wrote {len(docs)} records to {OUTPUT_PATH}")
    print(f"Wrote series definitions to {SERIES_DEFS_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
