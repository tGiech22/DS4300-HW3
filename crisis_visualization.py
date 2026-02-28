"""
Course: DS4300 with Professor Rachlin
Assignment: Homework 3 
Written By: Erika Sohn

Objective:
    Pulls data from MongoDB Atlas cloud via analysis_api.py and creates
    a three-panel time series chart showing the relationship
    between yield curve inversions, unemployment, and Fed policy
    from 1985 to present. Prints key findings as well. 
"""

import os
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from pymongo import MongoClient
from dotenv import load_dotenv

# import functions from analysis_api
from analysis_api import ( 
    get_high_unemployment_months,
    get_avg_unemployment_by_decade,
    get_yield_curve_inversions,
    get_monthly_snapshot
)

load_dotenv()

# --- Step 1: Pull time-series from atlas --- # 
client = MongoClient(os.getenv("MONGO_HOST"))
db = client["HW3"]
collection = db["macro_labor"]

docs = list(collection.find(
    {},
    {
        "_id": 0,
        "date": 1,
        "fred.yield_spread_10y_2y": 1,
        "fred.fed_funds_rate": 1,
        "bls.unemployment_rate_bls": 1
    }
).sort("date", 1))

# --- Step 2: Extract series --- # 
dates = []
spread = []
unemployment = []
fed_funds = []

for d in docs:
    dates.append(d["date"])
    spread.append(d["fred"].get("yield_spread_10y_2y"))
    unemployment.append(d["bls"].get("unemployment_rate_bls"))
    fed_funds.append(d["fred"].get("fed_funds_rate"))

# --- Step 3: Select periods to shade --- # 
CRISIS_BANDS = {
    "Dotcom":  ("2001-03-01", "2002-11-01"),
    "GFC":     ("2007-12-01", "2009-06-01"),
    "COVID":   ("2020-02-01", "2020-04-01"),
}

# --- Step 4: Print key findings --- #
print("-- Top 5 Worst Unemployment Months --")
for month in get_high_unemployment_months(threshold=8.0, limit=5):
    print(f"  {month['date']}: {month['bls']['unemployment_rate_bls']}%")

print("\n-- Avg Unemployment by Decade --")
for decade in get_avg_unemployment_by_decade():
    print(f"  {decade['_id']}: {round(decade['avg_unemployment'], 2)}%")

print("\n-- Top 5 Worst Yield Curve Inversions --")
for inversion in get_yield_curve_inversions(limit=5):
    print(f"  {inversion['date']}: {inversion['fred']['yield_spread_10y_2y']}%")

print("\n-- COVID Monthly Snapshot (2020) --")
for snapshot in get_monthly_snapshot(year=2020):
    print(f"  {snapshot['date']} | "
          f"Unemployment: {snapshot['bls']['unemployment_rate_bls']}% | "
          f"Fed Funds: {snapshot['fred']['fed_funds_rate']}% | "
          f"Yield Spread: {snapshot['fred']['yield_spread_10y_2y']}%")

# --- Step 5: Create visualization --- # 
fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(14, 10), sharex=True)
fig.suptitle(
    "Yield Curve Inversions, Unemployment & Fed Policy: 1985 - Present\n"
    "Gray bands mark recession periods (Dotcom, GFC, COVID)",
    fontsize=13,
    fontweight="bold"
)

# Panel 1 — Yield Spread
ax1.plot(dates, spread, color="steelblue", linewidth=1)
ax1.axhline(0, color="red", linewidth=0.8, linestyle="--", label="Inversion threshold")
ax1.fill_between(
    dates, spread, 0,
    where=[s is not None and s < 0 for s in spread],
    color="red", alpha=0.3, label="Inverted"
)
ax1.set_ylabel("10Y - 2Y Spread (%)")
ax1.legend(fontsize=8, loc="upper right")

# Panel 2 — Unemployment
ax2.plot(dates, unemployment, color="darkorange", linewidth=1)
ax2.set_ylabel("Unemployment Rate (%)")

# Panel 3 — Fed Funds Rate
ax3.plot(dates, fed_funds, color="green", linewidth=1)
ax3.set_ylabel("Fed Funds Rate (%)")
ax3.set_xlabel("Date")

# Add crisis shading to all panels
for label, (start, end) in CRISIS_BANDS.items():
    for ax in [ax1, ax2, ax3]:
        ax.axvspan(start, end, alpha=0.12, color="gray")
    ax1.text(
        start, ax1.get_ylim()[1] * 0.75,
        label, fontsize=8, color="dimgray"
    )

# X-axis ticks every 5 years
tick_dates = [d for d in dates if d[5:] == "01 - 01" and int(d[:4]) % 5 == 0]
ax3.set_xticks(tick_dates)
ax3.set_xticklabels([d[:4] for d in tick_dates], rotation=45)

plt.tight_layout()
os.makedirs("viz", exist_ok=True)
plt.savefig("viz/crisis_visualization.png", dpi=150, bbox_inches="tight")
plt.show()
print("\nChart saved to viz/crisis_visualization.png")

