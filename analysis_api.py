"""
Course: DS4300 with Professor Rachlin
Assignment: Homework 3
Written By: Erika Sohn

Objective:
- Analytical PyMongo API layer for US Macro/Labor dataset
- Connects directly to MongoDB Atlas and uses 4 functions that allow us to ask economic questions about dataset 
- Overrides the need to know mongoDB 

Dataset:
- Sourced from (1) FRED (2) Bureau of Labor Statistics (3) U.S. Censeus Bureau
- Monthly snapshots from 1985 to present
- Each document contains nested subdocuments for each source
- Data is keyed by date 

"""

import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

client = MongoClient(os.getenv("MONGO_HOST"))
db = client["HW3"]
collection = db["macro_labor"]


def get_high_unemployment_months(threshold: float = 8.0, limit: int = 10): # 8% is recognized as crisis-level unemployment in economics
    """
    Return months where unemployment exceeded a given threshold
    Parameters:
        threshold: unemployment rate cutoff (default 8.0)
        limit: max number of results (default 10)
    """
    results = collection.find(
        {"bls.unemployment_rate_bls": {"$gt": threshold}},
        {"_id": 0, "date": 1, "bls.unemployment_rate_bls": 1}
    ).sort("bls.unemployment_rate_bls", -1).limit(limit)
    return list(results)

def get_avg_unemployment_by_decade():
    """
    Return avg unemployment rate grouped by decade
    Reveals long-term and structural shifts in the US labor market
    """
    results = collection.aggregate([
        {"$match": {"bls.unemployment_rate_bls": {"$ne": None}}},
        {"$project": {
            "decade": {
                "$concat": [
                    {"$substr": ["$date", 0, 3]},
                    "0s"
                ]
            },
            "unemployment": "$bls.unemployment_rate_bls"
        }},
        {"$group": {
            "_id": "$decade",
            "avg_unemployment": {"$avg": "$unemployment"}
        }},
        {"$sort": {"_id": 1}}
    ])
    return list(results)


def get_yield_curve_inversions(limit: int = 10):
    """
    Return months with most negative 10Y-2Y yield spread
    Inversions historically come before recessions by 12-18 months
    Parameters:
        limit: number of results to return (default 10)
    """
    results = collection.find(
        {"fred.yield_spread_10y_2y": {"$lt": 0}},
        {"_id": 0, "date": 1, "fred.yield_spread_10y_2y": 1}
    ).sort("fred.yield_spread_10y_2y", 1).limit(limit)
    return list(results)


def get_monthly_snapshot(year: int = 2020):
    """
    Return all monthly records for a given year showing
    data from all three sources: FRED, BLS, and Census
    Parameters:
        year: the year to query (default 2020)
    """
    results = collection.find(
        {"date": {"$regex": f"^{year}"}},
        {"_id": 0}
    ).sort("date", 1)
    return list(results)


if __name__ == "__main__":
    print("--High Unemployment Months (>8%) --")
    for month in get_high_unemployment_months(threshold=8.0, limit=5):
        print(month)

    print("\n-- Avg Unemployment by Decade --")
    for month in get_avg_unemployment_by_decade():
        print(month)

    print("\n-- Worst Yield Curve Inversions --")
    for month in get_yield_curve_inversions(limit=5):
        print(month)

    print("\n-- Monthly Snapshot 2020 --")
    for month in get_monthly_snapshot(year=2020):
        print(month)