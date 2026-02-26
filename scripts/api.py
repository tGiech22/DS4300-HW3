"""Basic CRUD API for MongoDB macro/labor dataset."""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from pymongo import MongoClient

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

if load_dotenv is not None:
    load_dotenv()

MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

if not MONGO_HOST or not MONGO_DB or not MONGO_COLLECTION:
    raise RuntimeError("Missing MONGO_HOST, MONGO_DB, or MONGO_COLLECTION in environment.")

client = MongoClient(MONGO_HOST)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

app = FastAPI(title="Macro/Labor CRUD API")


class Record(BaseModel):
    date: str
    fred: Dict[str, Optional[float]]
    bls: Dict[str, Optional[float]]
    census: Dict[str, Optional[float]]


@app.get("/records", response_model=List[Record])
def list_records(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
) -> List[Record]:
    cursor = collection.find({}, {"_id": 0}).skip(skip).limit(limit).sort("date", 1)
    return list(cursor)


@app.get("/records/{date}", response_model=Record)
def get_record(date: str) -> Record:
    doc = collection.find_one({"date": date}, {"_id": 0})
    if not doc:
        raise HTTPException(status_code=404, detail="Record not found")
    return doc


@app.post("/records", response_model=Record, status_code=201)
def create_record(record: Record) -> Record:
    existing = collection.find_one({"date": record.date}, {"_id": 1})
    if existing:
        raise HTTPException(status_code=409, detail="Record with this date already exists")
    collection.insert_one(record.dict())
    return record


@app.put("/records/{date}", response_model=Record)
def update_record(date: str, record: Record) -> Record:
    if date != record.date:
        raise HTTPException(status_code=400, detail="Date in path must match record.date")
    result = collection.update_one({"date": date}, {"$set": record.dict()})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Record not found")
    return record


@app.delete("/records/{date}")
def delete_record(date: str) -> Dict[str, Any]:
    result = collection.delete_one({"date": date})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Record not found")
    return {"deleted": True, "date": date}
