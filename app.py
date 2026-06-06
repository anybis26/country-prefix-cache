#!/usr/bin/env python3
"""RU IPv4 Prefix Aggregator."""

import os
import sqlite3
import threading
import time

import requests

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from concurrent.futures import ThreadPoolExecutor, as_completed

DB = os.getenv("DB", "/data/ru-prefixes.db")

ASN_CACHE_URL = os.getenv(
    "ASN_CACHE_URL",
    "http://asn-cache:8080"
)

app = FastAPI(title="RU Prefix Aggregator")

refresh_lock = threading.Lock()

state = {
    "running": False,
    "last_update": 0,
    "prefixes": 0,
    "asns": 0,
    "asns_list": []
}


# ----------------------------------------------------
# DB
# ----------------------------------------------------

def db():
    """Create SQLite connection."""
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Initialize SQLite schema."""
    conn = db()

    conn.execute("""
    CREATE TABLE IF NOT EXISTS prefixes (
        prefix TEXT PRIMARY KEY,
        updated INTEGER NOT NULL
    )
    """)

    conn.commit()
    conn.close()


# ----------------------------------------------------
# SOURCES
# ----------------------------------------------------

def fetch_ru_asns():
    """Fetch RU ASN list from RIPE."""

    url = (
        "https://stat.ripe.net/data/"
        "country-resource-list/data.json"
        "?resource=RU"
    )

    r = requests.get(url, timeout=120)
    r.raise_for_status()

    data = r.json()

    return sorted(
        f"AS{asn}"
        for asn in data["data"]["resources"]["asn"]
    )


def fetch_asn_prefixes(asn):
    """Fetch ASN prefixes from ASN Cache Proxy."""

    url = (
        f"{ASN_CACHE_URL}"
        f"/data/announced-prefixes/data.json"
        f"?resource={asn}"
    )

    r = requests.get(url, timeout=120)
    r.raise_for_status()

    data = r.json()

    result = []

    for item in data.get("data", {}).get("prefixes", []):
        prefix = item.get("prefix")

        if prefix and "." in prefix:
            result.append(prefix)

    return result


# ----------------------------------------------------
# CACHE
# ----------------------------------------------------

def get_prefixes():
    """Return cached prefixes."""

    conn = db()

    rows = conn.execute(
        """
        SELECT prefix
        FROM prefixes
        ORDER BY prefix
        """
    ).fetchall()

    conn.close()

    return [row["prefix"] for row in rows]


def save_prefixes(prefixes):
    """Save prefixes to SQLite."""

    conn = db()

    conn.execute("DELETE FROM prefixes")

    now = int(time.time())

    for prefix in sorted(prefixes):
        conn.execute(
            """
            INSERT OR REPLACE
            INTO prefixes(prefix, updated)
            VALUES(?,?)
            """,
            (prefix, now)
        )

    conn.commit()
    conn.close()


# ----------------------------------------------------
# REFRESH
# ----------------------------------------------------

def refresh_worker():
    """Background refresh."""

    lock_acquired = False

    if not refresh_lock.acquire(blocking=False):
        return

    lock_acquired = True
    state["running"] = True

    try:
        print("Starting RU refresh...")

        asns = fetch_ru_asns()

        if not asns:
            print("RIPE returned empty ASN list, abort refresh")

            state["last_update"] = int(time.time())
            return

        prefixes = set()

        state["asns"] = len(asns)
        state["asns_list"] = asns

        with ThreadPoolExecutor(max_workers=20) as executor:

            futures = {
                executor.submit(fetch_asn_prefixes, asn): asn
                for asn in asns
            }

            processed = 0

            for future in as_completed(futures):

                asn = futures[future]
                processed += 1

                try:
                    data = future.result()
                    prefixes.update(data)

                except Exception as exc:
                    print(f"{asn}: {exc}")

                if processed % 100 == 0:
                    print(
                        f"Processed {processed}/{len(asns)} ASN "
                        f"({len(prefixes)} prefixes)"
                    )

        prefixes.discard("0.0.0.0/0")

        if not prefixes:
            print("No prefixes collected, skipping DB write")
            return

        save_prefixes(prefixes)

        state["prefixes"] = len(prefixes)
        state["last_update"] = int(time.time())

        print(
            f"Refresh complete: "
            f"{len(asns)} ASN, "
            f"{len(prefixes)} prefixes"
        )

    finally:
        state["running"] = False

        if lock_acquired:
            refresh_lock.release()


# ----------------------------------------------------
# API
# ----------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/status")
def status():
    return {
        "running": state["running"],
        "last_update": state["last_update"],
        "prefixes": state["prefixes"],
        "asns": state["asns"]
    }


@app.get("/prefixes")
def prefixes():
    data = get_prefixes()

    return JSONResponse(
        {
            "status": "ok",
            "country": "RU",
            "count": len(data),
            "prefixes": data
        }
    )


@app.post("/refresh")
def refresh():

    if state["running"]:
        return {"status": "already_running"}

    thread = threading.Thread(
        target=refresh_worker,
        daemon=True
    )

    thread.start()

    return {"status": "started"}


# ----------------------------------------------------

init_db()
