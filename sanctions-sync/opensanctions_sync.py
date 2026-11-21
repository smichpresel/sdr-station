#!/usr/bin/env python3
"""
Live OpenSanctions Maritime & Aviation Watchlist Auto-Sync Engine
backed by PostgreSQL 17 + TimescaleDB (dbname=sdr_station).

Downloads OpenSanctions' consolidated international sanctions feed
(https://data.opensanctions.org/datasets/latest/sanctions/targets.simple.csv),
extracts all sanctioned Vessel (IMO, MMSI, Name) and Airplane (Registration,
Aliases, Serial/Hex) identifiers across OFAC SDN, EU, UK FCDO, UN, SECO,
DFAT, and Canadian sanctions programs, and indexes them into `sanctions_watchlist`.
"""

import argparse
import csv
import io
import json
import os
import re
import ssl
import time
import urllib.request
from pathlib import Path

import psycopg2
import psycopg2.extras

PG_DSN = os.environ.get("SDR_PG_DSN", "dbname=sdr_station")
OPENSANCTIONS_CSV_URL = os.environ.get(
    "OPENSANCTIONS_CSV_URL",
    "https://data.opensanctions.org/datasets/latest/sanctions/targets.simple.csv",
)

IMO_RE = re.compile(r"\bIMO\s*(\d{7})\b", re.IGNORECASE)
MMSI_RE = re.compile(r"\b([2-7]\d{8})\b")
AIRCRAFT_REG_RE = re.compile(
    r"\b([A-Z]{1,2}-[A-Z0-9]{2,5}|RA-\d{5}|RF-\d{5}|EP-[A-Z]{3}|EW-\d{3}[A-Z]{2}|YV\d{3,4}|N\d{1,5}[A-Z]{0,2})\b"
)

_WATCHLIST_CACHE = {
    "ts": 0.0,
    "imo": {},
    "mmsi": {},
    "shipname": {},
    "reg": {},
    "icao_hex": {},
    "stats": {"vessels": 0, "aircraft": 0, "total_keys": 0, "last_sync_utc": None},
}


def init_sanctions_db(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sanctions_watchlist (
                id_type TEXT NOT NULL,
                id_value TEXT NOT NULL,
                domain TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                entity_name TEXT NOT NULL,
                countries TEXT,
                datasets TEXT NOT NULL,
                first_seen TEXT,
                last_seen TEXT,
                synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (id_type, id_value)
            );
            CREATE INDEX IF NOT EXISTS idx_sanctions_watchlist_domain
                ON sanctions_watchlist (domain, id_type);
            """
        )
    conn.commit()


def _shorten_datasets(raw_ds: str) -> str:
    parts = [p.strip() for p in (raw_ds or "").split(";") if p.strip()]
    tags = []
    joined = " ".join(parts).upper()
    if "OFAC" in joined or "SDN" in joined:
        tags.append("US OFAC SDN")
    if "EU " in joined or "EUROPEAN" in joined:
        tags.append("EU")
    if "UK " in joined or "FCDO" in joined or "OFSI" in joined:
        tags.append("UK FCDO")
    if "SECO" in joined or "SWITZERLAND" in joined:
        tags.append("Swiss SECO")
    if "AUSTRALIAN" in joined:
        tags.append("AU DFAT")
    if "CANADIAN" in joined:
        tags.append("CA SEMA")
    if "UN " in joined or "UNITED NATIONS" in joined:
        tags.append("UN")
    if not tags and parts:
        tags.append(parts[0][:36])
    return ", ".join(tags) if tags else "OpenSanctions Watchlist"


def sync_opensanctions(dsn: str = PG_DSN, url: str = OPENSANCTIONS_CSV_URL) -> dict:
    """
    Stream OpenSanctions targets.simple.csv and populate sanctions_watchlist in PostgreSQL.
    """
    t0 = time.time()
    conn = psycopg2.connect(dsn)
    init_sanctions_db(conn)

    ctx = ssl._create_unverified_context()
    req = urllib.request.Request(url, headers={"User-Agent": "SDR-Station-OpenSanctions-Sync/1.0"})
    resp = urllib.request.urlopen(req, timeout=60, context=ctx)
    reader = csv.DictReader(io.TextIOWrapper(resp, encoding="utf-8", errors="replace"))

    records = {}
    vessel_entities = 0
    airplane_entities = 0

    for row in reader:
        schema = (row.get("schema") or "").strip()
        if schema not in ("Vessel", "Airplane"):
            continue

        ent_id = (row.get("id") or "").strip()
        name = (row.get("name") or "").strip()
        aliases = (row.get("aliases") or "").strip()
        idents = (row.get("identifiers") or "").strip()
        countries = (row.get("countries") or "").strip()
        ds_short = _shorten_datasets(row.get("dataset") or "")
        first_seen = (row.get("first_seen") or "").strip()
        last_seen = (row.get("last_seen") or "").strip()

        if schema == "Vessel":
            vessel_entities += 1
            for m in IMO_RE.finditer(f"{idents};{aliases};{name}"):
                imo_val = m.group(1)
                records[("imo", imo_val)] = {
                    "id_type": "imo",
                    "id_value": imo_val,
                    "domain": "ship",
                    "entity_id": ent_id,
                    "entity_name": name,
                    "countries": countries,
                    "datasets": ds_short,
                    "first_seen": first_seen,
                    "last_seen": last_seen,
                }
            for m in MMSI_RE.finditer(idents):
                mmsi_val = m.group(1)
                records[("mmsi", mmsi_val)] = {
                    "id_type": "mmsi",
                    "id_value": mmsi_val,
                    "domain": "ship",
                    "entity_id": ent_id,
                    "entity_name": name,
                    "countries": countries,
                    "datasets": ds_short,
                    "first_seen": first_seen,
                    "last_seen": last_seen,
                }
            norm_name = re.sub(r"\s+", " ", name.upper()).strip()
            if len(norm_name) >= 5 and norm_name not in ("VESSEL", "UNKNOWN", "TANKER"):
                records[("shipname", norm_name)] = {
                    "id_type": "shipname",
                    "id_value": norm_name,
                    "domain": "ship",
                    "entity_id": ent_id,
                    "entity_name": name,
                    "countries": countries,
                    "datasets": ds_short,
                    "first_seen": first_seen,
                    "last_seen": last_seen,
                }

        elif schema == "Airplane":
            airplane_entities += 1
            hay = f"{name};{aliases};{idents}".upper()
            for m in AIRCRAFT_REG_RE.finditer(hay):
                reg_val = m.group(1).strip()
                for variant in {reg_val, reg_val.replace("-", "")}:
                    if len(variant) >= 4:
                        records[("reg", variant)] = {
                            "id_type": "reg",
                            "id_value": variant,
                            "domain": "aircraft",
                            "entity_id": ent_id,
                            "entity_name": name,
                            "countries": countries,
                            "datasets": ds_short,
                            "first_seen": first_seen,
                            "last_seen": last_seen,
                        }

    batch = list(records.values())
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            """
            INSERT INTO sanctions_watchlist (
                id_type, id_value, domain, entity_id, entity_name,
                countries, datasets, first_seen, last_seen, synced_at
            ) VALUES (
                %(id_type)s, %(id_value)s, %(domain)s, %(entity_id)s, %(entity_name)s,
                %(countries)s, %(datasets)s, %(first_seen)s, %(last_seen)s, NOW()
            )
            ON CONFLICT (id_type, id_value) DO UPDATE SET
                entity_id = EXCLUDED.entity_id,
                entity_name = EXCLUDED.entity_name,
                countries = EXCLUDED.countries,
                datasets = EXCLUDED.datasets,
                last_seen = EXCLUDED.last_seen,
                synced_at = NOW()
            """,
            batch,
            page_size=500,
        )
    conn.commit()
    conn.close()

    # Force cache refresh
    _WATCHLIST_CACHE["ts"] = 0.0
    cache = get_watchlist_cache(dsn)
    elapsed = round(time.time() - t0, 2)
    return {
        "ok": True,
        "vessel_entities": vessel_entities,
        "airplane_entities": airplane_entities,
        "indexed_identifiers": len(batch),
        "elapsed_sec": elapsed,
        "stats": cache["stats"],
    }


def get_watchlist_cache(dsn: str = PG_DSN) -> dict:
    now = time.time()
    if _WATCHLIST_CACHE["ts"] > 0 and (now - _WATCHLIST_CACHE["ts"]) < 300.0:
        return _WATCHLIST_CACHE

    imo_map = {}
    mmsi_map = {}
    name_map = {}
    reg_map = {}
    hex_map = {}
    last_sync = None
    vessels_cnt = 0
    aircraft_cnt = 0

    try:
        conn = psycopg2.connect(dsn)
        init_sanctions_db(conn)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM sanctions_watchlist")
            for r in cur.fetchall():
                entry = {
                    "entity_id": r["entity_id"],
                    "entity_name": r["entity_name"],
                    "countries": r["countries"],
                    "datasets": r["datasets"],
                }
                t = r["id_type"]
                v = r["id_value"]
                if t == "imo":
                    imo_map[v] = entry
                elif t == "mmsi":
                    mmsi_map[v] = entry
                elif t == "shipname":
                    name_map[v] = entry
                elif t == "reg":
                    reg_map[v] = entry
                elif t == "icao_hex":
                    hex_map[v] = entry
                if r.get("synced_at") and (last_sync is None or r["synced_at"] > last_sync):
                    last_sync = r["synced_at"]

            cur.execute(
                """
                SELECT
                    COUNT(DISTINCT entity_id) FILTER (WHERE domain = 'ship') AS vessels,
                    COUNT(DISTINCT entity_id) FILTER (WHERE domain = 'aircraft') AS aircraft
                FROM sanctions_watchlist
                """
            )
            agg = cur.fetchone() or {}
            vessels_cnt = agg.get("vessels") or 0
            aircraft_cnt = agg.get("aircraft") or 0
        conn.close()
    except Exception as e:
        print(f"[opensanctions] Cache load warning: {e}", flush=True)

    _WATCHLIST_CACHE.update(
        {
            "ts": now,
            "imo": imo_map,
            "mmsi": mmsi_map,
            "shipname": name_map,
            "reg": reg_map,
            "icao_hex": hex_map,
            "stats": {
                "vessels": vessels_cnt,
                "aircraft": aircraft_cnt,
                "imo_keys": len(imo_map),
                "mmsi_keys": len(mmsi_map),
                "reg_keys": len(reg_map),
                "total_keys": len(imo_map) + len(mmsi_map) + len(name_map) + len(reg_map) + len(hex_map),
                "last_sync_utc": last_sync.strftime("%Y-%m-%d %H:%M UTC") if last_sync else None,
            },
        }
    )
    return _WATCHLIST_CACHE


def match_vessel_sanctions(mmsi=None, imo=None, shipname: str | None = None, dsn: str = PG_DSN) -> dict | None:
    cache = get_watchlist_cache(dsn)
    if imo:
        m = cache["imo"].get(str(imo).strip())
        if m:
            return {**m, "matched_on": f"IMO {imo}"}
    if mmsi:
        m = cache["mmsi"].get(str(mmsi).strip())
        if m:
            return {**m, "matched_on": f"MMSI {mmsi}"}
    if shipname and shipname != "—":
        norm = re.sub(r"\s+", " ", shipname.upper()).strip()
        m = cache["shipname"].get(norm)
        if m:
            return {**m, "matched_on": f"Name '{shipname}'"}
    return None


def match_aircraft_sanctions(hex_code: str | None = None, reg: str | None = None, callsign: str | None = None, dsn: str = PG_DSN) -> dict | None:
    cache = get_watchlist_cache(dsn)
    if hex_code:
        m = cache["icao_hex"].get(hex_code.upper().strip())
        if m:
            return {**m, "matched_on": f"ICAO {hex_code.upper()}"}
    for cand in (reg, callsign):
        if cand and cand != "—":
            c_up = cand.upper().strip()
            m = cache["reg"].get(c_up) or cache["reg"].get(c_up.replace("-", ""))
            if m:
                return {**m, "matched_on": f"Registration {cand}"}
    return None


def main():
    parser = argparse.ArgumentParser(description="Sync OpenSanctions Maritime & Aviation Watchlist into TimescaleDB")
    parser.add_argument("--status", action="store_true", help="Show current watchlist count without syncing")
    args = parser.parse_args()

    if args.status:
        c = get_watchlist_cache()
        print(json.dumps(c["stats"], indent=2))
        return

    res = sync_opensanctions()
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
