#!/usr/bin/env python3
"""
Continuous ADS-B Flight & Position Logger for readsb (/run/readsb/aircraft.json)
backed by PostgreSQL 17 + TimescaleDB hypertables (dbname=sdr_station).
Logs aircraft positions, signal strength (RSSI), optional Haversine range/bearing
when STATION_LAT and STATION_LON are configured, and aggregated flight sessions.
"""

import json
import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras

AIRCRAFT_JSON = Path(os.environ.get("READSB_JSON", "/run/readsb/aircraft.json"))
RECEIVER_JSON = Path(os.environ.get("READSB_RECEIVER_JSON", "/run/readsb/receiver.json"))
PG_DSN = os.environ.get("SDR_PG_DSN", "dbname=sdr_station")
POLL_INTERVAL = float(os.environ.get("ADSB_POLL_INTERVAL", "5.0"))


def _parse_coord(env_key: str):
    raw = os.environ.get(env_key, "").strip()
    if not raw or raw.startswith("<"):
        return None
    try:
        return float(raw)
    except ValueError:
        return None


DEFAULT_LAT = _parse_coord("STATION_LAT")
DEFAULT_LON = _parse_coord("STATION_LON")


def haversine_km_bearing(lat1: float, lon1: float, lat2: float, lon2: float):
    """Calculate distance (km) and bearing (degrees 0-360) from station to aircraft."""
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    dist_km = 2 * r * math.asin(math.sqrt(a))

    y = math.sin(dl) * math.cos(p2)
    x = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    brng = (math.degrees(math.atan2(y, x)) + 360.0) % 360.0
    return round(dist_km, 2), round(brng, 1)


def init_db(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS adsb_positions (
                ts TIMESTAMPTZ NOT NULL,
                hex TEXT NOT NULL,
                flight TEXT,
                category TEXT,
                squawk TEXT,
                lat DOUBLE PRECISION,
                lon DOUBLE PRECISION,
                alt_baro INTEGER,
                alt_geom INTEGER,
                gs REAL,
                track REAL,
                vert_rate INTEGER,
                rssi REAL,
                messages INTEGER,
                distance_km REAL,
                bearing_deg REAL
            );
            SELECT create_hypertable('adsb_positions', 'ts', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
            CREATE INDEX IF NOT EXISTS idx_adsb_pos_hex_ts ON adsb_positions (hex, ts DESC);

            CREATE TABLE IF NOT EXISTS adsb_flights (
                session_id TEXT PRIMARY KEY,
                hex TEXT NOT NULL,
                flight TEXT,
                category TEXT,
                first_seen DOUBLE PRECISION NOT NULL,
                last_seen DOUBLE PRECISION NOT NULL,
                min_alt INTEGER,
                max_alt INTEGER,
                max_gs REAL,
                min_rssi REAL,
                max_rssi REAL,
                min_distance_km REAL,
                max_distance_km REAL,
                samples INTEGER DEFAULT 1
            );
            CREATE INDEX IF NOT EXISTS idx_adsb_flights_last_seen ON adsb_flights (last_seen DESC);
            """
        )
    conn.commit()


def get_station_coords():
    if DEFAULT_LAT is not None and DEFAULT_LON is not None:
        return DEFAULT_LAT, DEFAULT_LON
    if RECEIVER_JSON.exists():
        try:
            data = json.loads(RECEIVER_JSON.read_text())
            if "lat" in data and "lon" in data:
                return float(data["lat"]), float(data["lon"])
        except Exception:
            pass
    return None, None


def main():
    conn = psycopg2.connect(PG_DSN)
    init_db(conn)
    station_lat, station_lon = get_station_coords()
    last_messages_per_hex = {}

    print(f"[adsb-logger] Logging from {AIRCRAFT_JSON} -> TimescaleDB ({PG_DSN})", flush=True)
    while True:
        try:
            if conn.closed:
                conn = psycopg2.connect(PG_DSN)

            if AIRCRAFT_JSON.exists():
                mtime = AIRCRAFT_JSON.stat().st_mtime
                if time.time() - mtime < 30.0:
                    payload = json.loads(AIRCRAFT_JSON.read_text())
                    now_ts = float(payload.get("now", time.time()))
                    dt_utc = datetime.fromtimestamp(now_ts, tz=timezone.utc)
                    day_str = dt_utc.strftime("%Y-%m-%d")
                    aircraft_list = payload.get("aircraft", [])

                    rows_to_insert = []
                    flight_upserts = []
                    for ac in aircraft_list:
                        seen = ac.get("seen", 999)
                        if seen > 60:
                            continue
                        hex_code = str(ac.get("hex", "")).strip().lower()
                        if not hex_code:
                            continue

                        messages = ac.get("messages", 1)
                        if last_messages_per_hex.get(hex_code) == messages:
                            continue
                        last_messages_per_hex[hex_code] = messages

                        flight = (ac.get("flight") or "").strip() or None
                        category = ac.get("category")
                        squawk = ac.get("squawk")
                        lat = ac.get("lat")
                        lon = ac.get("lon")
                        alt_baro = ac.get("alt_baro") if isinstance(ac.get("alt_baro"), int) else None
                        alt_geom = ac.get("alt_geom") if isinstance(ac.get("alt_geom"), int) else None
                        gs = ac.get("gs")
                        track = ac.get("track")
                        vert_rate = ac.get("baro_rate") or ac.get("geom_rate")
                        rssi = ac.get("rssi")

                        dist_km, bearing_deg = (None, None)
                        if (
                            station_lat is not None
                            and station_lon is not None
                            and isinstance(lat, (int, float))
                            and isinstance(lon, (int, float))
                        ):
                            dist_km, bearing_deg = haversine_km_bearing(station_lat, station_lon, lat, lon)

                        rows_to_insert.append(
                            (
                                dt_utc,
                                hex_code,
                                flight,
                                category,
                                squawk,
                                lat,
                                lon,
                                alt_baro,
                                alt_geom,
                                gs,
                                track,
                                vert_rate,
                                rssi,
                                messages,
                                dist_km,
                                bearing_deg,
                            )
                        )

                        session_id = f"{day_str}:{hex_code}"
                        flight_upserts.append(
                            (
                                session_id,
                                hex_code,
                                flight,
                                category,
                                now_ts,
                                now_ts,
                                alt_baro,
                                alt_baro,
                                gs,
                                rssi,
                                rssi,
                                dist_km,
                                dist_km,
                            )
                        )

                    if rows_to_insert:
                        with conn.cursor() as cur:
                            psycopg2.extras.execute_values(
                                cur,
                                """
                                INSERT INTO adsb_positions (
                                    ts, hex, flight, category, squawk,
                                    lat, lon, alt_baro, alt_geom, gs, track, vert_rate,
                                    rssi, messages, distance_km, bearing_deg
                                ) VALUES %s
                                """,
                                rows_to_insert,
                            )
                            psycopg2.extras.execute_values(
                                cur,
                                """
                                INSERT INTO adsb_flights (
                                    session_id, hex, flight, category, first_seen, last_seen,
                                    min_alt, max_alt, max_gs, min_rssi, max_rssi,
                                    min_distance_km, max_distance_km, samples
                                ) VALUES %s
                                ON CONFLICT (session_id) DO UPDATE SET
                                    flight = COALESCE(EXCLUDED.flight, adsb_flights.flight),
                                    category = COALESCE(EXCLUDED.category, adsb_flights.category),
                                    last_seen = EXCLUDED.last_seen,
                                    min_alt = LEAST(COALESCE(adsb_flights.min_alt, EXCLUDED.min_alt), COALESCE(EXCLUDED.min_alt, adsb_flights.min_alt)),
                                    max_alt = GREATEST(COALESCE(adsb_flights.max_alt, EXCLUDED.max_alt), COALESCE(EXCLUDED.max_alt, adsb_flights.max_alt)),
                                    max_gs = GREATEST(COALESCE(adsb_flights.max_gs, EXCLUDED.max_gs), COALESCE(EXCLUDED.max_gs, adsb_flights.max_gs)),
                                    min_rssi = LEAST(COALESCE(adsb_flights.min_rssi, EXCLUDED.min_rssi), COALESCE(EXCLUDED.min_rssi, adsb_flights.min_rssi)),
                                    max_rssi = GREATEST(COALESCE(adsb_flights.max_rssi, EXCLUDED.max_rssi), COALESCE(EXCLUDED.max_rssi, adsb_flights.max_rssi)),
                                    min_distance_km = LEAST(COALESCE(adsb_flights.min_distance_km, EXCLUDED.min_distance_km), COALESCE(EXCLUDED.min_distance_km, adsb_flights.min_distance_km)),
                                    max_distance_km = GREATEST(COALESCE(adsb_flights.max_distance_km, EXCLUDED.max_distance_km), COALESCE(EXCLUDED.max_distance_km, adsb_flights.max_distance_km)),
                                    samples = adsb_flights.samples + 1
                                """,
                                flight_upserts,
                                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)",
                            )
                        conn.commit()
        except Exception as exc:
            print(f"[adsb-logger] Warning: {exc}", flush=True)
            try:
                conn.rollback()
            except Exception:
                pass

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
