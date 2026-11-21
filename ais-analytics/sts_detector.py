#!/usr/bin/env python3
"""
Dark-Fleet Ship-to-Ship (STS) Rendezvous, Offshore Loitering & AIS Dark-Gap Detector
backed by PostgreSQL 17 + TimescaleDB (dbname=sdr_station).

Analyzes historical and live AIS position time-series (ais_archive_ts + ais_position)
joined with vessel static metadata (ais_state) to detect:
  1. Ship-to-Ship (STS) transfers / close-quarters rendezvous (< 220m separation at <= 2.5 kt
     sustained across >= 3 five-minute buckets, excluding harbour tugs/pilots).
  2. Offshore loitering / drifting tankers & cargo ships (0.3-3.0 kt with >= 90 deg course wander).
  3. Underway AIS dark-gap / transponder blackout anomalies.

Persists findings into `ais_sts_events` and syncs high-confidence events into `spotter_highlights`.
"""

import argparse
import json
import math
import os
import time
from pathlib import Path
from urllib.parse import quote_plus

import psycopg2
import psycopg2.extras

PG_DSN = os.environ.get("SDR_PG_DSN", "dbname=sdr_station")
_LAST_STS_SCAN = 0.0


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return round(2 * r * math.asin(math.sqrt(a)), 1)


def init_sts_db(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ais_sts_events (
                event_id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                primary_mmsi BIGINT NOT NULL,
                primary_name TEXT NOT NULL,
                secondary_mmsi BIGINT,
                secondary_name TEXT,
                badge TEXT NOT NULL,
                score INTEGER NOT NULL,
                summary TEXT NOT NULL,
                min_dist_m DOUBLE PRECISION,
                duration_min INTEGER,
                lat DOUBLE PRECISION,
                lon DOUBLE PRECISION,
                first_seen TIMESTAMPTZ NOT NULL,
                last_seen TIMESTAMPTZ NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_ais_sts_events_score
                ON ais_sts_events (score DESC, last_seen DESC);
            """
        )
    conn.commit()


def run_sts_detection(dsn: str = PG_DSN, force: bool = False) -> list[dict]:
    """
    Executes time-bucketed spatial proximity and kinematic queries on TimescaleDB
    to detect STS transfers and loitering vessels.
    """
    global _LAST_STS_SCAN
    now = time.time()
    if not force and (now - _LAST_STS_SCAN) < 180.0:
        return get_sts_events(dsn)
    _LAST_STS_SCAN = now

    try:
        conn = psycopg2.connect(dsn)
        init_sts_db(conn)
    except Exception as e:
        print(f"[sts-detector] DB connect error: {e}", flush=True)
        return []

    detected_events = []
    highlight_upserts = []

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # 1. Ship-to-Ship (STS) Rendezvous Detection across 5-minute time buckets
            cur.execute(
                """
                WITH recent_pos AS (
                    SELECT
                        date_trunc('hour', received_at) +
                            floor(extract(minute FROM received_at) / 5.0) * interval '5 minutes' AS bucket,
                        mmsi,
                        AVG(lat)::float AS lat,
                        AVG(lon)::float AS lon,
                        AVG(COALESCE(speed, 0))::float AS spd
                    FROM ais_archive_ts
                    WHERE lat IS NOT NULL AND lon IS NOT NULL
                      AND (lat != 0 OR lon != 0)
                      AND COALESCE(speed, 0) <= 2.5
                    GROUP BY bucket, mmsi
                ),
                pairs AS (
                    SELECT
                        a.mmsi AS mmsi_a,
                        b.mmsi AS mmsi_b,
                        COUNT(*) AS buckets,
                        MIN(a.bucket) AS first_seen,
                        MAX(a.bucket) AS last_seen,
                        AVG(a.lat)::float AS lat_a,
                        AVG(a.lon)::float AS lon_a,
                        AVG(b.lat)::float AS lat_b,
                        AVG(b.lon)::float AS lon_b,
                        MIN(
                            6371000.0 * 2.0 * asin(sqrt(
                                power(sin(radians(b.lat - a.lat) / 2.0), 2) +
                                cos(radians(a.lat)) * cos(radians(b.lat)) *
                                power(sin(radians(b.lon - a.lon) / 2.0), 2)
                            ))
                        )::float AS min_dist_m
                    FROM recent_pos a
                    JOIN recent_pos b
                      ON a.bucket = b.bucket
                     AND a.mmsi < b.mmsi
                     AND ABS(a.lat - b.lat) <= 0.0022
                     AND ABS(a.lon - b.lon) <= 0.0022
                    GROUP BY a.mmsi, b.mmsi
                    HAVING COUNT(*) >= 3
                )
                SELECT
                    p.*,
                    COALESCE(NULLIF(TRIM(sa.shipname), ''), 'MMSI ' || p.mmsi_a::text) AS name_a,
                    COALESCE(sa.shiptype, 0) AS type_a,
                    (COALESCE(sa.to_bow, 0) + COALESCE(sa.to_stern, 0)) AS len_a,
                    COALESCE(NULLIF(TRIM(sa.destination), ''), '—') AS dest_a,
                    COALESCE(NULLIF(TRIM(sb.shipname), ''), 'MMSI ' || p.mmsi_b::text) AS name_b,
                    COALESCE(sb.shiptype, 0) AS type_b,
                    (COALESCE(sb.to_bow, 0) + COALESCE(sb.to_stern, 0)) AS len_b,
                    COALESCE(NULLIF(TRIM(sb.destination), ''), '—') AS dest_b
                FROM pairs p
                LEFT JOIN ais_state sa ON sa.mmsi = p.mmsi_a
                LEFT JOIN ais_state sb ON sb.mmsi = p.mmsi_b
                WHERE p.min_dist_m <= 220.0
                  AND COALESCE(sa.shiptype, 0) NOT IN (31, 32, 50, 52)
                  AND COALESCE(sb.shiptype, 0) NOT IN (31, 32, 50, 52)
                  AND (
                      (COALESCE(sa.shiptype, 0) BETWEEN 70 AND 89)
                      OR (COALESCE(sb.shiptype, 0) BETWEEN 70 AND 89)
                      OR (COALESCE(sa.to_bow, 0) + COALESCE(sa.to_stern, 0)) >= 120
                      OR (COALESCE(sb.to_bow, 0) + COALESCE(sb.to_stern, 0)) >= 120
                  )
                ORDER BY
                    CASE WHEN (COALESCE(sa.shiptype, 0) BETWEEN 80 AND 89 OR COALESCE(sb.shiptype, 0) BETWEEN 80 AND 89) THEN 0 ELSE 1 END,
                    p.buckets DESC,
                    p.min_dist_m ASC
                LIMIT 25
                """
            )
            sts_rows = cur.fetchall()

            for r in sts_rows:
                is_tanker_sts = (80 <= r["type_a"] <= 89) or (80 <= r["type_b"] <= 89)
                dur_min = int(r["buckets"]) * 5
                dist_m = round(float(r["min_dist_m"] or 0.0), 1)
                score = 156 if is_tanker_sts else 145
                badge = (
                    f"STS Tanker Transfer ({dist_m}m · {dur_min}m)"
                    if is_tanker_sts
                    else f"STS Close Rendezvous ({dist_m}m · {dur_min}m)"
                )
                summary = (
                    f"{r['name_a']} ({r['len_a'] or '—'}m) alongside {r['name_b']} ({r['len_b'] or '—'}m) "
                    f"within {dist_m}m for {dur_min}+ min (dest: '{r['dest_a']}' / '{r['dest_b']}')."
                )
                ev_id = f"sts:{r['mmsi_a']}:{r['mmsi_b']}"
                ev = {
                    "event_id": ev_id,
                    "event_type": "sts_transfer",
                    "primary_mmsi": r["mmsi_a"],
                    "primary_name": r["name_a"],
                    "secondary_mmsi": r["mmsi_b"],
                    "secondary_name": r["name_b"],
                    "badge": badge,
                    "score": score,
                    "summary": summary,
                    "min_dist_m": dist_m,
                    "duration_min": dur_min,
                    "lat": round(r["lat_a"], 5),
                    "lon": round(r["lon_a"], 5),
                    "first_seen": r["first_seen"],
                    "last_seen": r["last_seen"],
                }
                detected_events.append(ev)

            # 2. Offshore Loitering / Drifting Tankers & Large Cargo Vessels
            cur.execute(
                """
                WITH loiter AS (
                    SELECT
                        mmsi,
                        COUNT(DISTINCT date_trunc('minute', received_at)) AS active_mins,
                        MIN(received_at) AS first_seen,
                        MAX(received_at) AS last_seen,
                        AVG(lat)::float AS lat,
                        AVG(lon)::float AS lon,
                        ROUND(AVG(speed)::numeric, 1)::float AS avg_spd,
                        ROUND((MAX(course) - MIN(course))::numeric, 1)::float AS course_spread
                    FROM ais_archive_ts
                    WHERE lat IS NOT NULL AND lon IS NOT NULL
                      AND (lat != 0 OR lon != 0)
                      AND speed BETWEEN 0.4 AND 2.8
                      AND course IS NOT NULL
                    GROUP BY mmsi
                    HAVING COUNT(DISTINCT date_trunc('minute', received_at)) >= 12
                       AND (MAX(course) - MIN(course)) >= 110.0
                )
                SELECT
                    l.*,
                    COALESCE(NULLIF(TRIM(s.shipname), ''), 'MMSI ' || l.mmsi::text) AS shipname,
                    COALESCE(s.shiptype, 0) AS shiptype,
                    (COALESCE(s.to_bow, 0) + COALESCE(s.to_stern, 0)) AS len_m,
                    ROUND(s.draught::numeric, 1)::float AS draught,
                    COALESCE(NULLIF(TRIM(s.destination), ''), '—') AS destination
                FROM loiter l
                JOIN ais_state s ON s.mmsi = l.mmsi
                WHERE (COALESCE(s.shiptype, 0) BETWEEN 80 AND 89 OR (COALESCE(s.to_bow, 0) + COALESCE(s.to_stern, 0)) >= 200)
                ORDER BY
                    CASE WHEN COALESCE(s.shiptype, 0) BETWEEN 80 AND 89 THEN 0 ELSE 1 END,
                    l.active_mins DESC
                LIMIT 20
                """
            )
            for r in cur.fetchall():
                is_tanker = 80 <= r["shiptype"] <= 89
                dur_min = int(r["active_mins"])
                score = 142 if is_tanker else 134
                badge = (
                    f"Offshore Tanker Loitering ({r['avg_spd']} kt · {r['course_spread']}° drift)"
                    if is_tanker
                    else f"Offshore Vessel Loitering ({r['avg_spd']} kt · {dur_min}m)"
                )
                summary = (
                    f"{r['shipname']} (MMSI {r['mmsi']}, {r['len_m'] or '—'}m, {r['draught'] or '—'}m draught) — "
                    f"slow loitering/drifting at {r['avg_spd']} kt with {r['course_spread']}° heading wander over {dur_min} min (dest: '{r['destination']}')."
                )
                ev_id = f"loiter:{r['mmsi']}"
                detected_events.append(
                    {
                        "event_id": ev_id,
                        "event_type": "offshore_loitering",
                        "primary_mmsi": r["mmsi"],
                        "primary_name": r["shipname"],
                        "secondary_mmsi": None,
                        "secondary_name": None,
                        "badge": badge,
                        "score": score,
                        "summary": summary,
                        "min_dist_m": None,
                        "duration_min": dur_min,
                        "lat": round(r["lat"], 5),
                        "lon": round(r["lon"], 5),
                        "first_seen": r["first_seen"],
                        "last_seen": r["last_seen"],
                    }
                )

            if detected_events:
                psycopg2.extras.execute_batch(
                    cur,
                    """
                    INSERT INTO ais_sts_events (
                        event_id, event_type, primary_mmsi, primary_name,
                        secondary_mmsi, secondary_name, badge, score, summary,
                        min_dist_m, duration_min, lat, lon, first_seen, last_seen
                    ) VALUES (
                        %(event_id)s, %(event_type)s, %(primary_mmsi)s, %(primary_name)s,
                        %(secondary_mmsi)s, %(secondary_name)s, %(badge)s, %(score)s, %(summary)s,
                        %(min_dist_m)s, %(duration_min)s, %(lat)s, %(lon)s, %(first_seen)s, %(last_seen)s
                    )
                    ON CONFLICT (event_id) DO UPDATE SET
                        badge = EXCLUDED.badge,
                        score = GREATEST(ais_sts_events.score, EXCLUDED.score),
                        summary = EXCLUDED.summary,
                        min_dist_m = COALESCE(EXCLUDED.min_dist_m, ais_sts_events.min_dist_m),
                        duration_min = GREATEST(ais_sts_events.duration_min, EXCLUDED.duration_min),
                        lat = EXCLUDED.lat,
                        lon = EXCLUDED.lon,
                        last_seen = GREATEST(ais_sts_events.last_seen, EXCLUDED.last_seen)
                    """,
                    detected_events,
                    page_size=50,
                )

                for ev in detected_events[:15]:
                    m = str(ev["primary_mmsi"])
                    links = [
                        {"label": "OpenSanctions", "url": f"https://www.opensanctions.org/search/?q={quote_plus(ev['primary_name'])}"},
                        {"label": "VesselFinder", "url": f"https://www.vesselfinder.com/vessels/details/{m}"},
                        {"label": "MarineTraffic", "url": f"https://www.marinetraffic.com/en/ais/details/ships/mmsi:{m}"},
                    ]
                    if ev["secondary_mmsi"]:
                        m2 = str(ev["secondary_mmsi"])
                        links.append({"label": f"Partner ({ev['secondary_name']})", "url": f"https://www.marinetraffic.com/en/ais/details/ships/mmsi:{m2}"})
                    highlight_upserts.append(
                        {
                            "domain": "ship",
                            "target_id": m,
                            "name_or_reg": (
                                f"{ev['primary_name']} ↔ {ev['secondary_name']}"
                                if ev["secondary_name"]
                                else ev["primary_name"]
                            ),
                            "secondary_id": (
                                f"STS MMSI {ev['primary_mmsi']} / {ev['secondary_mmsi']}"
                                if ev["secondary_mmsi"]
                                else f"MMSI {ev['primary_mmsi']}"
                            ),
                            "category_key": "ais_incident",
                            "badge": ev["badge"],
                            "score": ev["score"],
                            "reason": ev["summary"],
                            "lat": ev["lat"],
                            "lon": ev["lon"],
                            "metric_summary": (
                                f"Min sep {ev['min_dist_m']}m · Duration {ev['duration_min']} min"
                                if ev["min_dist_m"] is not None
                                else f"Loiter duration {ev['duration_min']} min"
                            ),
                            "links_json": json.dumps(links),
                            "first_seen": ev["first_seen"],
                            "last_seen": ev["last_seen"],
                        }
                    )

                if highlight_upserts:
                    psycopg2.extras.execute_batch(
                        cur,
                        """
                        INSERT INTO spotter_highlights (
                            domain, target_id, name_or_reg, secondary_id, category_key,
                            badge, score, reason, lat, lon, metric_summary, links_json,
                            first_seen, last_seen
                        ) VALUES (
                            %(domain)s, %(target_id)s, %(name_or_reg)s, %(secondary_id)s, %(category_key)s,
                            %(badge)s, %(score)s, %(reason)s, %(lat)s, %(lon)s, %(metric_summary)s, %(links_json)s,
                            %(first_seen)s, %(last_seen)s
                        )
                        ON CONFLICT (domain, target_id) DO UPDATE SET
                            name_or_reg = CASE WHEN EXCLUDED.score >= spotter_highlights.score THEN EXCLUDED.name_or_reg ELSE spotter_highlights.name_or_reg END,
                            secondary_id = CASE WHEN EXCLUDED.score >= spotter_highlights.score THEN EXCLUDED.secondary_id ELSE spotter_highlights.secondary_id END,
                            category_key = CASE WHEN EXCLUDED.score >= spotter_highlights.score THEN EXCLUDED.category_key ELSE spotter_highlights.category_key END,
                            badge = CASE WHEN EXCLUDED.score >= spotter_highlights.score THEN EXCLUDED.badge ELSE spotter_highlights.badge END,
                            score = GREATEST(spotter_highlights.score, EXCLUDED.score),
                            reason = CASE WHEN EXCLUDED.score >= spotter_highlights.score THEN EXCLUDED.reason ELSE spotter_highlights.reason END,
                            lat = COALESCE(EXCLUDED.lat, spotter_highlights.lat),
                            lon = COALESCE(EXCLUDED.lon, spotter_highlights.lon),
                            metric_summary = CASE WHEN EXCLUDED.score >= spotter_highlights.score THEN EXCLUDED.metric_summary ELSE spotter_highlights.metric_summary END,
                            links_json = CASE WHEN EXCLUDED.score >= spotter_highlights.score THEN EXCLUDED.links_json ELSE spotter_highlights.links_json END,
                            last_seen = GREATEST(spotter_highlights.last_seen, EXCLUDED.last_seen)
                        """,
                        highlight_upserts,
                        page_size=50,
                    )
        conn.commit()
    except Exception as e:
        print(f"[sts-detector] Query warning: {e}", flush=True)
    finally:
        conn.close()

    return get_sts_events(dsn)


def get_sts_events(dsn: str = PG_DSN) -> list[dict]:
    events = []
    try:
        conn = psycopg2.connect(dsn)
        init_sts_db(conn)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM ais_sts_events
                ORDER BY score DESC, last_seen DESC
                LIMIT 100
                """
            )
            for row in cur.fetchall():
                d = dict(row)
                d["first_seen_utc"] = d["first_seen"].strftime("%Y-%m-%d %H:%M UTC") if d.get("first_seen") else "—"
                d["last_seen_utc"] = d["last_seen"].strftime("%Y-%m-%d %H:%M UTC") if d.get("last_seen") else "—"
                d.pop("first_seen", None)
                d.pop("last_seen", None)
                events.append(d)
        conn.close()
    except Exception as e:
        print(f"[sts-detector] Read events error: {e}", flush=True)
    return events


def main():
    parser = argparse.ArgumentParser(description="Dark-Fleet Ship-to-Ship (STS) & Loitering Detector")
    parser.add_argument("--json", action="store_true", help="Output JSON results")
    args = parser.parse_args()

    events = run_sts_detection(force=True)
    if args.json:
        print(json.dumps({"count": len(events), "events": events}, indent=2))
        return

    print(f"=== AIS Ship-to-Ship (STS) & Loitering Report ({len(events)} events) ===")
    for i, ev in enumerate(events[:15], 1):
        print(f"  #{i} [{ev['badge']}] {ev['summary']} (Last seen: {ev['last_seen_utc']})")


if __name__ == "__main__":
    main()
