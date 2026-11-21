#!/usr/bin/env python3
"""
Dark-Fleet Ship-to-Ship (STS) Rendezvous, Offshore Loitering & AIS Dark-Gap Detector
tailored for high-density Singapore Strait / EOPL traffic (dbname=sdr_station).

Because Singapore anchorages and fairways have hundreds of domestic bunker barges,
tugs, and pilot boats in close proximity, this detector strictly filters out routine
harbour traffic and ONLY flags events involving:
  1. OpenSanctions / OFAC / EU / UK designated vessels (`sanctions_watchlist`)
  2. Russian (MID 273), Iranian (MID 422), DPRK (MID 445), Syrian (MID 468),
     or Venezuelan (MID 775) flagged vessels
  3. Shadow-Fleet / Dark-Fleet flag-hopping registries (Gabon 626, Cameroon 613,
     Comoros 616, Palau 511, Cook Is. 518, Eswatini 669, Djibouti 621, Sierra Leone 667,
     St. Kitts 341)
  4. Ocean-going foreign tankers (len >= 220m, non-Singapore MID) broadcasting
     obfuscated offshore orders ('FOR ORDERS', 'TO ORDER', 'OPL', 'EOPL', 'STS', 'TBN')
     or spoofing/suppressing static voyage data outside inner port berths.
"""

import argparse
import json
import math
import os
import sys
import time
from pathlib import Path
from urllib.parse import quote_plus

import psycopg2
import psycopg2.extras

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR / "sanctions-sync"))
try:
    from opensanctions_sync import match_vessel_sanctions
except Exception:
    def match_vessel_sanctions(mmsi=None, imo=None, shipname=None, dsn=None):
        return None

PG_DSN = os.environ.get("SDR_PG_DSN", "dbname=sdr_station")
_LAST_STS_SCAN = 0.0

SANCTIONED_MIDS = {"273", "422", "445", "468", "775"}
SHADOW_FLEET_MIDS = {"626", "613", "616", "511", "518", "669", "621", "667", "341"}
DOMESTIC_PORT_MIDS = {"563", "564", "565", "566"}

MID_NAMES = {
    "273": "Russia",
    "422": "Iran",
    "445": "DPRK",
    "468": "Syria",
    "775": "Venezuela",
    "626": "Gabon (Shadow-Fleet)",
    "613": "Cameroon (Shadow-Fleet)",
    "616": "Comoros (Shadow-Fleet)",
    "511": "Palau (Shadow-Fleet)",
    "518": "Cook Is. (Shadow-Fleet)",
    "669": "Eswatini (Shadow-Fleet)",
    "621": "Djibouti (Shadow-Fleet)",
    "667": "Sierra Leone (Shadow-Fleet)",
    "341": "St. Kitts (Shadow-Fleet)",
}

OBFUSCATED_DESTS = {
    "FOR ORDERS", "FOR ORDER", "TO ORDER", "ORDERS", "ORDER",
    "OPL", "EOPL", "WOPL", "STS", "TBN", "HIGH SEA", "HIGH SEAS", "SEA",
}

HARBOUR_CRAFT_NAME_TOKENS = (
    "PILOT", "SCDF", "TUG", "ASD ", "HARBOUR", "FERRY", "MPA ", "PCG ", "POLICE", "CRAFT",
)


def _is_harbour_service_craft(mmsi: int, name: str, shiptype: int, len_m: int) -> bool:
    name_up = (name or "").upper()
    if any(tok in name_up for tok in HARBOUR_CRAFT_NAME_TOKENS):
        return True
    if shiptype in (30, 31, 32, 33, 34, 35, 36, 37, 50, 51, 52, 53, 54, 55, 58) and len_m < 140:
        return True
    mid = str(mmsi)[:3]
    if mid in DOMESTIC_PORT_MIDS and len_m < 180:
        return True
    return False


def classify_vessel_suspicion(
    mmsi: int,
    imo: int | None,
    name: str,
    shiptype: int,
    len_m: int,
    dest: str,
    dsn: str = PG_DSN,
) -> tuple[int, str, str] | None:
    """
    Returns (suspicion_level, short_tag, detail_reason) if and only if a vessel
    is sanctioned, Russian/Iranian/DPRK, shadow-fleet registered, or a large foreign
    tanker/cargo vessel with dark-fleet obfuscated orders. Returns None for normal traffic.
    """
    mmsi_str = str(mmsi)
    mid = mmsi_str[:3]
    dest_up = (dest or "").strip().upper()
    name_clean = (name or "").strip()

    if _is_harbour_service_craft(mmsi, name_clean, shiptype, len_m):
        return None

    # 1. Live OpenSanctions Watchlist match
    os_hit = match_vessel_sanctions(mmsi_str, imo, name_clean, dsn=dsn)
    if os_hit:
        return (
            3,
            f"OpenSanctions ({os_hit['datasets'][:20]})",
            f"Designated on {os_hit['datasets']} (matched {os_hit['matched_on']})",
        )

    # 2. Sanctioned State Registry (Russia 273, Iran 422, DPRK 445, Syria 468, Venezuela 775)
    if mid in SANCTIONED_MIDS:
        country = MID_NAMES.get(mid, f"MID {mid}")
        return (
            3,
            f"{country} Flag (MID {mid})",
            f"{country}-flagged vessel (MMSI {mmsi_str})",
        )

    # 3. High-Risk Shadow-Fleet Registry
    if mid in SHADOW_FLEET_MIDS:
        country = MID_NAMES.get(mid, f"MID {mid}")
        return (
            2,
            f"{country}",
            f"High-risk shadow-fleet convenience registry ({country})",
        )

    # 4. Foreign Ocean-Going Tanker (len >= 220m, shiptype 80-89) with Obfuscated / EOPL Orders
    is_tanker = 80 <= (shiptype or 0) <= 89
    if is_tanker and len_m >= 220 and mid not in DOMESTIC_PORT_MIDS:
        if dest_up in OBFUSCATED_DESTS or any(k in dest_up for k in ("FOR ORDER", "OPL", "EOPL", "STS")):
            return (
                2,
                f"Dark-Fleet Orders ('{dest_up}')",
                f"Ocean-going tanker ({len_m}m) broadcasting obfuscated offshore orders '{dest_up}'",
            )

    return None


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


def run_sts_detection(dsn: str = PG_DSN, force: bool = False, purge_old: bool = False) -> list[dict]:
    """
    Scans TimescaleDB for STS transfers, offshore loitering, and dark-fleet activity
    strictly filtered to sanctioned, Russian/Iranian, shadow-fleet, or obfuscated-order tankers.
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
            if purge_old:
                cur.execute("TRUNCATE TABLE ais_sts_events;")
                cur.execute("DELETE FROM spotter_highlights WHERE secondary_id LIKE 'STS MMSI %' OR badge LIKE 'STS Tanker Transfer%';")

            # 1. Candidate STS Pairs (close proximity <= 250m at <= 2.5 kt for >= 15 min)
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
                     AND ABS(a.lat - b.lat) <= 0.0024
                     AND ABS(a.lon - b.lon) <= 0.0024
                    GROUP BY a.mmsi, b.mmsi
                    HAVING COUNT(*) >= 3
                )
                SELECT
                    p.*,
                    COALESCE(NULLIF(TRIM(sa.shipname), ''), 'MMSI ' || p.mmsi_a::text) AS name_a,
                    sa.imo AS imo_a,
                    COALESCE(sa.shiptype, 0) AS type_a,
                    (COALESCE(sa.to_bow, 0) + COALESCE(sa.to_stern, 0)) AS len_a,
                    COALESCE(NULLIF(TRIM(sa.destination), ''), '—') AS dest_a,
                    COALESCE(NULLIF(TRIM(sb.shipname), ''), 'MMSI ' || p.mmsi_b::text) AS name_b,
                    sb.imo AS imo_b,
                    COALESCE(sb.shiptype, 0) AS type_b,
                    (COALESCE(sb.to_bow, 0) + COALESCE(sb.to_stern, 0)) AS len_b,
                    COALESCE(NULLIF(TRIM(sb.destination), ''), '—') AS dest_b
                FROM pairs p
                LEFT JOIN ais_state sa ON sa.mmsi = p.mmsi_a
                LEFT JOIN ais_state sb ON sb.mmsi = p.mmsi_b
                WHERE p.min_dist_m <= 240.0
                ORDER BY p.buckets DESC, p.min_dist_m ASC
                LIMIT 150
                """
            )
            sts_rows = cur.fetchall()

            for r in sts_rows:
                if _is_harbour_service_craft(r["mmsi_a"], r["name_a"], r["type_a"], r["len_a"]):
                    continue
                if _is_harbour_service_craft(r["mmsi_b"], r["name_b"], r["type_b"], r["len_b"]):
                    continue

                susp_a = classify_vessel_suspicion(
                    r["mmsi_a"], r["imo_a"], r["name_a"], r["type_a"], r["len_a"], r["dest_a"], dsn=dsn
                )
                susp_b = classify_vessel_suspicion(
                    r["mmsi_b"], r["imo_b"], r["name_b"], r["type_b"], r["len_b"], r["dest_b"], dsn=dsn
                )

                if not susp_a and not susp_b:
                    continue

                primary_susp = susp_a if (susp_a and (not susp_b or susp_a[0] >= susp_b[0])) else susp_b
                if primary_susp is None:
                    continue
                lvl, tag, reason_note = primary_susp
                if susp_b and (not susp_a or susp_b[0] > susp_a[0]):
                    p_mmsi, p_name, p_len, p_dest = r["mmsi_b"], r["name_b"], r["len_b"], r["dest_b"]
                    s_mmsi, s_name, s_len, s_dest = r["mmsi_a"], r["name_a"], r["len_a"], r["dest_a"]
                else:
                    p_mmsi, p_name, p_len, p_dest = r["mmsi_a"], r["name_a"], r["len_a"], r["dest_a"]
                    s_mmsi, s_name, s_len, s_dest = r["mmsi_b"], r["name_b"], r["len_b"], r["dest_b"]

                dur_min = int(r["buckets"]) * 5
                dist_m = round(float(r["min_dist_m"] or 0.0), 1)
                score = 158 if lvl >= 3 else 148
                badge = f"Suspicious STS · {tag} ({dist_m}m · {dur_min}m)"
                summary = (
                    f"{p_name} ({p_len or '—'}m, {reason_note}) alongside {s_name} ({s_len or '—'}m) "
                    f"within {dist_m}m for {dur_min}+ min (dest: '{p_dest}' / '{s_dest}')."
                )
                ev_id = f"sts:{min(p_mmsi, s_mmsi)}:{max(p_mmsi, s_mmsi)}"
                detected_events.append(
                    {
                        "event_id": ev_id,
                        "event_type": "sts_transfer",
                        "primary_mmsi": p_mmsi,
                        "primary_name": p_name,
                        "secondary_mmsi": s_mmsi,
                        "secondary_name": s_name,
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
                )

            # 2. Suspicious Offshore Loitering / Drifting & Dark-Fleet Behavior
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
                        ROUND(AVG(COALESCE(speed, 0))::numeric, 1)::float AS avg_spd,
                        ROUND(COALESCE(MAX(course) - MIN(course), 0)::numeric, 1)::float AS course_spread
                    FROM ais_archive_ts
                    WHERE lat IS NOT NULL AND lon IS NOT NULL
                      AND (lat != 0 OR lon != 0)
                      AND COALESCE(speed, 0) <= 3.5
                    GROUP BY mmsi
                    HAVING COUNT(DISTINCT date_trunc('minute', received_at)) >= 6
                )
                SELECT
                    l.*,
                    COALESCE(NULLIF(TRIM(s.shipname), ''), 'MMSI ' || l.mmsi::text) AS shipname,
                    s.imo,
                    COALESCE(s.shiptype, 0) AS shiptype,
                    (COALESCE(s.to_bow, 0) + COALESCE(s.to_stern, 0)) AS len_m,
                    ROUND(s.draught::numeric, 1)::float AS draught,
                    COALESCE(NULLIF(TRIM(s.destination), ''), '—') AS destination
                FROM loiter l
                JOIN ais_state s ON s.mmsi = l.mmsi
                ORDER BY l.active_mins DESC
                LIMIT 250
                """
            )
            for r in cur.fetchall():
                susp = classify_vessel_suspicion(
                    r["mmsi"], r["imo"], r["shipname"], r["shiptype"], r["len_m"], r["destination"], dsn=dsn
                )
                if not susp:
                    continue
                lvl, tag, reason_note = susp
                if lvl < 3 and r["course_spread"] < 45.0 and r["avg_spd"] < 0.2:
                    continue

                dur_min = int(r["active_mins"])
                score = 154 if lvl >= 3 else 142
                badge = f"Suspicious Loitering · {tag}"
                summary = (
                    f"{r['shipname']} (MMSI {r['mmsi']}, {r['len_m'] or '—'}m, {r['draught'] or '—'}m draught) — "
                    f"{reason_note}; loitering/drifting at {r['avg_spd']} kt ({r['course_spread']}° heading change over {dur_min} min, dest: '{r['destination']}')."
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
    parser.add_argument("--purge", action="store_true", help="Purge previous events before scanning")
    args = parser.parse_args()

    events = run_sts_detection(force=True, purge_old=args.purge)
    if args.json:
        print(json.dumps({"count": len(events), "events": events}, indent=2))
        return

    print(f"=== Suspicious / Sanctioned AIS STS & Loitering Report ({len(events)} events) ===")
    for i, ev in enumerate(events[:15], 1):
        print(f"  #{i} [{ev['badge']}] {ev['summary']} (Last seen: {ev['last_seen_utc']})")


if __name__ == "__main__":
    main()
