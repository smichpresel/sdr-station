#!/usr/bin/env python3
"""
ADS-B Historical Data Analysis, Aircraft Database Enrichment & Spotter Highlight Engine
backed by PostgreSQL 17 + TimescaleDB (dbname=sdr_station).

Provides:
  - Summary metrics & TimescaleDB hypertable chunk statistics
  - Detection of Russian / Iranian / Sanctioned state & cargo aircraft,
    Military & Government VIP aircraft, four-engine heavy quadjets, and corporate jets
  - Direct public lookup links (ADS-B Exchange, OpenSanctions, Planespotters.net, FlightRadar24)
  - CSV export
"""

import argparse
import csv
import gzip
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote_plus

import psycopg2
import psycopg2.extras

PG_DSN = os.environ.get("SDR_PG_DSN", "dbname=sdr_station")
TAR1090_DB_DIR = Path(os.environ.get("TAR1090_DB_DIR", "/usr/local/share/tar1090/git-db/db"))
SECTORS = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]

_TAR1090_CACHE = {}

VIP_BIZJET_TYPES = {
    "GLF4", "GLF5", "GLF6", "GLEX", "GL5T", "GL7T", "CL60", "CL30", "CL35",
    "FA7X", "FA8X", "F900", "F2TH", "C680", "C68A", "C700", "C750", "E35L",
    "A318", "A319", "A310", "BBJ", "B737",
}
QUADJET_RARE_TYPES = {"B744", "B748", "A388", "A343", "A346", "AN12", "IL76", "AN124"}

MILITARY_AIRFRAME_TYPES = {
    "C17", "C130", "C30J", "A400", "C5M", "K35R", "KC46", "P8", "P3",
    "E3CF", "E3TF", "E737", "E2", "F15", "F16", "F18", "F35", "EUFI",
    "RAFL", "SU27", "SU30", "SU35", "MIG29", "TU95", "TU160", "IL78",
    "V22", "H60", "H47", "AH64", "C295", "CN35", "B703", "U2", "RQ4",
}

MILITARY_CALLSIGN_PREFIXES = {
    "RCH": "USAF Air Mobility Command (Reach)",
    "CNV": "US Navy Logistics (Convoy)",
    "RRR": "Royal Air Force (Ascot)",
    "ASCOT": "Royal Air Force Transport",
    "CFC": "Canadian Armed Forces",
    "GAF": "German Air Force (Luftwaffe)",
    "CTM": "French Air & Space Force (Cotam)",
    "IAM": "Italian Air Force",
    "RSAF": "Republic of Singapore Air Force",
    "RMAF": "Royal Malaysian Air Force",
    "TNI": "Indonesian Air Force (TNI-AU)",
    "ASY": "Royal Australian Air Force",
    "KIW": "Royal New Zealand Air Force",
    "SAM": "USAF Special Air Mission (VIP)",
    "EXEC": "Government Executive Flight",
    "NATO": "NATO Operational Flight",
}

RUSSIAN_IRANIAN_CALLSIGN_PREFIXES = {
    "RSD": ("Russian State VIP", "Rossiya Special Flight Squadron (Presidential / Government Fleet)"),
    "RFF": ("Russian Military", "Russian Aerospace Forces (VKS) state transport"),
    "TTF": ("Russian Military Cargo", "224th Flight Unit (Russian Ministry of Defense cargo)"),
    "VDA": ("Sanctioned Russian Cargo", "Volga-Dnepr Airlines heavy airlift fleet"),
    "AZS": ("Sanctioned Russian Cargo", "Aviacon Zitotrans IL-76 cargo fleet (OFAC designated)"),
    "AFL": ("Russian Flag Carrier", "Aeroflot Russian Airlines"),
    "SBI": ("Russian Carrier", "S7 Airlines (Siberia Airlines)"),
    "SVR": ("Russian Carrier", "Ural Airlines"),
    "IRM": ("Sanctioned Iranian Carrier", "Mahan Air (OFAC / EU sanctioned Iranian carrier)"),
    "IRA": ("Iranian Flag Carrier", "Iran Air state carrier"),
    "QFZ": ("Sanctioned Iranian Cargo", "Qeshm Fars Air (sanctioned B747 cargo operator)"),
    "IRC": ("Iranian Military-Linked", "Saha Airlines (IRIAF-affiliated carrier)"),
    "IRG": ("Iranian Military Cargo", "Pouya Air (IRGC-linked cargo operator)"),
}


def lookup_aircraft_db(hex_code: str) -> dict:
    """Resolve ICAO hex against tar1090-db gzipped JSON shards."""
    h = (hex_code or "").strip().upper()
    if not h or not TAR1090_DB_DIR.exists():
        return {"reg": None, "type_code": None, "db_flags": "00", "type_desc": None}

    for p_len in (4, 3, 2, 1):
        if len(h) <= p_len:
            continue
        prefix = h[:p_len]
        shard_path = TAR1090_DB_DIR / f"{prefix}.js"
        if shard_path.exists():
            if prefix not in _TAR1090_CACHE:
                try:
                    with gzip.open(shard_path, "rt", encoding="utf-8") as f:
                        _TAR1090_CACHE[prefix] = json.load(f)
                except Exception:
                    _TAR1090_CACHE[prefix] = {}
            rem = h[p_len:]
            entry = _TAR1090_CACHE[prefix].get(rem)
            if isinstance(entry, list) and len(entry) >= 4:
                return {
                    "reg": entry[0],
                    "type_code": entry[1],
                    "db_flags": str(entry[2] or "00"),
                    "type_desc": entry[3],
                }
    return {"reg": None, "type_code": None, "db_flags": "00", "type_desc": None}


def build_aircraft_links(hex_code: str, reg: str = None, callsign: str = None) -> list[dict]:
    """Generate public aviation & sanctions lookup URLs for an aircraft."""
    h_low = (hex_code or "").strip().lower()
    h_up = h_low.upper()
    q_term = reg if (reg and reg != "—") else h_up
    links = [
        {"label": "ADS-B Exchange", "url": f"https://globe.adsbexchange.com/?icao={h_low}"},
        {"label": "OpenSanctions", "url": f"https://www.opensanctions.org/search/?q={quote_plus(q_term)}"},
        {"label": "Planespotters", "url": f"https://www.planespotters.net/hex/{h_up}"},
    ]
    if reg and reg != "—":
        links.append({"label": f"FR24 ({reg})", "url": f"https://www.flightradar24.com/data/aircraft/{reg.lower()}"})
    elif callsign and callsign != "—":
        links.append({"label": f"FR24 ({callsign})", "url": f"https://www.flightradar24.com/data/flights/{callsign.lower()}"})
    return links


def _classify_icao_block(hex_code: str, reg: str) -> tuple[str | None, str | None]:
    """Detect Russian, Iranian, DPRK, or Belarus ICAO 24-bit address allocations or registration prefixes."""
    reg_up = (reg or "").upper().strip()
    if reg_up.startswith(("RA-", "RF-")):
        return ("Russian Registry", f"Russian Federation aircraft registration ({reg_up})")
    if reg_up.startswith("EP-"):
        return ("Iranian Registry", f"Islamic Republic of Iran aircraft registration ({reg_up})")
    if reg_up.startswith("P-"):
        return ("DPRK Registry", f"DPRK state aircraft registration ({reg_up})")
    if reg_up.startswith("EW-"):
        return ("Belarus Registry", f"Belarus aircraft registration ({reg_up})")

    try:
        val = int(hex_code, 16)
    except ValueError:
        return (None, None)

    if 0x100000 <= val <= 0x1FFFFF:
        return ("Russian ICAO Block", f"Russian Federation Mode-S allocation (ICAO {hex_code.upper()})")
    if 0x730000 <= val <= 0x737FFF:
        return ("Iranian ICAO Block", f"Iran Mode-S allocation (ICAO {hex_code.upper()})")
    if 0x720000 <= val <= 0x727FFF:
        return ("DPRK ICAO Block", f"DPRK Mode-S allocation (ICAO {hex_code.upper()})")
    if 0x510000 <= val <= 0x5103FF:
        return ("Belarus ICAO Block", f"Belarus Mode-S allocation (ICAO {hex_code.upper()})")
    return (None, None)


def score_interesting_aircraft(flight_row: dict, meta: dict) -> tuple[int, str, str, str]:
    """
    Returns (score, category_key, badge_text, reason_text) for spotter ranking.
    Prioritizes:
      1. Russian / Iranian / Sanctioned state & cargo aircraft (sanctioned_aviation)
      2. Military, Reconnaissance & Government VIP aircraft (military_gov)
      3. Heavy Quadjets & Cargo Airlifters (quadjet)
      4. Long-Range Corporate & VIP Bizjets (bizjet)
      5. Light General Aviation (light_ga) & Extreme Range (extreme_range)
    """
    hex_code = (flight_row.get("hex") or "").lower().strip()
    callsign = (flight_row.get("callsign") or "—").strip().upper()
    reg = meta.get("reg") or "—"
    tcode = (meta.get("type_code") or "").upper()
    tdesc = meta.get("type_desc") or tcode or "Unknown Aircraft"
    flags = meta.get("db_flags") or "00"
    max_km = flight_row.get("max_km") or 0.0
    max_gs = flight_row.get("max_gs") or 0.0
    min_alt = flight_row.get("min_alt")
    max_alt = flight_row.get("max_alt")

    # 1. Check Russian / Iranian / Sanctioned operators by callsign prefix
    cs_prefix3 = callsign[:3] if callsign != "—" else ""
    if cs_prefix3 in RUSSIAN_IRANIAN_CALLSIGN_PREFIXES:
        badge_lbl, op_desc = RUSSIAN_IRANIAN_CALLSIGN_PREFIXES[cs_prefix3]
        return (
            144,
            "sanctioned_aviation",
            f"{badge_lbl} · {tcode or 'ICAO ' + hex_code.upper()}",
            f"{tdesc} ({reg}, flight {callsign}) — {op_desc}, tracked at {min_alt or '—'}–{max_alt or '—'} ft ({max_gs or '—'} kt).",
        )

    # 2. Check Russian / Iranian / DPRK / Belarus ICAO hex block or registration
    block_badge, block_desc = _classify_icao_block(hex_code, reg)
    if block_badge:
        is_airlifter = tcode in ("IL76", "AN124", "AN12", "B744", "B748", "TU154", "TU204", "IL96")
        score_val = 142 if is_airlifter else 136
        return (
            score_val,
            "sanctioned_aviation",
            f"{block_badge} · {tcode or reg}",
            f"{tdesc} ({reg}, callsign {callsign}) — {block_desc}, tracked at {min_alt or '—'}–{max_alt or '—'} ft.",
        )

    # 3. Check Military / Reconnaissance / Government VIP (tar1090-db military bit, airframe, or callsign)
    try:
        flag_int = int(flags, 16)
    except ValueError:
        flag_int = 0
    is_db_mil = bool(flag_int & 1)
    mil_op = None
    for pref, desc in MILITARY_CALLSIGN_PREFIXES.items():
        if callsign.startswith(pref):
            mil_op = desc
            break

    if is_db_mil or (tcode in MILITARY_AIRFRAME_TYPES) or mil_op:
        role_label = mil_op or ("Military / Gov Registry" if is_db_mil else f"Military {tcode} Airframe")
        return (
            128,
            "military_gov",
            f"Military / Gov · {tcode or 'Mode-S'}",
            f"{tdesc} ({reg}, callsign {callsign}) — {role_label}, tracked at {min_alt or '—'}–{max_alt or '—'} ft ({max_gs or '—'} kt).",
        )

    # 4. Heavy Quadjets & Outsized Cargo (B744, B748, A388, IL76, AN12, AN124)
    if tcode in QUADJET_RARE_TYPES:
        priority = 110 if tcode in ("B748", "B744", "AN12", "IL76", "AN124") else 102
        badge = f"Outsized Cargo · {tcode}" if tcode in ("IL76", "AN12", "AN124") else f"Four-Engine Heavy · {tcode}"
        return (
            priority,
            "quadjet",
            badge,
            f"{tdesc} ({reg}, flight {callsign}) — four-engine heavy aircraft tracked at {min_alt or '—'}–{max_alt or '—'} ft.",
        )

    # 5. Long-Range Corporate & VIP Jets
    if tcode in VIP_BIZJET_TYPES and tcode not in ("A319", "B737", "A310"):
        return (
            96,
            "bizjet",
            f"Corporate Jet · {tcode}",
            f"{tdesc} ({reg}, callsign {callsign}) — long-range corporate jet tracked at {max_gs} kt.",
        )

    # 6. Light General Aviation / Turboprop
    if tcode in ("DA40", "C172", "PA28", "SR22", "AT72", "B350"):
        return (
            88,
            "light_ga",
            f"General Aviation · {tcode}",
            f"{tdesc} ({reg}) — low-altitude general aviation flight ({min_alt or '—'} ft, {max_gs or '—'} kt).",
        )

    if max_km >= 115.0:
        return (
            84 + int(min(max_km / 20.0, 10)),
            "extreme_range",
            f"Long-Range Reception ({max_km} km)",
            f"{tdesc} ({reg}, flight {callsign}) — received at {max_alt or '—'} ft ({max_gs} kt).",
        )

    return (
        int(max_km / 5.0),
        "standard",
        "Commercial Flight",
        f"{tdesc} ({reg}, flight {callsign}).",
    )


def compute_analytics(dsn: str = PG_DSN) -> dict:
    try:
        conn = psycopg2.connect(dsn)
    except Exception:
        return {
            "total_positions": 0,
            "unique_aircraft": 0,
            "total_flights": 0,
            "max_distance_km": None,
            "max_altitude_ft": None,
            "max_speed_kt": None,
            "avg_rssi_dbfs": None,
            "top_airlines": [],
            "recent_flights": [],
            "top_interesting_planes": [],
            "polar_coverage": {s: {"count": 0, "max_km": 0} for s in SECTORS},
            "altitude_bands": [],
        }

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                COUNT(*) AS total_positions,
                COUNT(DISTINCT hex) AS unique_aircraft,
                ROUND(MAX(distance_km)::numeric, 2)::float AS max_distance_km,
                MAX(alt_baro) AS max_altitude_ft,
                ROUND(MAX(gs)::numeric, 1)::float AS max_speed_kt,
                ROUND(AVG(rssi)::numeric, 1)::float AS avg_rssi_dbfs,
                MIN(ts) AS first_sample_utc,
                MAX(ts) AS last_sample_utc
            FROM adsb_positions
            """
        )
        summary = cur.fetchone()

        cur.execute("SELECT COUNT(*) AS cnt FROM adsb_flights")
        total_flights = cur.fetchone()["cnt"]

        cur.execute(
            """
            SELECT
                SUBSTR(TRIM(flight), 1, 3) AS prefix,
                COUNT(DISTINCT session_id) AS flights_count,
                ROUND(MAX(max_distance_km)::numeric, 1)::float AS max_km
            FROM adsb_flights
            WHERE flight IS NOT NULL AND LENGTH(TRIM(flight)) >= 3
            GROUP BY prefix
            ORDER BY flights_count DESC
            LIMIT 10
            """
        )
        airlines = cur.fetchall()

        cur.execute(
            """
            SELECT
                hex, COALESCE(flight, '—') AS callsign,
                min_alt, max_alt,
                ROUND(max_gs::numeric, 1)::float AS max_gs,
                ROUND(max_rssi::numeric, 1)::float AS best_rssi,
                ROUND(max_distance_km::numeric, 1)::float AS max_km,
                samples, last_seen
            FROM adsb_flights
            ORDER BY last_seen DESC
            """
        )
        all_flights_raw = cur.fetchall()

        polar = {s: {"count": 0, "max_km": 0.0} for s in SECTORS}
        cur.execute(
            """
            SELECT
                (CAST(((bearing_deg + 22.5) / 45.0) AS INT) % 8) AS octant,
                COUNT(*) AS samples,
                ROUND(MAX(distance_km)::numeric, 2)::float AS max_km
            FROM adsb_positions
            WHERE bearing_deg IS NOT NULL AND distance_km IS NOT NULL
            GROUP BY octant
            """
        )
        for r in cur.fetchall():
            idx = r["octant"]
            if idx is not None and 0 <= idx < 8:
                polar[SECTORS[idx]] = {"count": r["samples"], "max_km": r["max_km"]}

        cur.execute(
            """
            SELECT
                CASE
                    WHEN alt_baro < 5000 THEN '0 - 5,000 ft'
                    WHEN alt_baro < 15000 THEN '5,000 - 15,000 ft'
                    WHEN alt_baro < 30000 THEN '15,000 - 30,000 ft'
                    ELSE '30,000+ ft'
                END AS band,
                COUNT(*) AS samples,
                ROUND(AVG(rssi)::numeric, 1)::float AS avg_rssi,
                ROUND(AVG(distance_km)::numeric, 1)::float AS avg_km
            FROM adsb_positions
            WHERE alt_baro IS NOT NULL
            GROUP BY band
            ORDER BY MIN(alt_baro)
            """
        )
        alt_bands = cur.fetchall()

    conn.close()

    enriched_flights = []
    scored_candidates = []
    for r in all_flights_raw:
        d = dict(r)
        meta = lookup_aircraft_db(d["hex"])
        d["reg"] = meta["reg"] or "—"
        d["type_code"] = meta["type_code"] or "—"
        d["type_desc"] = meta["type_desc"] or "—"
        d["last_seen_utc"] = datetime.fromtimestamp(d["last_seen"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        d["links"] = build_aircraft_links(d["hex"], d["reg"], d["callsign"])
        enriched_flights.append(d)

        score, cat_key, badge, reason = score_interesting_aircraft(d, meta)
        scored_candidates.append(
            {
                "score": score,
                "cat_key": cat_key,
                "badge": badge,
                "reason": reason,
                "hex": d["hex"].upper(),
                "callsign": d["callsign"],
                "reg": d["reg"],
                "type_code": d["type_code"],
                "type_desc": d["type_desc"],
                "min_alt": d["min_alt"],
                "max_alt": d["max_alt"],
                "max_gs": d["max_gs"],
                "max_km": d["max_km"],
                "samples": d["samples"],
                "links": d["links"],
            }
        )

    scored_candidates.sort(key=lambda x: (x["score"], x["max_km"] or 0), reverse=True)
    top_planes = []
    used_cats = set()
    for cand in scored_candidates:
        if cand["cat_key"] not in used_cats:
            top_planes.append(cand)
            used_cats.add(cand["cat_key"])
        if len(top_planes) == 3:
            break
    if len(top_planes) < 3:
        for cand in scored_candidates:
            if cand not in top_planes:
                top_planes.append(cand)
            if len(top_planes) == 3:
                break

    return {
        "total_positions": summary["total_positions"] or 0,
        "unique_aircraft": summary["unique_aircraft"] or 0,
        "total_flights": total_flights or 0,
        "max_distance_km": summary["max_distance_km"],
        "max_altitude_ft": summary["max_altitude_ft"],
        "max_speed_kt": summary["max_speed_kt"],
        "avg_rssi_dbfs": summary["avg_rssi_dbfs"],
        "first_sample_utc": summary["first_sample_utc"].isoformat() if summary["first_sample_utc"] else None,
        "last_sample_utc": summary["last_sample_utc"].isoformat() if summary["last_sample_utc"] else None,
        "top_airlines": [dict(r) for r in airlines],
        "recent_flights": enriched_flights[:20],
        "top_interesting_planes": top_planes,
        "polar_coverage": polar,
        "altitude_bands": [dict(r) for r in alt_bands],
    }


def export_csv(out_path: str, dsn: str = PG_DSN):
    conn = psycopg2.connect(dsn)
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM adsb_positions ORDER BY ts DESC LIMIT 50000")
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    conn.close()
    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)
    print(f"Exported {len(rows)} positions from TimescaleDB to {out_path}")


def main():
    parser = argparse.ArgumentParser(description="ADS-B TimescaleDB Analytics & Export Tool")
    parser.add_argument("--json", action="store_true", help="Output JSON summary")
    parser.add_argument("--export-csv", metavar="FILE", help="Export positions hypertable to CSV")
    args = parser.parse_args()

    if args.export_csv:
        export_csv(args.export_csv)
        return

    data = compute_analytics()
    if args.json:
        print(json.dumps(data, indent=2))
        return

    print("=== SDR Station ADS-B TimescaleDB Report ===")
    print(f"Engine           : PostgreSQL 17 + TimescaleDB 2.19.3 ({PG_DSN})")
    print(f"Total Positions  : {data['total_positions']}")
    print(f"Unique Aircraft  : {data['unique_aircraft']}")
    print(f"Flight Sessions  : {data['total_flights']}")
    print(f"Max Range (km)   : {data['max_distance_km']}")
    print(f"Max Altitude (ft): {data['max_altitude_ft']}")
    print("\n--- Top 3 Notable Aircraft of the Day ---")
    for i, p in enumerate(data["top_interesting_planes"], 1):
        print(f"  #{i} [{p['badge']}] {p['callsign']} ({p['reg']} · {p['type_desc']}) — {p['reason']}")
        for lnk in p["links"]:
            print(f"      -> {lnk['label']}: {lnk['url']}")


if __name__ == "__main__":
    main()
