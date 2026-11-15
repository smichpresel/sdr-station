#!/usr/bin/env python3
"""
SDR Station — Web Launcher, Dual-Band Auto-Cycler, Combined Air/Sea Map & Notable Catch Portal.
Database Engine: PostgreSQL 17 + TimescaleDB 2.19 (dbname=sdr_station)

Features:
  1. Overview of installed SDR receivers with descriptions, frequencies, and ports
  2. Selectable ADS-B / Marine AIS Auto-Cycler (default 2 min each) + Manual Mode Switcher
  3. Interactive Combined Air & Sea Map (Leaflet) showing aircraft and vessels
     with callout markers for notable vessels and aircraft
  4. "Notable Catches of the Day" (Top 3 Aircraft & Top 3 Ships) + Persistent
     "Spotter Logbook" (spotter_highlights table in TimescaleDB)
  5. TimescaleDB Hypertable Analytics & CSV Export
"""

import csv
import io
import json
import math
import os
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR / "adsb-analytics"))
try:
    from adsb_analyze import (
        build_aircraft_links,
        compute_analytics as compute_adsb_analytics,
        lookup_aircraft_db,
        score_interesting_aircraft,
    )
except Exception:
    def compute_adsb_analytics():
        return {}
    def lookup_aircraft_db(h):
        return {"reg": None, "type_code": None, "db_flags": "00", "type_desc": None}
    def build_aircraft_links(h, reg=None, callsign=None):
        return []
    def score_interesting_aircraft(f, m):
        return (0, "standard", "Flight", "")

PORT = int(os.environ.get("LAUNCHER_PORT", "8080"))
PG_DSN = os.environ.get("SDR_PG_DSN", "dbname=sdr_station")
CYCLER_STATE_FILE = Path(os.environ.get("CYCLER_STATE_FILE", str(BASE_DIR / "data" / "cycler_state.json")))


def _parse_coord(env_key: str):
    raw = os.environ.get(env_key, "").strip()
    if not raw or raw.startswith("<"):
        return None
    try:
        return float(raw)
    except ValueError:
        return None


STATION_NAME = os.environ.get("STATION_NAME", "SDR Station")
STATION_LAT = _parse_coord("STATION_LAT")
STATION_LON = _parse_coord("STATION_LON")

EXCLUSIVE_MODES = {
    "adsb": {
        "id": "adsb",
        "service": "readsb.service",
        "title": "ADS-B Aircraft Radar",
        "badge": "1090 MHz · Mode-S",
        "icon": "ADS-B",
        "port": 8504,
        "path": "/tar1090/",
        "PortLabel": "Port 8504 / /tar1090 + TCP 30002/30003/30005",
        "stack": "readsb v3.16 + tar1090 + adsb-logger (TimescaleDB)",
        "description": (
            "Decodes 1090 MHz Mode-S and ADS-B aircraft transponders in real time. "
            "Serves the tar1090 aviation radar interface and logs flight positions into "
            "TimescaleDB hypertables."
        ),
    },
    "ais": {
        "id": "ais",
        "service": "ais-catcher.service",
        "title": "Marine AIS Ship Radar",
        "badge": "161.975 / 162.025 MHz",
        "icon": "AIS",
        "port": 8100,
        "path": "/",
        "PortLabel": "Port 8100 (Web Map) + TCP 10110 (NMEA)",
        "stack": "AIS-catcher v0.70 + TimescaleDB (libpq) + NMEA Server",
        "description": (
            "Dual-channel VHF marine AIS receiver tracking vessel positions and static voyage data. "
            "Streams decoded messages into PostgreSQL 17 / TimescaleDB with 14-day chunk retention."
        ),
    },
    "voice": {
        "id": "voice",
        "service": "sdr-voice-web.service",
        "title": "VHF / FM Voice Receiver",
        "badge": "88 – 163 MHz · AM / NFM / WFM",
        "icon": "Audio",
        "port": 8073,
        "path": "/",
        "PortLabel": "Port 8073 (Web Tuner & MP3 Stream)",
        "stack": "rtl_fm + FFmpeg MP3 Streamer + Scanner",
        "description": (
            "Browser-accessible AM, NFM, and WFM demodulator and frequency scanner streaming "
            "HTTP MP3 audio. Check local regulations before receiving or recording voice traffic."
        ),
    },
    "ism": {
        "id": "ism",
        "service": "rtl-433-web.service",
        "title": "433.92 MHz ISM Sensor Decoder",
        "badge": "433.92 MHz · OOK / FSK",
        "icon": "ISM",
        "port": 8433,
        "path": "/",
        "PortLabel": "Port 8433 (Sensor Feed & JSONL Logger)",
        "stack": "rtl_433 v25.02 + Web Telemetry Monitor",
        "description": (
            "Decodes wireless sensor protocols on the 433.92 MHz ISM band, including weather "
            "stations, tire pressure monitoring sensors (TPMS), and utility telemetry."
        ),
    },
    "rtltcp": {
        "id": "rtltcp",
        "service": "rtl-tcp.service",
        "title": "Remote Raw I/Q Server (rtl_tcp)",
        "badge": "24 – 1766 MHz · Raw I/Q",
        "icon": "I/Q",
        "port": 1234,
        "path": None,
        "PortLabel": "TCP Port 1234 (rtl_tcp protocol)",
        "stack": "rtl_tcp (0.0.0.0:1234)",
        "description": (
            "Streams raw I/Q samples over TCP port 1234 for desktop SDR clients (SDR++, Gqrx) "
            "and mobile SDR applications over LAN or Tailscale."
        ),
    },
    "soapy": {
        "id": "soapy",
        "service": "soapyremote-server.service",
        "title": "SoapySDR Remote Network Server",
        "badge": "24 – 1766 MHz · SoapyRemote",
        "icon": "Soapy",
        "port": 55132,
        "path": None,
        "PortLabel": "TCP/UDP Port 55132 (SoapySDRRemote)",
        "stack": "SoapySDRServer (0.8)",
        "description": (
            "Exposes the RTL-SDR v5 over the SoapySDRRemote network protocol for "
            "CubicSDR, GNU Radio, SatDump, and Python SoapySDR scripts."
        ),
    },
}

MMSI_MID_FLAGS = {
    "563": ("Singapore", "Singapore"),
    "564": ("Singapore", "Singapore"),
    "565": ("Singapore", "Singapore"),
    "566": ("Singapore", "Singapore"),
    "636": ("Liberia", "Liberia"),
    "538": ("Marshall Is.", "Marshall Islands"),
    "351": ("Panama", "Panama"),
    "352": ("Panama", "Panama"),
    "353": ("Panama", "Panama"),
    "354": ("Panama", "Panama"),
    "355": ("Panama", "Panama"),
    "356": ("Panama", "Panama"),
    "357": ("Panama", "Panama"),
    "370": ("Panama", "Panama"),
    "477": ("Hong Kong", "Hong Kong"),
    "412": ("China", "China"),
    "413": ("China", "China"),
    "414": ("China", "China"),
    "211": ("Germany", "Germany"),
    "218": ("Germany", "Germany"),
    "219": ("Denmark", "Denmark"),
    "220": ("Denmark", "Denmark"),
    "229": ("Malta", "Malta"),
    "248": ("Malta", "Malta"),
    "249": ("Malta", "Malta"),
    "256": ("Malta", "Malta"),
    "241": ("Greece", "Greece"),
    "311": ("Bahamas", "Bahamas"),
    "440": ("South Korea", "South Korea"),
    "525": ("Indonesia", "Indonesia"),
    "533": ("Malaysia", "Malaysia"),
}


def init_spotter_db():
    """Ensure the persistent spotter_highlights logbook table exists in TimescaleDB."""
    try:
        conn = psycopg2.connect(PG_DSN)
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS spotter_highlights (
                        domain TEXT NOT NULL,
                        target_id TEXT NOT NULL,
                        name_or_reg TEXT NOT NULL,
                        secondary_id TEXT,
                        category_key TEXT NOT NULL,
                        badge TEXT NOT NULL,
                        score INTEGER NOT NULL,
                        reason TEXT NOT NULL,
                        lat DOUBLE PRECISION,
                        lon DOUBLE PRECISION,
                        heading DOUBLE PRECISION,
                        dist_km DOUBLE PRECISION,
                        metric_summary TEXT,
                        links_json JSONB,
                        first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (domain, target_id)
                    );
                    CREATE INDEX IF NOT EXISTS idx_spotter_highlights_score
                        ON spotter_highlights (score DESC, last_seen DESC);
                    """
                )
        conn.close()
    except Exception as e:
        print(f"[sdr-launcher] DB init warning: {e}", flush=True)


_LAST_HIGHLIGHT_UPSERT = 0.0
_UPSERT_LOCK = threading.Lock()
_STATUS_CACHE = {"ts": 0.0, "data": None}
_MAP_CACHE = {"ts": 0.0, "data": None}


def upsert_spotter_highlights(entries: list[dict], force: bool = False):
    """Persist notable aircraft & ships into spotter_highlights (rate-limited)."""
    global _LAST_HIGHLIGHT_UPSERT
    if not entries:
        return
    now = time.time()
    if not force and (now - _LAST_HIGHLIGHT_UPSERT) < 45.0:
        return
    with _UPSERT_LOCK:
        if not force and (time.time() - _LAST_HIGHLIGHT_UPSERT) < 45.0:
            return
        _LAST_HIGHLIGHT_UPSERT = time.time()
        dedup = {}
        for e in entries:
            dedup[(e["domain"], e["target_id"])] = e
        sorted_entries = [dedup[k] for k in sorted(dedup.keys())]
        try:
            conn = psycopg2.connect(PG_DSN)
            conn.autocommit = True
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(
                    cur,
                    """
                    INSERT INTO spotter_highlights (
                        domain, target_id, name_or_reg, secondary_id, category_key,
                        badge, score, reason, lat, lon, heading, dist_km,
                        metric_summary, links_json, first_seen, last_seen
                    ) VALUES (
                        %(domain)s, %(target_id)s, %(name_or_reg)s, %(secondary_id)s, %(category_key)s,
                        %(badge)s, %(score)s, %(reason)s, %(lat)s, %(lon)s, %(heading)s, %(dist_km)s,
                        %(metric_summary)s, %(links_json)s, COALESCE(%(seen_ts)s, NOW()), COALESCE(%(seen_ts)s, NOW())
                    )
                    ON CONFLICT (domain, target_id) DO UPDATE SET
                        name_or_reg = CASE WHEN EXCLUDED.name_or_reg != '—' THEN EXCLUDED.name_or_reg ELSE spotter_highlights.name_or_reg END,
                        secondary_id = COALESCE(EXCLUDED.secondary_id, spotter_highlights.secondary_id),
                        category_key = CASE WHEN EXCLUDED.score >= spotter_highlights.score THEN EXCLUDED.category_key ELSE spotter_highlights.category_key END,
                        badge = CASE WHEN EXCLUDED.score >= spotter_highlights.score THEN EXCLUDED.badge ELSE spotter_highlights.badge END,
                        score = GREATEST(spotter_highlights.score, EXCLUDED.score),
                        reason = CASE WHEN EXCLUDED.score >= spotter_highlights.score THEN EXCLUDED.reason ELSE spotter_highlights.reason END,
                        lat = COALESCE(EXCLUDED.lat, spotter_highlights.lat),
                        lon = COALESCE(EXCLUDED.lon, spotter_highlights.lon),
                        heading = COALESCE(EXCLUDED.heading, spotter_highlights.heading),
                        dist_km = COALESCE(EXCLUDED.dist_km, spotter_highlights.dist_km),
                        metric_summary = COALESCE(EXCLUDED.metric_summary, spotter_highlights.metric_summary),
                        links_json = COALESCE(EXCLUDED.links_json, spotter_highlights.links_json),
                        last_seen = GREATEST(spotter_highlights.last_seen, EXCLUDED.last_seen)
                    """,
                    sorted_entries,
                    page_size=100,
                )
            conn.close()
        except Exception as e:
            print(f"[sdr-launcher] Highlight upsert warning: {e}", flush=True)


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return round(2 * r * math.asin(math.sqrt(a)), 2)


def run_cmd(cmd: list[str], timeout: float = 5.0) -> str:
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return res.stdout.strip()
    except Exception:
        return ""


def get_service_active(service: str) -> bool:
    return run_cmd(["systemctl", "is-active", service]) == "active"


def _switch_hardware_to(mode_id: str) -> dict:
    if mode_id not in EXCLUSIVE_MODES and mode_id != "off":
        return {"ok": False, "error": f"Unknown mode: {mode_id}"}

    to_stop = [m["service"] for m in EXCLUSIVE_MODES.values() if m["id"] != mode_id]
    subprocess.run(["sudo", "systemctl", "stop"] + to_stop, capture_output=True, timeout=15)
    time.sleep(0.4)

    if mode_id == "off":
        subprocess.run(
            ["sudo", "systemctl", "stop"] + [m["service"] for m in EXCLUSIVE_MODES.values()],
            capture_output=True,
            timeout=15,
        )
        return {"ok": True, "active_mode": "off"}

    target_svc = EXCLUSIVE_MODES[mode_id]["service"]
    if not get_service_active(target_svc):
        subprocess.run(["sudo", "systemctl", "start", target_svc], capture_output=True, timeout=15)
        time.sleep(0.6)
    return {"ok": True, "active_mode": mode_id, "is_active": get_service_active(target_svc)}


class DualBandCycler:
    def __init__(self):
        self.lock = threading.Lock()
        self.enabled = True
        self.interval_sec = 120
        self.current_target = "adsb"
        self.last_switch_ts = time.time()
        self.cycles_completed = 0
        self._load_state()
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()

    def _load_state(self):
        if CYCLER_STATE_FILE.exists():
            try:
                data = json.loads(CYCLER_STATE_FILE.read_text())
                self.enabled = bool(data.get("enabled", True))
                self.interval_sec = int(data.get("interval_sec", 120))
                self.current_target = data.get("current_target", "adsb")
            except Exception:
                pass

    def _save_state(self):
        try:
            CYCLER_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
            CYCLER_STATE_FILE.write_text(
                json.dumps(
                    {
                        "enabled": self.enabled,
                        "interval_sec": self.interval_sec,
                        "current_target": self.current_target,
                    },
                    indent=2,
                )
            )
        except Exception:
            pass

    def configure(self, enabled: bool, interval_sec: int = None) -> dict:
        with self.lock:
            if interval_sec is not None and int(interval_sec) >= 30:
                self.interval_sec = int(interval_sec)
            self.enabled = bool(enabled)
            self.last_switch_ts = time.time()
            self._save_state()
            target = self.current_target if self.enabled else None

        if target:
            _switch_hardware_to(target)
        return self.status()

    def pause_for_manual_mode(self):
        with self.lock:
            if self.enabled:
                self.enabled = False
                self._save_state()

    def status(self) -> dict:
        with self.lock:
            elapsed = time.time() - self.last_switch_ts
            remaining = max(0, int(self.interval_sec - elapsed)) if self.enabled else None
            next_mode = "ais" if self.current_target == "adsb" else "adsb"
            return {
                "enabled": self.enabled,
                "interval_sec": self.interval_sec,
                "current_target": self.current_target,
                "next_target": next_mode,
                "seconds_remaining": remaining,
                "cycles_completed": self.cycles_completed,
            }

    def _run_loop(self):
        time.sleep(1.5)
        with self.lock:
            init_en = self.enabled
            init_target = self.current_target
        if init_en:
            _switch_hardware_to(init_target)

        while True:
            time.sleep(1.0)
            do_switch = None
            with self.lock:
                if not self.enabled:
                    continue
                if time.time() - self.last_switch_ts >= self.interval_sec:
                    self.current_target = "ais" if self.current_target == "adsb" else "adsb"
                    self.last_switch_ts = time.time()
                    self.cycles_completed += 1
                    self._save_state()
                    do_switch = self.current_target
            if do_switch:
                print(f"[cycler] Switching SDR hardware to {do_switch.upper()} ({self.interval_sec}s cycle)", flush=True)
                _switch_hardware_to(do_switch)


init_spotter_db()
CYCLER = DualBandCycler()


def activate_mode(mode_id: str) -> dict:
    if mode_id == "cycle":
        return CYCLER.configure(enabled=True)
    CYCLER.pause_for_manual_mode()
    return _switch_hardware_to(mode_id)


def get_system_info() -> dict:
    lsusb_out = run_cmd(["lsusb"]) or run_cmd(["/usr/bin/lsusb"])
    usb_connected = ("0bda:2838" in lsusb_out) or Path("/dev/bus/usb").exists()

    temp_c = None
    try:
        raw_t = Path("/sys/class/thermal/thermal_zone0/temp").read_text().strip()
        temp_c = round(int(raw_t) / 1000.0, 1)
    except Exception:
        pass

    load_avg = os.getloadavg()
    disk = shutil.disk_usage("/")
    disk_used_pct = round((disk.used / disk.total) * 100, 1)
    disk_free_gb = round(disk.free / (1024**3), 1)

    lan_ip = run_cmd(["hostname", "-I"]).split()
    primary_lan = next((ip for ip in lan_ip if not ip.startswith("100.") and ":" not in ip), None)
    tailscale_ip = run_cmd(["tailscale", "ip", "-4"])
    tailscale_auth_url = None
    if not tailscale_ip:
        try:
            ts_json = json.loads(run_cmd(["tailscale", "status", "--json"]) or "{}")
            tailscale_auth_url = ts_json.get("AuthURL") or None
        except Exception:
            pass
    tailscale_status = "Connected" if tailscale_ip else "Click to Authenticate"

    active_mode = "off"
    modes_status = {}
    for mid, m in EXCLUSIVE_MODES.items():
        act = get_service_active(m["service"])
        modes_status[mid] = act
        if act:
            active_mode = mid

    return {
        "station_name": STATION_NAME,
        "usb_sdr": "Nooelec NESDR SMArt v5 (RTL2832U / R820T2)" if usb_connected else "Not detected on USB",
        "usb_connected": usb_connected,
        "cpu_temp_c": temp_c,
        "load_1m": round(load_avg[0], 2),
        "disk_used_pct": disk_used_pct,
        "disk_free_gb": disk_free_gb,
        "lan_connected": bool(primary_lan),
        "tailscale_connected": bool(tailscale_ip),
        "tailscale_auth_url": tailscale_auth_url,
        "tailscale_status": tailscale_status,
        "active_mode": active_mode,
        "modes_status": modes_status,
        "cycler": CYCLER.status(),
    }


def build_ship_links(mmsi: int, imo: int = None) -> list[dict]:
    """Generate public maritime lookup URLs for a vessel."""
    m = str(mmsi)
    return [
        {
            "label": "VesselFinder",
            "url": f"https://www.vesselfinder.com/vessels/details/{imo if imo else m}",
        },
        {
            "label": "MarineTraffic",
            "url": f"https://www.marinetraffic.com/en/ais/details/ships/mmsi:{m}",
        },
        {
            "label": "MyShipTracking",
            "url": f"https://www.myshiptracking.com/vessels/mmsi-{m}",
        },
    ]


def score_interesting_ship(v: dict) -> tuple[int, str, str, str]:
    mmsi_str = str(v.get("mmsi", ""))
    mid = mmsi_str[:3]
    name = (v.get("shipname") or "—").strip()
    dest = (v.get("destination") or "—").strip()
    length_m = int(v.get("len_m") or 0)
    beam_m = int(v.get("beam_m") or 0)
    draught = float(v.get("draught") or 0.0)
    shiptype = int(v.get("shiptype") or 0)
    flag_short, _ = MMSI_MID_FLAGS.get(mid, ("Vessel", f"MID {mid}"))

    if length_m >= 395:
        return (
            125 + (1 if draught >= 15.5 else 0),
            "megaship_400m",
            f"400m Ultra-Large Vessel ({length_m}m × {beam_m}m · {flag_short})",
            f"{name} (IMO {v.get('imo') or '—'}) — 400m-class ultra-large container vessel ({draught}m draught), dest: '{dest}'.",
        )

    if (80 <= shiptype <= 89 and draught >= 19.0) or (draught >= 20.0 and length_m >= 300):
        return (
            112,
            "vlcc_supertanker",
            f"Deep-Draught VLCC Tanker ({draught}m · {flag_short})",
            f"{name} ({length_m}m × {beam_m}m) — deep-draught tanker drawing {draught}m, dest: '{dest}'.",
        )

    if length_m >= 350:
        return (
            102,
            "ulcv_350m",
            f"350m+ Large Vessel ({length_m}m · {flag_short})",
            f"{name} (IMO {v.get('imo') or '—'}) — {length_m}m × {beam_m}m vessel ({draught}m draught), dest: '{dest}'.",
        )

    if shiptype in (51, 55) or (shiptype == 37 and length_m >= 50):
        badge_lbl = "Patrol Vessel" if shiptype == 55 else ("Search & Rescue" if shiptype == 51 else f"{length_m}m Yacht")
        return (
            94,
            "special_craft",
            f"{badge_lbl} ({flag_short})",
            f"{name} (MMSI {mmsi_str}) — special-purpose vessel ({length_m}m), dest: '{dest}'.",
        )

    return (
        min(80, int(length_m / 5)),
        "standard_vessel",
        f"Vessel ({length_m}m)",
        f"{name} — {length_m}m × {beam_m}m vessel, dest: '{dest}'.",
    )


def get_ais_summary() -> dict:
    try:
        conn = psycopg2.connect(PG_DSN)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT COALESCE(SUM(count), 0)::bigint AS total_msgs, COUNT(*) AS cnt, COUNT(*) FILTER (WHERE shipname IS NOT NULL AND TRIM(shipname) != '') AS named FROM ais_state")
            agg = cur.fetchone()
            msgs = agg["total_msgs"]
            vessels = agg["cnt"]
            named = agg["named"]

            cur.execute(
                """
                SELECT mmsi, COALESCE(NULLIF(TRIM(shipname), ''), '—') AS shipname,
                       COALESCE(NULLIF(TRIM(callsign), ''), '—') AS callsign,
                       imo, shiptype,
                       (COALESCE(to_bow, 0) + COALESCE(to_stern, 0)) AS len_m,
                       (COALESCE(to_port, 0) + COALESCE(to_starboard, 0)) AS beam_m,
                       ROUND(draught::numeric, 1)::float AS draught,
                       COALESCE(NULLIF(TRIM(destination), ''), '—') AS destination,
                       ROUND(speed::numeric, 1)::float AS speed,
                       ROUND(course::numeric, 1)::float AS course,
                       ROUND(lat::numeric, 5)::float AS lat,
                       ROUND(lon::numeric, 5)::float AS lon,
                       count, received_at
                FROM ais_state
                ORDER BY CASE WHEN shipname IS NOT NULL AND TRIM(shipname) != '' THEN 0 ELSE 1 END, received_at DESC NULLS LAST
                """
            )
            rows = cur.fetchall()

            cur.execute("SELECT pg_size_pretty(pg_database_size('sdr_station')) AS db_size")
            db_size = cur.fetchone()["db_size"]
        conn.close()

        all_vessels = []
        scored_ships = []
        to_persist = []
        for r in rows:
            d = dict(r)
            raw_ts = d.get("received_at")
            if raw_ts:
                d["received_at"] = raw_ts.isoformat()
            if (
                STATION_LAT is not None
                and STATION_LON is not None
                and isinstance(d.get("lat"), (int, float))
                and isinstance(d.get("lon"), (int, float))
                and (d["lat"] != 0 or d["lon"] != 0)
            ):
                d["dist_km"] = haversine_km(STATION_LAT, STATION_LON, d["lat"], d["lon"])
            else:
                d["dist_km"] = None
            d["links"] = build_ship_links(d["mmsi"], d.get("imo"))
            all_vessels.append(d)

            if d["shipname"] and d["shipname"] != "—":
                score, cat_key, badge, reason = score_interesting_ship(d)
                ship_entry = {
                    "score": score,
                    "cat_key": cat_key,
                    "badge": badge,
                    "reason": reason,
                    "mmsi": d["mmsi"],
                    "shipname": d["shipname"],
                    "imo": d["imo"],
                    "len_m": d["len_m"],
                    "beam_m": d["beam_m"],
                    "draught": d["draught"],
                    "destination": d["destination"],
                    "speed": d["speed"],
                    "course": d["course"],
                    "lat": d["lat"],
                    "lon": d["lon"],
                    "dist_km": d["dist_km"],
                    "links": d["links"],
                }
                scored_ships.append(ship_entry)
                if score >= 90:
                    to_persist.append(
                        {
                            "domain": "ship",
                            "target_id": str(d["mmsi"]),
                            "name_or_reg": d["shipname"],
                            "secondary_id": f"IMO {d['imo']}" if d.get("imo") else f"MMSI {d['mmsi']}",
                            "category_key": cat_key,
                            "badge": badge,
                            "score": score,
                            "reason": reason,
                            "lat": d["lat"],
                            "lon": d["lon"],
                            "heading": d["course"],
                            "dist_km": d["dist_km"],
                            "metric_summary": f"{d['len_m'] or '—'}m × {d['beam_m'] or '—'}m · {d['draught'] or '—'}m draught · Dest: {d['destination']}",
                            "links_json": json.dumps(d["links"]),
                            "seen_ts": raw_ts,
                        }
                    )

        if to_persist:
            upsert_spotter_highlights(to_persist)

        scored_ships.sort(key=lambda x: (x["score"], x["len_m"] or 0), reverse=True)
        top_ships = []
        used_cats = set()
        for cand in scored_ships:
            if cand["cat_key"] not in used_cats:
                top_ships.append(cand)
                used_cats.add(cand["cat_key"])
            if len(top_ships) == 3:
                break
        if len(top_ships) < 3:
            for cand in scored_ships:
                if cand not in top_ships:
                    top_ships.append(cand)
                if len(top_ships) == 3:
                    break

        return {
            "total_messages": msgs,
            "unique_vessels": vessels,
            "named_vessels": named,
            "timescaledb_size": db_size,
            "recent_vessels": all_vessels[:15],
            "top_interesting_ships": top_ships,
        }
    except Exception as e:
        return {
            "total_messages": 0,
            "unique_vessels": 0,
            "named_vessels": 0,
            "timescaledb_size": "N/A",
            "recent_vessels": [],
            "top_interesting_ships": [],
            "error": str(e),
        }


def get_combined_map_and_log() -> dict:
    """
    Returns combined aircraft + ship targets with coordinates for the unified map,
    plus the persistent spotter_highlights logbook.
    """
    aircraft_map = []
    ships_map = []
    highlights_log = []
    plane_to_persist = []

    try:
        conn = psycopg2.connect(PG_DSN)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # 1. Latest position per aircraft + flight summary
            cur.execute(
                """
                WITH latest_pos AS (
                    SELECT DISTINCT ON (hex)
                        hex,
                        COALESCE(NULLIF(TRIM(flight), ''), '—') AS pos_flight,
                        ROUND(lat::numeric, 5)::float AS lat,
                        ROUND(lon::numeric, 5)::float AS lon,
                        alt_baro,
                        ROUND(gs::numeric, 1)::float AS gs,
                        ROUND(track::numeric, 1)::float AS track,
                        ROUND(distance_km::numeric, 1)::float AS dist_km,
                        ts
                    FROM adsb_positions
                    WHERE lat IS NOT NULL AND lon IS NOT NULL
                    ORDER BY hex, ts DESC
                ),
                flight_agg AS (
                    SELECT
                        hex,
                        MAX(NULLIF(TRIM(flight), '')) AS callsign,
                        MIN(min_alt) AS min_alt,
                        MAX(max_alt) AS max_alt,
                        ROUND(MAX(max_gs)::numeric, 1)::float AS max_gs,
                        ROUND(MAX(max_distance_km)::numeric, 1)::float AS max_km,
                        SUM(samples) AS samples
                    FROM adsb_flights
                    GROUP BY hex
                )
                SELECT
                    p.hex,
                    COALESCE(f.callsign, NULLIF(p.pos_flight, '—'), '—') AS callsign,
                    p.lat, p.lon, p.alt_baro, p.gs, p.track, p.dist_km, p.ts,
                    COALESCE(f.min_alt, p.alt_baro) AS min_alt,
                    COALESCE(f.max_alt, p.alt_baro) AS max_alt,
                    COALESCE(f.max_gs, p.gs) AS max_gs,
                    COALESCE(f.max_km, p.dist_km) AS max_km,
                    COALESCE(f.samples, 1) AS samples
                FROM latest_pos p
                LEFT JOIN flight_agg f ON f.hex = p.hex
                ORDER BY p.ts DESC
                LIMIT 350
                """
            )
            ac_rows = cur.fetchall()

            for r in ac_rows:
                d = dict(r)
                meta = lookup_aircraft_db(d["hex"])
                d["reg"] = meta.get("reg") or "—"
                d["type_code"] = meta.get("type_code") or "—"
                d["type_desc"] = meta.get("type_desc") or d["type_code"] or "Aircraft"
                score, cat_key, badge, reason = score_interesting_aircraft(d, meta)
                links = build_aircraft_links(d["hex"], d["reg"], d["callsign"])
                is_int = score >= 84
                raw_ts = d.get("ts")
                aircraft_map.append(
                    {
                        "id": d["hex"].upper(),
                        "hex": d["hex"].upper(),
                        "callsign": d["callsign"],
                        "reg": d["reg"],
                        "type_code": d["type_code"],
                        "type_desc": d["type_desc"],
                        "lat": d["lat"],
                        "lon": d["lon"],
                        "alt_baro": d["alt_baro"],
                        "gs": d["gs"],
                        "track": d["track"] or 0,
                        "dist_km": d["dist_km"],
                        "max_km": d["max_km"],
                        "seen_utc": raw_ts.strftime("%H:%M:%S UTC") if raw_ts else "—",
                        "is_interesting": is_int,
                        "score": score,
                        "cat_key": cat_key,
                        "badge": badge,
                        "reason": reason,
                        "links": links,
                    }
                )
                if is_int:
                    label = d["callsign"] if d["callsign"] != "—" else d["hex"].upper()
                    if d["reg"] != "—":
                        label = f"{label} ({d['reg']})"
                    plane_to_persist.append(
                        {
                            "domain": "aircraft",
                            "target_id": d["hex"].upper(),
                            "name_or_reg": label,
                            "secondary_id": f"{d['type_desc']} · ICAO {d['hex'].upper()}",
                            "category_key": cat_key,
                            "badge": badge,
                            "score": score,
                            "reason": reason,
                            "lat": d["lat"],
                            "lon": d["lon"],
                            "heading": d["track"],
                            "dist_km": d["max_km"] or d["dist_km"],
                            "metric_summary": f"Alt {d['min_alt'] or '—'}–{d['max_alt'] or '—'} ft · {d['max_gs'] or '—'} kt",
                            "links_json": json.dumps(links),
                            "seen_ts": raw_ts,
                        }
                    )

            # 2. Ships with valid coordinates
            cur.execute(
                """
                SELECT mmsi, COALESCE(NULLIF(TRIM(shipname), ''), '—') AS shipname,
                       imo, shiptype,
                       (COALESCE(to_bow, 0) + COALESCE(to_stern, 0)) AS len_m,
                       (COALESCE(to_port, 0) + COALESCE(to_starboard, 0)) AS beam_m,
                       ROUND(draught::numeric, 1)::float AS draught,
                       COALESCE(NULLIF(TRIM(destination), ''), '—') AS destination,
                       ROUND(speed::numeric, 1)::float AS speed,
                       ROUND(course::numeric, 1)::float AS course,
                       ROUND(lat::numeric, 5)::float AS lat,
                       ROUND(lon::numeric, 5)::float AS lon,
                       received_at
                FROM ais_state
                WHERE lat IS NOT NULL AND lon IS NOT NULL
                  AND lat BETWEEN -85 AND 85 AND lon BETWEEN -180 AND 180
                  AND (lat != 0 OR lon != 0)
                ORDER BY
                    CASE WHEN shipname IS NOT NULL AND TRIM(shipname) != '' THEN 0 ELSE 1 END,
                    (COALESCE(to_bow, 0) + COALESCE(to_stern, 0)) DESC,
                    received_at DESC NULLS LAST
                LIMIT 600
                """
            )
            ship_rows = cur.fetchall()
            for r in ship_rows:
                d = dict(r)
                score, cat_key, badge, reason = score_interesting_ship(d)
                is_int = score >= 90 and d["shipname"] != "—"
                links = build_ship_links(d["mmsi"], d.get("imo"))
                dist_km = (
                    haversine_km(STATION_LAT, STATION_LON, d["lat"], d["lon"])
                    if (STATION_LAT is not None and STATION_LON is not None)
                    else None
                )
                raw_ts = d.get("received_at")
                ships_map.append(
                    {
                        "id": str(d["mmsi"]),
                        "mmsi": d["mmsi"],
                        "shipname": d["shipname"] if d["shipname"] != "—" else f"MMSI {d['mmsi']}",
                        "imo": d["imo"],
                        "shiptype": d["shiptype"],
                        "len_m": d["len_m"],
                        "beam_m": d["beam_m"],
                        "draught": d["draught"],
                        "destination": d["destination"],
                        "speed": d["speed"],
                        "course": d["course"] or 0,
                        "lat": d["lat"],
                        "lon": d["lon"],
                        "dist_km": dist_km,
                        "seen_utc": raw_ts.strftime("%H:%M:%S UTC") if raw_ts else "—",
                        "is_interesting": is_int,
                        "score": score,
                        "cat_key": cat_key,
                        "badge": badge,
                        "reason": reason,
                        "links": links,
                    }
                )

        conn.close()

        if plane_to_persist:
            upsert_spotter_highlights(plane_to_persist)

        # 3. Query the persistent Spotter Logbook table
        conn = psycopg2.connect(PG_DSN)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    domain, target_id, name_or_reg, secondary_id, category_key,
                    badge, score, reason, lat, lon, heading, dist_km,
                    metric_summary, links_json,
                    first_seen, last_seen
                FROM spotter_highlights
                WHERE category_key IN ('megaship_400m', 'vlcc_supertanker', 'ulcv_350m', 'special_craft', 'notable_registry', 'bizjet', 'quadjet', 'light_ga', 'extreme_range')
                ORDER BY score DESC, last_seen DESC
                LIMIT 150
                """
            )
            for row in cur.fetchall():
                item = dict(row)
                item["first_seen_utc"] = item["first_seen"].strftime("%Y-%m-%d %H:%M UTC") if item.get("first_seen") else "—"
                item["last_seen_utc"] = item["last_seen"].strftime("%Y-%m-%d %H:%M UTC") if item.get("last_seen") else "—"
                item.pop("first_seen", None)
                item.pop("last_seen", None)
                item["links"] = item.pop("links_json", None) or []
                if isinstance(item["links"], str):
                    try:
                        item["links"] = json.loads(item["links"])
                    except Exception:
                        item["links"] = []
                highlights_log.append(item)
        conn.close()

    except Exception as e:
        print(f"[sdr-launcher] Map/Log query error: {e}", flush=True)

    return {
        "station": {"name": STATION_NAME},
        "aircraft": aircraft_map,
        "ships": ships_map,
        "highlights_log": highlights_log,
    }


HTML_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SDR Station — Launcher, Combined Map & Telemetry Hub</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" crossorigin=""/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" crossorigin=""></script>
<style>
  :root {
    --bg: #080d17; --card: #111a2b; --card-hover: #162238; --border: #22324d;
    --text: #e2e8f0; --muted: #94a3b8; --accent: #38bdf8;
    --green: #22c55e; --amber: #f59e0b; --purple: #a855f7; --rose: #f43f5e;
  }
  * { box-sizing: border-box; }
  body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: var(--bg); color: var(--text); line-height: 1.45; }
  .wrap { max-width: 1260px; margin: 0 auto; padding: 22px; }
  header { display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 16px; padding-bottom: 18px; border-bottom: 1px solid var(--border); margin-bottom: 18px; }
  .brand h1 { margin: 0; font-size: 1.48rem; display: flex; align-items: center; gap: 10px; }
  .brand p { margin: 4px 0 0; color: var(--muted); font-size: 0.88rem; }
  .sys-pills { display: flex; flex-wrap: wrap; gap: 8px; }
  .pill { background: var(--card); border: 1px solid var(--border); padding: 6px 12px; border-radius: 999px; font-size: 0.79rem; color: var(--muted); display: flex; align-items: center; gap: 6px; }
  .pill b { color: var(--text); }
  .dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
  .dot.green { background: var(--green); box-shadow: 0 0 8px var(--green); }
  .dot.amber { background: var(--amber); box-shadow: 0 0 8px var(--amber); }

  .cycler-bar {
    background: linear-gradient(135deg, #0f223d, #131b2e);
    border: 1px solid #2b4c7e;
    border-radius: 14px;
    padding: 16px 20px;
    margin-bottom: 22px;
    display: flex;
    flex-wrap: wrap;
    justify-content: space-between;
    align-items: center;
    gap: 14px;
  }
  .cycler-bar.active-cycle {
    border-color: var(--accent);
    box-shadow: 0 0 0 1px rgba(56, 189, 248, 0.28);
  }
  .cycler-left { flex: 1; min-width: 280px; }
  .cycler-title { font-weight: 700; font-size: 0.98rem; color: #f8fafc; display: flex; align-items: center; gap: 8px; }
  .cycler-sub { font-size: 0.84rem; color: #cbd5e1; margin-top: 4px; }
  .cycler-controls { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
  select.cycle-select, input.search-input {
    background: #091220;
    color: var(--text);
    border: 1px solid #2b4c7e;
    padding: 8px 12px;
    border-radius: 8px;
    font-size: 0.83rem;
    font-weight: 600;
  }

  .section-title { font-size: 1.05rem; font-weight: 700; margin: 24px 0 12px; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 8px; color: #f8fafc; }

  .map-shell {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 14px;
    padding: 14px;
    margin-bottom: 24px;
  }
  .map-toolbar {
    display: flex;
    justify-content: space-between;
    align-items: center;
    flex-wrap: wrap;
    gap: 10px;
    margin-bottom: 12px;
  }
  .filter-group { display: flex; flex-wrap: wrap; gap: 6px; }
  .chip-btn {
    background: #0b1322;
    color: var(--muted);
    border: 1px solid var(--border);
    padding: 6px 12px;
    border-radius: 8px;
    font-size: 0.78rem;
    font-weight: 600;
    cursor: pointer;
    display: inline-flex;
    align-items: center;
    gap: 5px;
  }
  .chip-btn:hover { border-color: var(--accent); color: var(--text); }
  .chip-btn.active {
    background: rgba(56, 189, 248, 0.16);
    color: var(--accent);
    border-color: var(--accent);
  }
  .map-legend {
    display: flex;
    flex-wrap: wrap;
    gap: 12px;
    font-size: 0.76rem;
    color: var(--muted);
  }
  .map-legend span { display: inline-flex; align-items: center; gap: 5px; }
  #combinedMap {
    width: 100%;
    height: 480px;
    border-radius: 10px;
    border: 1px solid #1e2f4a;
    background: #070c14;
  }

  .leaflet-popup-content-wrapper, .leaflet-popup-tip {
    background: #0d1626 !important;
    color: #e2e8f0 !important;
    border: 1px solid #2b4c7e;
    box-shadow: 0 12px 28px rgba(0,0,0,0.75);
  }
  .map-tag {
    white-space: nowrap;
    font-family: "SF Mono", Menlo, monospace;
    font-size: 10.5px;
    font-weight: 700;
    padding: 2px 6px;
    border-radius: 5px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.75);
    pointer-events: auto;
  }
  .map-tag.ship-hi {
    background: rgba(245, 158, 11, 0.94);
    color: #090d16;
    border: 1px solid #fde68a;
  }
  .map-tag.plane-hi {
    background: rgba(244, 63, 94, 0.94);
    color: #ffffff;
    border: 1px solid #fecdd3;
  }

  .spotter-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 24px; }
  @media (max-width: 920px) { .spotter-grid { grid-template-columns: 1fr; } }
  .spotter-col { background: var(--card); border: 1px solid var(--border); border-radius: 14px; padding: 16px 18px; }
  .spotter-col h3 { margin: 0 0 12px; font-size: 0.98rem; display: flex; justify-content: space-between; align-items: center; }
  .highlight-card {
    background: #0b1322;
    border: 1px solid #1e3252;
    border-radius: 10px;
    padding: 12px 14px;
    margin-bottom: 10px;
  }
  .highlight-card:last-child { margin-bottom: 0; }
  .hl-top { display: flex; justify-content: space-between; align-items: center; gap: 8px; flex-wrap: wrap; margin-bottom: 5px; }
  .hl-badge {
    font-size: 0.72rem;
    font-weight: 700;
    padding: 2px 8px;
    border-radius: 6px;
    background: rgba(56, 189, 248, 0.14);
    color: var(--accent);
    border: 1px solid rgba(56, 189, 248, 0.35);
  }
  .hl-name { font-weight: 700; font-size: 0.95rem; color: #f8fafc; font-family: "SF Mono", Menlo, monospace; }
  .hl-reason { font-size: 0.82rem; color: #cbd5e1; line-height: 1.4; margin-bottom: 8px; }
  .hl-links { display: flex; flex-wrap: wrap; gap: 6px; }
  .hl-link {
    font-size: 0.72rem;
    font-weight: 600;
    color: #93c5fd;
    background: #102039;
    border: 1px solid #26436e;
    padding: 3px 9px;
    border-radius: 6px;
    text-decoration: none;
    cursor: pointer;
  }
  .hl-link:hover { border-color: var(--accent); color: #fff; }
  .hl-link.map-jump {
    background: rgba(245, 158, 11, 0.16);
    color: #fcd34d;
    border-color: rgba(245, 158, 11, 0.45);
  }

  .modes-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(350px, 1fr)); gap: 16px; }
  .mode-card { background: var(--card); border: 1px solid var(--border); border-radius: 14px; padding: 18px; display: flex; flex-direction: column; justify-content: space-between; transition: border-color 0.15s; }
  .mode-card.active { border-color: var(--green); box-shadow: 0 0 0 1px rgba(34, 197, 94, 0.35), 0 8px 24px rgba(0,0,0,0.35); }
  .mode-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 10px; margin-bottom: 8px; }
  .mode-title { font-size: 1.06rem; font-weight: 700; display: flex; align-items: center; gap: 8px; }
  .freq-badge { font-family: "SF Mono", Menlo, monospace; font-size: 0.73rem; background: #091220; color: var(--accent); border: 1px solid #1e3a5f; padding: 3px 8px; border-radius: 6px; white-space: nowrap; }
  .status-tag { font-size: 0.73rem; font-weight: 700; padding: 3px 9px; border-radius: 999px; display: inline-flex; align-items: center; gap: 5px; }
  .status-tag.on { background: rgba(34, 197, 94, 0.15); color: #4ade80; border: 1px solid rgba(34, 197, 94, 0.4); }
  .status-tag.off { background: rgba(148, 163, 184, 0.1); color: var(--muted); border: 1px solid var(--border); }
  .mode-desc { font-size: 0.84rem; color: #cbd5e1; margin: 8px 0 12px; }
  .mode-meta { font-size: 0.75rem; color: var(--muted); border-top: 1px solid var(--border); padding-top: 10px; margin-bottom: 14px; }
  .mode-meta div { margin-bottom: 3px; }
  .btn-row { display: flex; gap: 8px; flex-wrap: wrap; }
  .btn { padding: 9px 14px; border-radius: 9px; font-size: 0.84rem; font-weight: 600; cursor: pointer; text-decoration: none; border: 1px solid transparent; display: inline-flex; align-items: center; justify-content: center; gap: 6px; flex: 1; text-align: center; }
  .btn-activate { background: #1e293b; color: var(--text); border-color: #334155; }
  .btn-activate:hover { background: #334155; }
  .btn-active { background: rgba(34, 197, 94, 0.18); color: #4ade80; border-color: rgba(34, 197, 94, 0.4); cursor: default; }
  .btn-open { background: var(--accent); color: #051321; font-weight: 700; }
  .btn-open:hover { filter: brightness(1.08); }

  .analytics-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; margin-bottom: 16px; }
  .stat-box { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 14px 16px; }
  .stat-label { font-size: 0.75rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.05em; }
  .stat-val { font-size: 1.52rem; font-weight: 700; color: var(--text); margin-top: 4px; font-family: "SF Mono", Menlo, monospace; }
  .stat-sub { font-size: 0.75rem; color: var(--accent); margin-top: 2px; }

  .two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
  @media (max-width: 900px) { .two-col { grid-template-columns: 1fr; } }
  .panel { background: var(--card); border: 1px solid var(--border); border-radius: 14px; padding: 18px; overflow-x: auto; margin-bottom: 18px; }
  .panel h3 { margin: 0 0 12px; font-size: 0.98rem; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 8px; }
  table { width: 100%; border-collapse: collapse; font-size: 0.81rem; }
  th, td { text-align: left; padding: 7px 8px; border-bottom: 1px solid var(--border); vertical-align: middle; }
  th { color: var(--muted); font-weight: 600; white-space: nowrap; }
  td.mono { font-family: "SF Mono", Menlo, monospace; }
  .dl-link { font-size: 0.78rem; color: var(--accent); text-decoration: none; border: 1px solid var(--border); padding: 4px 10px; border-radius: 6px; background: #091220; }
  .dl-link:hover { border-color: var(--accent); }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div class="brand">
      <h1>SDR Radio Receiver Station</h1>
      <p>Raspberry Pi CM4 · Nooelec NESDR SMArt v5 · PostgreSQL 17 + TimescaleDB</p>
    </div>
    <div class="sys-pills">
      <div class="pill"><span class="dot green" id="usbDot"></span> <b id="usbName">Checking SDR...</b></div>
      <div class="pill">CPU: <b id="cpuTemp">--</b></div>
      <div class="pill">LAN: <b id="lanIp">--</b></div>
      <div class="pill">Tailscale: <b id="tsIp">--</b></div>
      <a class="pill" href="https://github.com/smichpresel/sdr-station/blob/main/doc/README.md" target="_blank" style="text-decoration:none;color:var(--accent)">Build Documentation</a>
    </div>
  </header>

  <div class="cycler-bar" id="cyclerBar">
    <div class="cycler-left">
      <div class="cycler-title">
        <span class="dot green" id="cyclerDot"></span>
        <span id="cyclerHeadline">Dual-Band Auto-Cycler (ADS-B 1090 MHz / Marine AIS 162 MHz)</span>
      </div>
      <div class="cycler-sub" id="cyclerSub">
        Alternates the RTL-SDR v5 between aircraft Mode-S reception and VHF marine AIS reception automatically.
      </div>
    </div>
    <div class="cycler-controls">
      <select class="cycle-select" id="cycleIntervalSelect" onchange="changeCycleInterval()">
        <option value="60">Cycle: 1 min each</option>
        <option value="120" selected>Cycle: 2 min each (Default)</option>
        <option value="180">Cycle: 3 min each</option>
        <option value="300">Cycle: 5 min each</option>
        <option value="600">Cycle: 10 min each</option>
      </select>
      <button class="btn btn-open" id="cyclerToggleBtn" style="flex:none" onclick="toggleCycler()">
        Toggle Auto-Cycler
      </button>
      <button class="btn btn-activate" style="flex:none" onclick="switchMode('off')">
        Standby
      </button>
    </div>
  </div>

  <!-- COMBINED AIR & SEA RADAR MAP -->
  <div class="section-title" id="mapSection">
    <span>Combined Air &amp; Sea Map — Aircraft &amp; Vessels</span>
    <span style="font-size:0.78rem;font-weight:400;color:var(--muted)" id="mapCountBanner">Loading radar targets...</span>
  </div>
  <div class="map-shell">
    <div class="map-toolbar">
      <div class="filter-group">
        <button class="chip-btn active" id="fltAll" onclick="setMapFilter('all')">All Air &amp; Sea</button>
        <button class="chip-btn" id="fltHighlights" onclick="setMapFilter('highlights')">Notable Highlights Only</button>
        <button class="chip-btn" id="fltShips" onclick="setMapFilter('ships')">Ships Only</button>
        <button class="chip-btn" id="fltPlanes" onclick="setMapFilter('planes')">Aircraft Only</button>
        <button class="chip-btn" onclick="fitMapHighlights()">Zoom to Highlights</button>
        <button class="chip-btn" onclick="resetMapView()">Reset View</button>
      </div>
      <div class="map-legend">
        <span><span class="dot" style="background:#f59e0b;box-shadow:0 0 8px #f59e0b"></span> Notable Ship</span>
        <span><span class="dot" style="background:#f43f5e;box-shadow:0 0 8px #f43f5e"></span> Notable Aircraft</span>
        <span><span class="dot" style="background:#22c55e"></span> Vessel (AIS)</span>
        <span><span class="dot" style="background:#38bdf8"></span> Aircraft (ADS-B)</span>
      </div>
    </div>
    <div id="combinedMap"></div>
  </div>

  <div class="section-title">
    <span>Notable Catches of the Day — Aircraft &amp; Vessels</span>
    <span style="font-size:0.78rem;font-weight:400;color:var(--muted)">Ranked from TimescaleDB (400m ULCS vessels, deep-draught tankers, corporate jets, four-engine widebodies)</span>
  </div>

  <div class="spotter-grid">
    <div class="spotter-col">
      <h3>
        <span>Top 3 Notable Aircraft Today</span>
        <span style="font-size:0.74rem;color:var(--muted)">1090 MHz Mode-S · Public Lookups</span>
      </h3>
      <div id="topPlanesContainer">Loading aircraft highlights...</div>
    </div>

    <div class="spotter-col">
      <h3>
        <span>Top 3 Notable Ships Today</span>
        <span style="font-size:0.74rem;color:var(--muted)">162 MHz AIS · Public Lookups</span>
      </h3>
      <div id="topShipsContainer">Loading vessel highlights...</div>
    </div>
  </div>

  <!-- PERSISTENT HISTORICAL SPOTTER LOGBOOK -->
  <div class="panel" id="logbookSection">
    <h3>
      <div>
        <span>Saved Spotter Logbook — Notable Ships &amp; Aircraft Recorded</span>
        <span style="font-size:0.76rem;font-weight:400;color:var(--muted);margin-left:8px" id="logbookCountLabel">(0 saved)</span>
      </div>
      <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center">
        <button class="chip-btn active" id="logFltAll" onclick="setLogFilter('all')">All</button>
        <button class="chip-btn" id="logFltShip" onclick="setLogFilter('ship')">Notable Ships</button>
        <button class="chip-btn" id="logFltAircraft" onclick="setLogFilter('aircraft')">Notable Aircraft</button>
        <button class="chip-btn" id="logFltNotable" onclick="setLogFilter('notable')">400m ULCS &amp; Quadjets</button>
        <input class="search-input" id="logSearchInput" type="text" placeholder="Search name, MMSI, reg, type..." oninput="renderLogbookTable()">
        <a class="dl-link" href="/api/highlights/export.csv">Export Logbook CSV</a>
      </div>
    </h3>
    <div style="max-height:390px;overflow-y:auto">
      <table>
        <thead>
          <tr>
            <th>Domain</th>
            <th>Vessel / Aircraft</th>
            <th>Classification</th>
            <th>Details &amp; Specifications</th>
            <th>Last Seen</th>
            <th>Map &amp; Public Lookups</th>
          </tr>
        </thead>
        <tbody id="logbookTableBody">
          <tr><td colspan="6">Loading saved spotter logbook...</td></tr>
        </tbody>
      </table>
    </div>
  </div>

  <div class="section-title">
    <span>Installed SDR Applications &amp; Manual Mode Switcher</span>
    <span style="font-size:0.8rem;font-weight:400;color:var(--muted)" id="activeBanner">Loading status...</span>
  </div>
  <div class="modes-grid" id="modesGrid"></div>

  <div class="section-title">
    <span>TimescaleDB Aviation &amp; Marine AIS Analytics</span>
    <div style="display:flex;gap:8px;flex-wrap:wrap">
      <a class="dl-link" href="/api/adsb/export.csv">Download ADS-B Positions CSV</a>
      <a class="dl-link" href="/api/ais/export.csv">Download AIS Vessels CSV</a>
    </div>
  </div>

  <div class="analytics-grid">
    <div class="stat-box">
      <div class="stat-label">ADS-B Hypertable Rows</div>
      <div class="stat-val" id="stAdsbPos">0</div>
      <div class="stat-sub" id="stAdsbAc">0 unique aircraft</div>
    </div>
    <div class="stat-box">
      <div class="stat-label">ADS-B Altitude &amp; Signal</div>
      <div class="stat-val" id="stAdsbRange">—</div>
      <div class="stat-sub" id="stAdsbAlt">Max alt: —</div>
    </div>
    <div class="stat-box">
      <div class="stat-label">Marine AIS Vessels Seen</div>
      <div class="stat-val" id="stAisVessels">0</div>
      <div class="stat-sub" id="stAisNamed">0 with static ship name</div>
    </div>
    <div class="stat-box">
      <div class="stat-label">TimescaleDB Hypertables</div>
      <div class="stat-val" id="stAisMsgs">0</div>
      <div class="stat-sub" id="stDbSize">PostgreSQL 17 + TimescaleDB 2.19</div>
    </div>
  </div>

  <div class="two-col">
    <div class="panel">
      <h3>
        <span>Recent Aircraft Logged (TimescaleDB)</span>
        <span style="font-size:0.75rem;color:var(--muted)">adsb_flights</span>
      </h3>
      <table>
        <thead>
          <tr><th>ICAO</th><th>Callsign</th><th>Reg / Type</th><th>Alt (ft)</th><th>Samples</th><th>Lookup</th></tr>
        </thead>
        <tbody id="adsbTableBody">
          <tr><td colspan="6">Loading aircraft history...</td></tr>
        </tbody>
      </table>
    </div>

    <div class="panel">
      <h3>
        <span>Recent Vessels Logged (TimescaleDB)</span>
        <span style="font-size:0.75rem;color:var(--muted)">ais_state</span>
      </h3>
      <table>
        <thead>
          <tr><th>MMSI</th><th>Vessel Name</th><th>Size</th><th>Destination</th><th>Speed</th><th>Lookup</th></tr>
        </thead>
        <tbody id="aisTableBody">
          <tr><td colspan="6">Loading vessel history...</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</div>

<script>
const host = window.location.hostname;
let modesMeta = {};
let cyclerState = {enabled: false, interval_sec: 120};
let switching = false;

let map = null;
let mapLayerGroup = null;
let markerIndex = {};
let initialBoundsFitted = false;
let cachedMapData = {station: {}, aircraft: [], ships: [], highlights_log: []};
let currentMapFilter = 'all';
let currentLogFilter = 'all';

function initMap() {
  if (map) return;
  map = L.map('combinedMap').setView([1.22, 103.80], 10);
  L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '&copy; OpenStreetMap contributors',
    maxZoom: 18
  }).addTo(map);
  mapLayerGroup = L.layerGroup().addTo(map);
}

function buildPopupHtml(domain, item) {
  const linksHtml = (item.links || []).map(l =>
    `<a class="hl-link" href="${l.url}" target="_blank" rel="noopener">${l.label}</a>`
  ).join(' ');
  if (domain === 'ship') {
    return `
      <div style="min-width:240px;font-family:sans-serif">
        <div style="font-weight:700;font-size:0.95rem;color:#f8fafc;margin-bottom:3px">${item.shipname}</div>
        <div style="margin-bottom:6px"><span class="hl-badge">${item.badge}</span></div>
        <div style="font-size:0.8rem;color:#cbd5e1;margin-bottom:6px">${item.reason}</div>
        <div style="font-size:0.76rem;color:#94a3b8;margin-bottom:8px;font-family:monospace">
          MMSI: ${item.mmsi} ${item.imo ? '· IMO: ' + item.imo : ''}<br>
          Hull: ${item.len_m || '—'}m × ${item.beam_m || '—'}m · Draught: ${item.draught || '—'}m<br>
          Speed: ${item.speed ?? '—'} kt · Course: ${item.course ?? '—'}°
        </div>
        <div style="display:flex;gap:5px;flex-wrap:wrap">${linksHtml}</div>
      </div>
    `;
  } else {
    const title = item.callsign && item.callsign !== '—' ? `${item.callsign} (${item.reg})` : `${item.hex} (${item.reg})`;
    return `
      <div style="min-width:240px;font-family:sans-serif">
        <div style="font-weight:700;font-size:0.95rem;color:#f8fafc;margin-bottom:3px">${title}</div>
        <div style="margin-bottom:6px"><span class="hl-badge">${item.badge}</span></div>
        <div style="font-size:0.8rem;color:#cbd5e1;margin-bottom:6px">${item.reason}</div>
        <div style="font-size:0.76rem;color:#94a3b8;margin-bottom:8px;font-family:monospace">
          ICAO: ${item.hex} · Type: ${item.type_desc}<br>
          Alt: ${item.alt_baro ?? '—'} ft · Speed: ${item.gs ?? '—'} kt · Track: ${item.track ?? '—'}°
        </div>
        <div style="display:flex;gap:5px;flex-wrap:wrap">${linksHtml}</div>
      </div>
    `;
  }
}

function renderCombinedMap() {
  initMap();
  mapLayerGroup.clearLayers();
  markerIndex = {};

  const ships = cachedMapData.ships || [];
  const planes = cachedMapData.aircraft || [];
  let shownShips = 0, shownPlanes = 0, hiCount = 0;
  const allPts = [];

  const topShipIds = new Set(
    ships.filter(s => s.is_interesting).sort((a, b) => b.score - a.score).slice(0, 5).map(s => s.id)
  );
  if (currentMapFilter === 'all' || currentMapFilter === 'ships' || currentMapFilter === 'highlights') {
    ships.forEach(s => {
      if (currentMapFilter === 'highlights' && !s.is_interesting) return;
      shownShips++;
      if (s.is_interesting) hiCount++;
      allPts.push([s.lat, s.lon]);

      let marker;
      if (s.is_interesting) {
        const showLabel = topShipIds.has(s.id);
        marker = L.circleMarker([s.lat, s.lon], {
          radius: 7,
          color: '#fef08a',
          weight: 2,
          fillColor: '#f59e0b',
          fillOpacity: 0.95
        });
        if (showLabel) {
          marker.bindTooltip(`${s.shipname}`, {
            permanent: true,
            direction: 'right',
            offset: [8, 0],
            className: 'map-tag ship-hi'
          });
        } else {
          marker.bindTooltip(`<b>${s.shipname}</b><br>${s.badge}`, {direction: 'top'});
        }
      } else {
        marker = L.circleMarker([s.lat, s.lon], {
          radius: 3.8,
          color: '#052e16',
          weight: 1,
          fillColor: '#22c55e',
          fillOpacity: 0.78
        });
        marker.bindTooltip(`${s.shipname} (${s.len_m || '—'}m)`, {direction: 'top'});
      }
      marker.bindPopup(buildPopupHtml('ship', s));
      marker.addTo(mapLayerGroup);
      markerIndex['ship:' + s.id] = marker;
    });
  }

  const topPlaneIds = new Set(
    planes.filter(p => p.is_interesting).sort((a, b) => b.score - a.score).slice(0, 5).map(p => p.id)
  );
  if (currentMapFilter === 'all' || currentMapFilter === 'planes' || currentMapFilter === 'highlights') {
    planes.forEach(p => {
      if (currentMapFilter === 'highlights' && !p.is_interesting) return;
      shownPlanes++;
      if (p.is_interesting) hiCount++;
      allPts.push([p.lat, p.lon]);

      let marker;
      const lbl = p.callsign !== '—' ? p.callsign : (p.reg !== '—' ? p.reg : p.hex);
      if (p.is_interesting) {
        const showLabel = topPlaneIds.has(p.id);
        marker = L.circleMarker([p.lat, p.lon], {
          radius: 7,
          color: '#fecdd3',
          weight: 2,
          fillColor: '#f43f5e',
          fillOpacity: 0.95
        });
        if (showLabel) {
          marker.bindTooltip(`${lbl}`, {
            permanent: true,
            direction: 'right',
            offset: [8, 0],
            className: 'map-tag plane-hi'
          });
        } else {
          marker.bindTooltip(`<b>${lbl} (${p.reg})</b><br>${p.badge}`, {direction: 'top'});
        }
      } else {
        marker = L.circleMarker([p.lat, p.lon], {
          radius: 4.2,
          color: '#082f49',
          weight: 1,
          fillColor: '#38bdf8',
          fillOpacity: 0.85
        });
        marker.bindTooltip(`${lbl} (${p.type_code || p.hex})`, {direction: 'top'});
      }
      marker.bindPopup(buildPopupHtml('aircraft', p));
      marker.addTo(mapLayerGroup);
      markerIndex['aircraft:' + p.id] = marker;
    });
  }

  document.getElementById('mapCountBanner').textContent =
    `Showing ${shownPlanes} aircraft + ${shownShips} vessels (${hiCount} notable catches highlighted)`;
}

function setMapFilter(flt) {
  currentMapFilter = flt;
  ['All', 'Highlights', 'Ships', 'Planes'].forEach(k => {
    const el = document.getElementById('flt' + k);
    if (el) el.classList.toggle('active', k.toLowerCase() === flt);
  });
  renderCombinedMap();
}

function resetMapView() {
  fitMapHighlights();
}

function fitMapHighlights() {
  if (!map) return;
  const pts = [];
  (cachedMapData.ships || []).forEach(s => { if (s.is_interesting) pts.push([s.lat, s.lon]); });
  (cachedMapData.aircraft || []).forEach(p => { if (p.is_interesting) pts.push([p.lat, p.lon]); });
  if (pts.length > 0) {
    map.fitBounds(pts, {padding: [45, 45], maxZoom: 11});
  }
}

function locateOnMap(domain, targetId, lat, lon) {
  document.getElementById('mapSection').scrollIntoView({behavior: 'smooth', block: 'start'});
  if (currentMapFilter !== 'all') {
    setMapFilter('all');
  }
  const key = domain + ':' + targetId;
  const m = markerIndex[key];
  if (m && map) {
    const ll = m.getLatLng();
    map.setView(ll, 12, {animate: true});
    setTimeout(() => m.openPopup(), 300);
  } else if (lat && lon && map) {
    map.setView([lat, lon], 12, {animate: true});
  }
}

function setLogFilter(flt) {
  currentLogFilter = flt;
  document.getElementById('logFltAll').classList.toggle('active', flt === 'all');
  document.getElementById('logFltShip').classList.toggle('active', flt === 'ship');
  document.getElementById('logFltAircraft').classList.toggle('active', flt === 'aircraft');
  document.getElementById('logFltNotable').classList.toggle('active', flt === 'notable');
  renderLogbookTable();
}

function renderLogbookTable() {
  const rows = cachedMapData.highlights_log || [];
  const q = (document.getElementById('logSearchInput').value || '').trim().toLowerCase();
  const filtered = rows.filter(r => {
    if (currentLogFilter === 'ship' && r.domain !== 'ship') return false;
    if (currentLogFilter === 'aircraft' && r.domain !== 'aircraft') return false;
    if (currentLogFilter === 'notable' && !['megaship_400m', 'quadjet', 'bizjet', 'vlcc_supertanker'].includes(r.category_key)) return false;
    if (q) {
      const hay = `${r.name_or_reg} ${r.target_id} ${r.secondary_id} ${r.badge} ${r.reason}`.toLowerCase();
      if (!hay.includes(q)) return false;
    }
    return true;
  });

  document.getElementById('logbookCountLabel').textContent = `(${filtered.length} of ${rows.length} notable aircraft & vessels saved in TimescaleDB)`;
  const tbody = document.getElementById('logbookTableBody');
  if (filtered.length === 0) {
    tbody.innerHTML = '<tr><td colspan="6">No matching entries in logbook.</td></tr>';
    return;
  }

  tbody.innerHTML = filtered.map(r => {
    const icon = r.domain === 'ship' ? 'Ship' : 'Aircraft';
    const mapBtn = (r.lat && r.lon)
      ? `<button class="hl-link map-jump" onclick="locateOnMap('${r.domain}', '${r.target_id}', ${r.lat}, ${r.lon})">Map</button>`
      : '';
    const pubLinks = (r.links || []).slice(0, 2).map(l =>
      `<a class="hl-link" href="${l.url}" target="_blank" rel="noopener">${l.label}</a>`
    ).join(' ');
    return `
      <tr>
        <td class="mono">${icon}</td>
        <td>
          <b>${r.name_or_reg}</b>
          <div style="font-size:0.72rem;color:var(--muted)" class="mono">${r.secondary_id || r.target_id}</div>
        </td>
        <td><span class="hl-badge">${r.badge}</span></td>
        <td>
          <div style="font-size:0.8rem;color:#e2e8f0">${r.reason}</div>
          <div style="font-size:0.72rem;color:var(--muted)" class="mono">${r.metric_summary || ''}</div>
        </td>
        <td class="mono" style="font-size:0.74rem;color:var(--muted)">${r.last_seen_utc}</td>
        <td><div style="display:flex;gap:5px;flex-wrap:wrap">${mapBtn} ${pubLinks}</div></td>
      </tr>
    `;
  }).join('');
}

function buildAppUrl(m) {
  if (!m.path) return null;
  if (m.id === 'adsb') {
    return 'http://' + host + '/tar1090/';
  }
  return 'http://' + host + ':' + m.port + m.path;
}

async function fetchStatus() {
  const res = await fetch('/api/status');
  const data = await res.json();
  modesMeta = data.modes;
  const sys = data.system;
  cyclerState = sys.cycler || {enabled: false, interval_sec: 120};

  document.getElementById('usbName').textContent = sys.usb_connected ? 'Nooelec NESDR v5 Ready' : 'SDR Unplugged';
  document.getElementById('usbDot').className = 'dot ' + (sys.usb_connected ? 'green' : 'amber');
  document.getElementById('cpuTemp').textContent = sys.cpu_temp_c ? sys.cpu_temp_c + ' °C' : 'N/A';
  document.getElementById('lanIp').textContent = sys.lan_connected ? 'Connected' : 'Offline';
  const tsEl = document.getElementById('tsIp');
  if (sys.tailscale_connected) {
    tsEl.textContent = 'Connected';
  } else if (sys.tailscale_auth_url) {
    tsEl.innerHTML = `<a href="${sys.tailscale_auth_url}" target="_blank" style="color:var(--accent)">Authenticate Tailscale</a>`;
  } else {
    tsEl.textContent = sys.tailscale_status;
  }

  renderCycler(cyclerState, sys.active_mode);

  const activeName = sys.active_mode !== 'off' && modesMeta[sys.active_mode]
    ? modesMeta[sys.active_mode].title
    : 'Standby (Tuner Free)';
  document.getElementById('activeBanner').textContent = 'Hardware Tuner Assigned To: ' + activeName;

  renderModes(modesMeta, sys.modes_status);
  renderHighlights(data.adsb, data.ais);
  renderAnalytics(data.adsb, data.ais);
}

async function fetchMapAndLog() {
  try {
    const res = await fetch('/api/map');
    cachedMapData = await res.json();
    renderCombinedMap();
    renderLogbookTable();
  } catch (e) {
    console.error('Map fetch error:', e);
  }
}

function renderCycler(cyc, activeMode) {
  const bar = document.getElementById('cyclerBar');
  const dot = document.getElementById('cyclerDot');
  const head = document.getElementById('cyclerHeadline');
  const sub = document.getElementById('cyclerSub');
  const btn = document.getElementById('cyclerToggleBtn');
  const sel = document.getElementById('cycleIntervalSelect');

  if ([60, 120, 180, 300, 600].includes(cyc.interval_sec)) {
    sel.value = String(cyc.interval_sec);
  }

  const mins = Math.round((cyc.interval_sec || 120) / 60);
  if (cyc.enabled) {
    bar.classList.add('active-cycle');
    dot.className = 'dot green';
    head.textContent = `Auto-Cycling ADS-B / Marine AIS (${mins} min each · Active)`;
    sub.innerHTML = `Currently capturing: <b>${(activeMode || cyc.current_target).toUpperCase()}</b> · Next switch to <b>${(cyc.next_target || '').toUpperCase()}</b> in <b>${cyc.seconds_remaining ?? '--'}s</b> (Completed cycles: ${cyc.cycles_completed || 0})`;
    btn.textContent = 'Pause Auto-Cycler';
    btn.className = 'btn btn-activate';
  } else {
    bar.classList.remove('active-cycle');
    dot.className = 'dot amber';
    head.textContent = `Dual-Band Auto-Cycler (Paused — Single Mode Locked: ${activeMode.toUpperCase()})`;
    sub.textContent = `Enable to automatically alternate between ADS-B (1090 MHz) and Marine AIS (162 MHz) every ${mins} minutes.`;
    btn.textContent = 'Start ADS-B / AIS Auto-Cycle';
    btn.className = 'btn btn-open';
  }
}

async function toggleCycler() {
  const interval = parseInt(document.getElementById('cycleIntervalSelect').value || '120', 10);
  await fetch('/api/cycler', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({enabled: !cyclerState.enabled, interval_sec: interval})
  });
  await fetchStatus();
}

async function changeCycleInterval() {
  const interval = parseInt(document.getElementById('cycleIntervalSelect').value || '120', 10);
  await fetch('/api/cycler', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({enabled: true, interval_sec: interval})
  });
  await fetchStatus();
}

function renderHighlights(adsb, ais) {
  const planesBox = document.getElementById('topPlanesContainer');
  const planes = adsb.top_interesting_planes || [];
  if (planes.length === 0) {
    planesBox.innerHTML = '<div class="highlight-card">No aircraft highlights yet.</div>';
  } else {
    planesBox.innerHTML = planes.map((p, i) => `
      <div class="highlight-card">
        <div class="hl-top">
          <span class="hl-name">#${i + 1} · ${p.callsign !== '—' ? p.callsign : p.hex} (${p.reg})</span>
          <span class="hl-badge">${p.badge}</span>
        </div>
        <div class="hl-reason">${p.reason}</div>
        <div class="hl-links">
          <button class="hl-link map-jump" onclick="locateOnMap('aircraft', '${p.hex}', null, null)">Locate on Map</button>
          ${(p.links || []).map(l => `<a class="hl-link" href="${l.url}" target="_blank" rel="noopener">${l.label}</a>`).join('')}
        </div>
      </div>
    `).join('');
  }

  const shipsBox = document.getElementById('topShipsContainer');
  const ships = ais.top_interesting_ships || [];
  if (ships.length === 0) {
    shipsBox.innerHTML = '<div class="highlight-card">No ship highlights yet.</div>';
  } else {
    shipsBox.innerHTML = ships.map((s, i) => `
      <div class="highlight-card">
        <div class="hl-top">
          <span class="hl-name">#${i + 1} · ${s.shipname}</span>
          <span class="hl-badge">${s.badge}</span>
        </div>
        <div class="hl-reason">${s.reason}</div>
        <div class="hl-links">
          ${(s.lat && s.lon) ? `<button class="hl-link map-jump" onclick="locateOnMap('ship', '${s.mmsi}', ${s.lat}, ${s.lon})">Locate on Map</button>` : `<span class="hl-link" style="opacity:0.75;cursor:default">Static AIS Only</span>`}
          ${(s.links || []).map(l => `<a class="hl-link" href="${l.url}" target="_blank" rel="noopener">${l.label}</a>`).join('')}
        </div>
      </div>
    `).join('');
  }
}

function renderModes(modes, statusMap) {
  const grid = document.getElementById('modesGrid');
  grid.innerHTML = '';
  for (const [id, m] of Object.entries(modes)) {
    const isAct = !!statusMap[id];
    const url = buildAppUrl(m);
    const card = document.createElement('div');
    card.className = 'mode-card' + (isAct ? ' active' : '');
    card.innerHTML = `
      <div>
        <div class="mode-head">
          <div class="mode-title"><span>[${m.icon}]</span> <span>${m.title}</span></div>
          <span class="status-tag ${isAct ? 'on' : 'off'}">
            <span class="dot ${isAct ? 'green' : ''}"></span>
            ${isAct ? 'ACTIVE ON SDR' : 'STANDBY'}
          </span>
        </div>
        <div style="margin-bottom:8px"><span class="freq-badge">${m.badge}</span></div>
        <div class="mode-desc">${m.description}</div>
      </div>
      <div>
        <div class="mode-meta">
          <div><b>Stack:</b> ${m.stack}</div>
          <div><b>Endpoints:</b> ${m.PortLabel}</div>
        </div>
        <div class="btn-row">
          ${isAct
            ? `<button class="btn btn-active" disabled>Tuner Assigned</button>`
            : `<button class="btn btn-activate" onclick="switchMode('${id}')">Lock Mode</button>`
          }
          ${url
            ? `<a class="btn btn-open" href="${url}" target="_blank">Open Web UI</a>`
            : `<button class="btn btn-activate" onclick="copyEndpoint('${id}', ${m.port})">Copy ${host}:${m.port}</button>`
          }
        </div>
      </div>
    `;
    grid.appendChild(card);
  }
}

function copyEndpoint(id, port) {
  const txt = host + ':' + port;
  navigator.clipboard.writeText(txt).catch(() => {});
  alert('Connect your SDR client (e.g. SDR++ or Gqrx) to: ' + txt);
}

async function switchMode(modeId) {
  if (switching) return;
  switching = true;
  document.getElementById('activeBanner').textContent = 'Switching radio hardware to ' + modeId.toUpperCase() + '...';
  try {
    await fetch('/api/mode', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({mode: modeId})
    });
    await fetchStatus();
  } finally {
    switching = false;
  }
}

function renderAnalytics(adsb, ais) {
  document.getElementById('stAdsbPos').textContent = (adsb.total_positions || 0).toLocaleString();
  document.getElementById('stAdsbAc').textContent = `${adsb.unique_aircraft || 0} unique aircraft · ${adsb.total_flights || 0} flights`;
  document.getElementById('stAdsbRange').textContent = adsb.max_altitude_ft ? `${adsb.max_altitude_ft.toLocaleString()} ft` : '—';
  document.getElementById('stAdsbAlt').textContent = `Avg RSSI: ${adsb.avg_rssi_dbfs ?? '—'} dBFS · Max speed: ${adsb.max_speed_kt ?? '—'} kt`;

  document.getElementById('stAisVessels').textContent = (ais.unique_vessels || 0).toLocaleString();
  document.getElementById('stAisNamed').textContent = `${ais.named_vessels || 0} with decoded static vessel name`;
  document.getElementById('stAisMsgs').textContent = (ais.total_messages || 0).toLocaleString() + ' msgs';
  document.getElementById('stDbSize').textContent = `TimescaleDB Size: ${ais.timescaledb_size || '—'} (~65% smaller than SQLite)`;

  const adsbBody = document.getElementById('adsbTableBody');
  const flights = adsb.recent_flights || [];
  if (flights.length === 0) {
    adsbBody.innerHTML = '<tr><td colspan="6">No aircraft logged yet.</td></tr>';
  } else {
    adsbBody.innerHTML = flights.slice(0, 12).map(f => {
      const firstLink = (f.links && f.links[0]) ? `<a class="hl-link" href="${f.links[0].url}" target="_blank">Track</a>` : '—';
      return `
        <tr>
          <td class="mono"><b>${(f.hex || '').toUpperCase()}</b></td>
          <td class="mono">${f.callsign || '—'}</td>
          <td>${f.reg !== '—' ? `<b>${f.reg}</b> · ` : ''}${f.type_code || '—'}</td>
          <td>${f.min_alt ?? '—'}..${f.max_alt ?? '—'}</td>
          <td>${f.samples || 1}</td>
          <td>${firstLink}</td>
        </tr>
      `;
    }).join('');
  }

  const aisBody = document.getElementById('aisTableBody');
  const vessels = ais.recent_vessels || [];
  if (vessels.length === 0) {
    aisBody.innerHTML = '<tr><td colspan="6">No vessels logged yet.</td></tr>';
  } else {
    aisBody.innerHTML = vessels.slice(0, 12).map(v => {
      const firstLink = (v.links && v.links[0]) ? `<a class="hl-link" href="${v.links[0].url}" target="_blank">Vessel</a>` : '—';
      return `
        <tr>
          <td class="mono">${v.mmsi}</td>
          <td><b>${v.shipname}</b></td>
          <td class="mono">${v.len_m ? v.len_m + 'm' : '—'}</td>
          <td>${v.destination}</td>
          <td>${v.speed !== null && v.speed !== undefined ? v.speed + ' kt' : '—'}</td>
          <td>${firstLink}</td>
        </tr>
      `;
    }).join('');
  }
}

fetchStatus();
fetchMapAndLog();
setInterval(fetchStatus, 4000);
setInterval(fetchMapAndLog, 12000);
</script>
</body>
</html>
"""


class LauncherHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def _send_json(self, payload: dict, status=HTTPStatus.OK):
        raw = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path in ("/", "/index.html"):
            raw = HTML_PAGE.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
        elif parsed.path in ("/blog", "/blog/", "/doc", "/doc/"):
            self.send_response(HTTPStatus.FOUND)
            self.send_header("Location", "https://github.com/smichpresel/sdr-station/blob/main/doc/README.md")
            self.end_headers()
        elif parsed.path == "/api/status":
            now = time.time()
            if _STATUS_CACHE["data"] and (now - _STATUS_CACHE["ts"]) < 3.0:
                payload = _STATUS_CACHE["data"]
                payload["system"] = get_system_info()
            else:
                payload = {
                    "system": get_system_info(),
                    "modes": EXCLUSIVE_MODES,
                    "adsb": compute_adsb_analytics(),
                    "ais": get_ais_summary(),
                }
                _STATUS_CACHE["ts"] = now
                _STATUS_CACHE["data"] = payload
            self._send_json(payload)
        elif parsed.path == "/api/map":
            now = time.time()
            if _MAP_CACHE["data"] and (now - _MAP_CACHE["ts"]) < 6.0:
                payload = _MAP_CACHE["data"]
            else:
                payload = get_combined_map_and_log()
                _MAP_CACHE["ts"] = now
                _MAP_CACHE["data"] = payload
            self._send_json(payload)
        elif parsed.path == "/api/highlights/export.csv":
            buf = io.StringIO()
            try:
                conn = psycopg2.connect(PG_DSN)
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT domain, target_id, name_or_reg, secondary_id, category_key,
                               badge, score, reason, lat, lon, dist_km, metric_summary,
                               first_seen, last_seen
                        FROM spotter_highlights
                        ORDER BY score DESC, last_seen DESC
                        """
                    )
                    writer = csv.writer(buf)
                    writer.writerow([d[0] for d in cur.description])
                    writer.writerows(cur.fetchall())
                conn.close()
            except Exception:
                pass
            raw = buf.getvalue().encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/csv")
            self.send_header("Content-Disposition", 'attachment; filename="spotter_highlights_logbook.csv"')
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
        elif parsed.path == "/api/adsb/export.csv":
            buf = io.StringIO()
            try:
                conn = psycopg2.connect(PG_DSN)
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM adsb_positions ORDER BY ts DESC LIMIT 50000")
                    writer = csv.writer(buf)
                    writer.writerow([d[0] for d in cur.description])
                    writer.writerows(cur.fetchall())
                conn.close()
            except Exception:
                pass
            raw = buf.getvalue().encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/csv")
            self.send_header("Content-Disposition", 'attachment; filename="adsb_positions.csv"')
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
        elif parsed.path == "/api/ais/export.csv":
            buf = io.StringIO()
            try:
                conn = psycopg2.connect(PG_DSN)
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM ais_state ORDER BY received_at DESC NULLS LAST")
                    writer = csv.writer(buf)
                    writer.writerow([d[0] for d in cur.description])
                    writer.writerows(cur.fetchall())
                conn.close()
            except Exception:
                pass
            raw = buf.getvalue().encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/csv")
            self.send_header("Content-Disposition", 'attachment; filename="ais_vessels.csv"')
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
        else:
            self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self):
        parsed = urlparse(self.path)
        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length).decode("utf-8")) if length else {}
        if parsed.path == "/api/mode":
            mode_id = body.get("mode", "")
            result = activate_mode(mode_id)
            self._send_json(result)
        elif parsed.path == "/api/cycler":
            en = bool(body.get("enabled", True))
            interval = body.get("interval_sec")
            result = CYCLER.configure(enabled=en, interval_sec=interval)
            self._send_json({"ok": True, "cycler": result})
        else:
            self.send_error(HTTPStatus.NOT_FOUND)


def main():
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), LauncherHandler)
    print(f"[sdr-launcher] Listening on 0.0.0.0:{PORT}", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
