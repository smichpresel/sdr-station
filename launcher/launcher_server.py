#!/usr/bin/env python3
"""
SDR Station — Web Control Plane, Dual-Band Cycler, Cloudflare Quick Tunnel & Radar Hub.
Database Engine: PostgreSQL 17 + TimescaleDB 2.19 (dbname=sdr_station)

Features:
  1. Hardware receiver multiplexer & selectable ADS-B / Marine AIS Auto-Cycler
  2. Built-in Cloudflare Quick Tunnel manager (cloudflared tunnel --url, no account needed)
  3. Combined Air & Sea Map (Leaflet) with notable vessel & aircraft callouts
  4. Notable Catches of the Day + Persistent Spotter Logbook (spotter_highlights)
  5. TimescaleDB Hypertable Analytics & CSV Exports
"""

import csv
import io
import json
import math
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote_plus, urlparse

import psycopg2
import psycopg2.extras

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR / "adsb-analytics"))
sys.path.insert(0, str(BASE_DIR / "ais-analytics"))
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

try:
    from sts_detector import get_sts_events, run_sts_detection
except Exception:
    def run_sts_detection(dsn=None, force=False):
        return []
    def get_sts_events(dsn=None):
        return []

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
        "icon": "adsb",
        "port": 8504,
        "path": "/tar1090/",
        "PortLabel": ":8504 / /tar1090 · TCP 30002–30005",
        "stack": "readsb v3.16 + tar1090 + adsb-logger",
        "description": (
            "Real-time 1090 MHz Mode-S and ADS-B transponder demodulator. "
            "Serves the tar1090 radar display and logs flight positions into TimescaleDB."
        ),
    },
    "ais": {
        "id": "ais",
        "service": "ais-catcher.service",
        "title": "Marine AIS Receiver",
        "badge": "161.975 / 162.025 MHz",
        "icon": "ais",
        "port": 8100,
        "path": "/",
        "PortLabel": ":8100 (Web Map) · TCP 10110 (NMEA)",
        "stack": "AIS-catcher v0.70 + TimescaleDB (libpq)",
        "description": (
            "Dual-channel VHF maritime AIS GMSK receiver tracking vessel positions and static "
            "voyage reports into PostgreSQL 17 / TimescaleDB."
        ),
    },
    "voice": {
        "id": "voice",
        "service": "sdr-voice-web.service",
        "title": "VHF / FM Audio Receiver",
        "badge": "88 – 163 MHz · AM / NFM / WFM",
        "icon": "voice",
        "port": 8073,
        "path": "/",
        "PortLabel": ":8073 (Web Tuner & /stream.mp3)",
        "stack": "rtl_fm + FFmpeg MP3 Streamer",
        "description": (
            "Browser-streamed AM, NFM, and WFM demodulator and frequency scanner. "
            "Check local regulations before receiving or recording voice traffic."
        ),
    },
    "ism": {
        "id": "ism",
        "service": "rtl-433-web.service",
        "title": "433.92 MHz ISM Decoder",
        "badge": "433.92 MHz · OOK / FSK",
        "icon": "ism",
        "port": 8433,
        "path": "/",
        "PortLabel": ":8433 (Packet Feed & JSONL)",
        "stack": "rtl_433 v25.02 + Web Monitor",
        "description": (
            "Decodes wireless sensor protocols on 433.92 MHz including weather stations, "
            "tire pressure sensors (TPMS), and utility telemetry."
        ),
    },
    "rtltcp": {
        "id": "rtltcp",
        "service": "rtl-tcp.service",
        "title": "Raw I/Q Server (rtl_tcp)",
        "badge": "24 – 1766 MHz · 8-bit I/Q",
        "icon": "iq",
        "port": 1234,
        "path": None,
        "PortLabel": "TCP :1234 (rtl_tcp)",
        "stack": "rtl_tcp (0.0.0.0:1234)",
        "description": (
            "Streams raw baseband I/Q samples over TCP port 1234 for desktop waterfall clients "
            "(SDR++, Gqrx) and mobile applications."
        ),
    },
    "soapy": {
        "id": "soapy",
        "service": "soapyremote-server.service",
        "title": "SoapySDR Remote Server",
        "badge": "24 – 1766 MHz · SoapyRemote",
        "icon": "soapy",
        "port": 55132,
        "path": None,
        "PortLabel": "TCP/UDP :55132 (SoapySDR)",
        "stack": "SoapySDRServer (0.8)",
        "description": (
            "Exposes the RTL-SDR v5 over the SoapySDRRemote network protocol for "
            "CubicSDR, GNU Radio, SatDump, and Python scripts."
        ),
    },
}

MMSI_MID_FLAGS = {
    "273": ("Russia", "Russian Federation"),
    "422": ("Iran", "Islamic Republic of Iran"),
    "445": ("DPRK", "North Korea (DPRK)"),
    "468": ("Syria", "Syrian Arab Republic"),
    "775": ("Venezuela", "Venezuela"),
    "626": ("Gabon", "Gabon (Shadow-Fleet Registry)"),
    "613": ("Cameroon", "Cameroon (Shadow-Fleet Registry)"),
    "616": ("Comoros", "Comoros (Shadow-Fleet Registry)"),
    "511": ("Palau", "Palau (Shadow-Fleet Registry)"),
    "518": ("Cook Is.", "Cook Islands (Shadow-Fleet Registry)"),
    "669": ("Eswatini", "Eswatini (Shadow-Fleet Registry)"),
    "621": ("Djibouti", "Djibouti"),
    "667": ("Sierra Leone", "Sierra Leone"),
    "341": ("St. Kitts", "St. Kitts & Nevis"),
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

SANCTIONED_MMSI_WATCHLIST = {
    "273212000": "OFAC / UK / EU Sanctioned Russian Polar/Subsea Vessel",
    "273296440": "OFAC / EU Sanctioned Russian-Flagged Tanker Fleet",
    "273452310": "OFAC Sanctioned Sovcomflot / Russian Tanker",
    "273355210": "OFAC Sanctioned Russian Crude Carrier",
    "422263900": "OFAC Sanctioned NITC / IRISL Iranian State Fleet",
}

SANCTIONED_IMO_WATCHLIST = {
    "9318539": "OFAC / EU Sanctioned Russian Fleet",
    "9299721": "OFAC / UK Sanctioned Shadow-Fleet Crude Tanker",
    "9323326": "OFAC Designated Sanctioned Tanker",
    "9179842": "OFAC Designated IRISL / NITC Vessel",
}

SHADOW_FLEET_MIDS = {"626", "613", "616", "511", "518", "669", "621", "667", "341"}

AIS_SECURITY_KEYWORDS = ("ARMED GUARD", "SECURITY", "NO PIRACY", "ESCORT", "CITADEL", "HIGH RISK")
AIS_DISTRESSED_KEYWORDS = (
    "DISABLED", "NUC", "NOT UNDER COMMAND", "AGROUND", "DRIFT", "CASUALTY",
    "FIRE", "MEDEVAC", "MEDICAL", "DISTRESS", "SOS", "MAYDAY", "COLLISION",
    "TOW", "TOWING", "SALVAGE",
)
AIS_NAVAL_KEYWORDS = ("NAVY", "WARSHIP", "COAST GUARD", "PATROL", "MILITARY", "EXERCISE")


class CloudflareTunnelManager:
    """
    Manages a temporary, account-free Cloudflare Quick Tunnel (`cloudflared tunnel --url http://127.0.0.1:<port>`).
    Generates a public `https://<words>.trycloudflare.com` share link on demand.
    """

    URL_RE = re.compile(r"https://[a-zA-Z0-9-]+\.trycloudflare\.com")

    def __init__(self):
        self.lock = threading.Lock()
        self.proc = None
        self.target_port = PORT
        self.public_url = None
        self.status_text = "Idle"
        self.started_at = None

    def _find_binary(self) -> str | None:
        for candidate in (shutil.which("cloudflared"), "/usr/local/bin/cloudflared", "/usr/bin/cloudflared"):
            if candidate and Path(candidate).exists():
                return candidate
        return None

    def start(self, target_port: int = PORT) -> dict:
        with self.lock:
            self._stop_locked()
            bin_path = self._find_binary()
            if not bin_path:
                self.status_text = "cloudflared not installed (run ./install.sh)"
                return self.status()

            self.target_port = int(target_port or PORT)
            self.public_url = None
            self.status_text = "Requesting trycloudflare.com edge URL..."
            self.started_at = time.time()
            self.proc = subprocess.Popen(
                [
                    bin_path,
                    "tunnel",
                    "--no-autoupdate",
                    "--url",
                    f"http://127.0.0.1:{self.target_port}",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            t = threading.Thread(target=self._watch_output, args=(self.proc,), daemon=True)
            t.start()

        # Wait up to 4.5s for the URL to appear so the immediate API response can return it
        deadline = time.time() + 4.5
        while time.time() < deadline:
            with self.lock:
                if self.public_url or (self.proc and self.proc.poll() is not None):
                    break
            time.sleep(0.2)
        return self.status()

    def _watch_output(self, proc: subprocess.Popen):
        try:
            for line in proc.stdout:
                m = self.URL_RE.search(line)
                if m:
                    with self.lock:
                        if self.proc is proc:
                            self.public_url = m.group(0)
                            self.status_text = "Active"
        except Exception:
            pass
        finally:
            with self.lock:
                if self.proc is proc and proc.poll() is not None:
                    self.public_url = None
                    self.status_text = "Stopped"

    def _stop_locked(self):
        if self.proc and self.proc.poll() is None:
            try:
                self.proc.terminate()
                self.proc.wait(timeout=3)
            except Exception:
                try:
                    self.proc.kill()
                except Exception:
                    pass
        self.proc = None
        self.public_url = None
        self.status_text = "Idle"
        self.started_at = None

    def stop(self) -> dict:
        with self.lock:
            self._stop_locked()
            return self.status()

    def status(self) -> dict:
        with self.lock:
            running = self.proc is not None and self.proc.poll() is None
            if not running and self.status_text == "Active":
                self.status_text = "Stopped"
                self.public_url = None
            return {
                "installed": bool(self._find_binary()),
                "enabled": running,
                "port": self.target_port,
                "url": self.public_url,
                "status": self.status_text,
                "uptime_sec": int(time.time() - self.started_at) if (running and self.started_at) else None,
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
                    CREATE INDEX IF NOT EXISTS idx_ais_archive_mmsi_ts
                        ON ais_archive_ts (mmsi, received_at DESC);
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
TUNNEL = CloudflareTunnelManager()


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
    tailscale_status = "Connected" if tailscale_ip else "Not Connected"

    active_mode = "off"
    modes_status = {}
    for mid, m in EXCLUSIVE_MODES.items():
        act = get_service_active(m["service"])
        modes_status[mid] = act
        if act:
            active_mode = mid

    return {
        "station_name": STATION_NAME,
        "usb_sdr": "Nooelec NESDR SMArt v5" if usb_connected else "Not detected",
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
        "tunnel": TUNNEL.status(),
    }


def build_ship_links(mmsi: int, imo: int | None = None, shipname: str | None = None) -> list[dict]:
    """Generate public maritime & sanctions lookup URLs for a vessel."""
    m = str(mmsi)
    q_term = str(imo) if imo else (shipname if (shipname and shipname != "—") else m)
    return [
        {
            "label": "OpenSanctions",
            "url": f"https://www.opensanctions.org/search/?q={quote_plus(q_term)}",
        },
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
    """
    Scores vessels for the Notable Spotter Logbook across:
      1. Active AIS Emergencies, SAR, Distress, Towing, Armed Security & Spoofing Incidents (ais_incident)
      2. Sanctioned / Watchlist / Russian / Iranian / DPRK State & Commercial Fleet (sanctioned_vessel)
      3. High-Risk Shadow-Fleet / Dark-Fleet Crude & Product Tankers (shadow_fleet)
      4. 400m Ultra-Large Container Ships (megaship_400m) & Deep-Draught VLCC Supertankers (vlcc_supertanker)
      5. 350m+ Large Vessels (ulcv_350m) & Special / Law Enforcement / Patrol Craft (special_craft)
    """
    mmsi_str = str(v.get("mmsi", ""))
    imo_str = str(v.get("imo") or "")
    mid = mmsi_str[:3]
    name = (v.get("shipname") or "—").strip()
    dest = (v.get("destination") or "—").strip()
    dest_up = dest.upper()
    length_m = int(v.get("len_m") or 0)
    beam_m = int(v.get("beam_m") or 0)
    draught = float(v.get("draught") or 0.0)
    speed = float(v.get("speed") or 0.0)
    shiptype = int(v.get("shiptype") or 0)
    flag_short, flag_full = MMSI_MID_FLAGS.get(mid, ("Vessel", f"MID {mid}"))

    # 1A. Emergency AIS Beacons (AIS-SART 970, MOB 972, EPIRB 974, SAR Aircraft 111)
    if mmsi_str.startswith(("970", "972", "974", "111")):
        beacon_type = (
            "AIS-SART Survival Craft Beacon" if mmsi_str.startswith("970")
            else "Man Overboard (MOB) Beacon" if mmsi_str.startswith("972")
            else "EPIRB Distress Beacon" if mmsi_str.startswith("974")
            else "VHF SAR Aircraft Transponder"
        )
        b_label = name if name != "—" else f"{beacon_type.split()[0]} {mmsi_str}"
        return (
            154 if name != "—" else 133,
            "ais_incident",
            f"AIS Emergency · {beacon_type}",
            f"{b_label} (MMSI {mmsi_str}) — active {beacon_type} transmission ({speed} kt).",
        )

    # 1B. Distress / Casualty / Towing / Disabled / Fire / Medevac keywords in AIS destination
    for kw in AIS_DISTRESSED_KEYWORDS:
        if kw in dest_up:
            return (
                152,
                "ais_incident",
                f"AIS Incident · {kw.title()} ({flag_short})",
                f"{name} (MMSI {mmsi_str}, {length_m}m) — AIS destination reports '{dest}' at {speed} kt.",
            )

    # 1C. Armed Security / Anti-Piracy / High-Risk Transit Broadcasts in AIS destination
    for kw in AIS_SECURITY_KEYWORDS:
        if kw in dest_up:
            return (
                138,
                "ais_incident",
                f"Security Alert · Armed Guards ({flag_short})",
                f"{name} (MMSI {mmsi_str}, {length_m}m) — vessel broadcasting security status '{dest}' ({speed} kt).",
            )

    # 1D. Naval / Coast Guard / High-Speed SAR & Law Enforcement Intercepts over AIS
    for kw in AIS_NAVAL_KEYWORDS:
        if kw in dest_up or kw in name.upper():
            return (
                135,
                "ais_incident",
                f"Naval / Patrol Op ({flag_short})",
                f"{name} (MMSI {mmsi_str}) — military/patrol broadcast (dest: '{dest}', {speed} kt).",
            )

    if shiptype in (51, 55, 58) and speed >= 14.0:
        role = "Law Enforcement Intercept" if shiptype == 55 else ("SAR High-Speed Response" if shiptype == 51 else "Medical Dispatch")
        return (
            134,
            "ais_incident",
            f"AIS Incident · {role} ({speed} kt)",
            f"{name} (MMSI {mmsi_str}, {flag_short}) — operating at {speed} kt (dest: '{dest}').",
        )

    # 1E. AIS Kinematics / GPS Spoofing Anomaly (surface vessel > 48 kt outside HSC/WIG)
    if speed >= 48.0 and not (40 <= shiptype <= 49) and not mmsi_str.startswith("111"):
        return (
            136,
            "ais_incident",
            f"AIS Spoofing / Speed Anomaly ({speed} kt)",
            f"{name} (MMSI {mmsi_str}, {flag_short}) — anomalous surface speed {speed} kt reported over AIS.",
        )

    # 2A. Explicit OFAC / EU / UK Sanctioned Vessel Watchlist (MMSI or IMO)
    if mmsi_str in SANCTIONED_MMSI_WATCHLIST or imo_str in SANCTIONED_IMO_WATCHLIST:
        note = SANCTIONED_MMSI_WATCHLIST.get(mmsi_str) or SANCTIONED_IMO_WATCHLIST.get(imo_str)
        return (
            155,
            "sanctioned_vessel",
            f"Sanctioned Watchlist · {flag_short}",
            f"{name} (MMSI {mmsi_str}{', IMO ' + imo_str if imo_str else ''}) — {note}, dest: '{dest}'.",
        )

    # 2B. Russian (MID 273), Iranian (MID 422), DPRK (MID 445), Syrian (MID 468), Venezuelan (MID 775) Flagged Vessels
    if mid in ("273", "422", "445", "468", "775"):
        return (
            146,
            "sanctioned_vessel",
            f"Sanctioned / Watchlist Flag · {flag_short} (MID {mid})",
            f"{name} (MMSI {mmsi_str}{', IMO ' + imo_str if imo_str else ''}) — {flag_full} maritime registry ({length_m}m × {beam_m}m, {draught}m draught), dest: '{dest}'.",
        )

    # 3. High-Risk Shadow-Fleet / Dark-Fleet Tankers (flag-hopping registries + crude/product tankers orSTS/OPL orders)
    is_tanker = 80 <= shiptype <= 89
    is_obfuscated_orders = dest_up in ("FOR ORDERS", "FOR ORDER", "ORDERS", "OPL", "STS", "TBN", "HIGH SEA")
    if (mid in SHADOW_FLEET_MIDS and (is_tanker or length_m >= 180)) or (is_tanker and length_m >= 240 and is_obfuscated_orders):
        reason_tag = f"{flag_full} tanker registry" if mid in SHADOW_FLEET_MIDS else f"Tanker broadcasting '{dest}' orders"
        return (
            131,
            "shadow_fleet",
            f"Shadow-Fleet Watch · {flag_short} ({length_m or '—'}m)",
            f"{name} (MMSI {mmsi_str}{', IMO ' + imo_str if imo_str else ''}) — {reason_tag} ({draught}m draught, {speed} kt), dest: '{dest}'.",
        )

    # 4. 400m Ultra-Large Container Ships (ULCS)
    if length_m >= 395:
        return (
            125 + (1 if draught >= 15.5 else 0),
            "megaship_400m",
            f"400m ULCS ({length_m}m × {beam_m}m · {flag_short})",
            f"{name} (IMO {v.get('imo') or '—'}) — 400m-class container vessel ({draught}m draught), dest: '{dest}'.",
        )

    # 5. Deep-Draught VLCC Supertankers
    if (is_tanker and draught >= 19.0) or (draught >= 20.0 and length_m >= 300):
        return (
            112,
            "vlcc_supertanker",
            f"Deep-Draught VLCC ({draught}m · {flag_short})",
            f"{name} ({length_m}m × {beam_m}m) — deep-draught supertanker drawing {draught}m, dest: '{dest}'.",
        )

    # 6. 350m+ Large Vessels
    if length_m >= 350:
        return (
            102,
            "ulcv_350m",
            f"350m+ Large Vessel ({length_m}m · {flag_short})",
            f"{name} (IMO {v.get('imo') or '—'}) — {length_m}m × {beam_m}m vessel ({draught}m draught), dest: '{dest}'.",
        )

    # 7. Special Craft (SAR, Law Enforcement, Towing, Superyachts)
    if shiptype in (51, 55, 31, 32) or (shiptype == 37 and length_m >= 50):
        badge_lbl = (
            "Law Enforcement / Patrol" if shiptype == 55
            else "Search & Rescue" if shiptype == 51
            else "Ocean Towing" if shiptype in (31, 32)
            else f"{length_m}m Superyacht"
        )
        return (
            94,
            "special_craft",
            f"{badge_lbl} ({flag_short})",
            f"{name} (MMSI {mmsi_str}) — special-purpose vessel ({length_m}m, {speed} kt), dest: '{dest}'.",
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
            d["links"] = build_ship_links(d["mmsi"], d.get("imo"), d.get("shipname"))
            all_vessels.append(d)

            score, cat_key, badge, reason = score_interesting_ship(d)
            if (d["shipname"] and d["shipname"] != "—") or cat_key in ("sanctioned_vessel", "shadow_fleet", "ais_incident"):
                disp_name = d["shipname"] if (d["shipname"] and d["shipname"] != "—") else f"MMSI {d['mmsi']}"
                ship_entry = {
                    "score": score,
                    "cat_key": cat_key,
                    "badge": badge,
                    "reason": reason,
                    "mmsi": d["mmsi"],
                    "shipname": disp_name,
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
                            "name_or_reg": disp_name,
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
                is_int = score >= 90 and (d["shipname"] != "—" or cat_key in ("sanctioned_vessel", "shadow_fleet", "ais_incident"))
                links = build_ship_links(d["mmsi"], d.get("imo"), d.get("shipname"))
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
                WHERE category_key IN (
                    'ais_incident', 'sanctioned_vessel', 'shadow_fleet',
                    'sanctioned_aviation', 'military_gov',
                    'megaship_400m', 'vlcc_supertanker', 'ulcv_350m',
                    'special_craft', 'notable_registry', 'bizjet',
                    'quadjet', 'light_ga', 'extreme_range'
                )
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


def get_target_track(domain: str, target_id: str, hours: int = 24) -> dict:
    """
    Query historical trajectory polylines for an aircraft (adsb_positions)
    or vessel (ais_archive_ts + ais_position/ais_message) from TimescaleDB.
    """
    domain = (domain or "").strip().lower()
    target_id = (target_id or "").strip()
    hours = max(1, min(int(hours or 24), 168))
    points = []

    if not target_id:
        return {"domain": domain, "target_id": target_id, "points": []}

    try:
        conn = psycopg2.connect(PG_DSN)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if domain == "aircraft":
                cur.execute(
                    """
                    SELECT
                        ROUND(lat::numeric, 5)::float AS lat,
                        ROUND(lon::numeric, 5)::float AS lon,
                        alt_baro,
                        ROUND(gs::numeric, 1)::float AS gs,
                        ROUND(track::numeric, 1)::float AS heading,
                        ts
                    FROM adsb_positions
                    WHERE LOWER(hex) = LOWER(%s)
                      AND lat IS NOT NULL AND lon IS NOT NULL
                      AND ts >= NOW() - (%s || ' hours')::interval
                    ORDER BY ts ASC
                    LIMIT 900
                    """,
                    (target_id, str(hours)),
                )
                rows = cur.fetchall()
                if not rows:
                    cur.execute(
                        """
                        SELECT
                            ROUND(lat::numeric, 5)::float AS lat,
                            ROUND(lon::numeric, 5)::float AS lon,
                            alt_baro,
                            ROUND(gs::numeric, 1)::float AS gs,
                            ROUND(track::numeric, 1)::float AS heading,
                            ts
                        FROM adsb_positions
                        WHERE LOWER(hex) = LOWER(%s)
                          AND lat IS NOT NULL AND lon IS NOT NULL
                        ORDER BY ts ASC
                        LIMIT 900
                        """,
                        (target_id,),
                    )
                    rows = cur.fetchall()
                for r in rows:
                    points.append(
                        {
                            "lat": r["lat"],
                            "lon": r["lon"],
                            "alt_baro": r["alt_baro"],
                            "speed": r["gs"],
                            "heading": r["heading"],
                            "ts_utc": r["ts"].strftime("%Y-%m-%d %H:%M:%S UTC") if r.get("ts") else "—",
                        }
                    )
            elif domain == "ship":
                mmsi_int = int(target_id)
                cur.execute(
                    """
                    WITH raw_pts AS (
                        SELECT
                            received_at AS ts,
                            ROUND(lat::numeric, 5)::float AS lat,
                            ROUND(lon::numeric, 5)::float AS lon,
                            ROUND(speed::numeric, 1)::float AS speed,
                            ROUND(course::numeric, 1)::float AS heading
                        FROM ais_archive_ts
                        WHERE mmsi = %s
                          AND lat IS NOT NULL AND lon IS NOT NULL
                          AND lat BETWEEN -85 AND 85 AND lon BETWEEN -180 AND 180
                          AND (lat != 0 OR lon != 0)
                        UNION ALL
                        SELECT
                            m.received_at AS ts,
                            ROUND(p.lat::numeric, 5)::float AS lat,
                            ROUND(p.lon::numeric, 5)::float AS lon,
                            ROUND(p.speed::numeric, 1)::float AS speed,
                            ROUND(p.course::numeric, 1)::float AS heading
                        FROM ais_position p
                        JOIN ais_message m ON m.id = p.msg_id
                        WHERE m.mmsi = %s
                          AND p.lat IS NOT NULL AND p.lon IS NOT NULL
                          AND p.lat BETWEEN -85 AND 85 AND p.lon BETWEEN -180 AND 180
                          AND (p.lat != 0 OR p.lon != 0)
                    ),
                    bucketed AS (
                        SELECT DISTINCT ON (date_trunc('minute', ts))
                            ts, lat, lon, speed, heading
                        FROM raw_pts
                        ORDER BY date_trunc('minute', ts) ASC, ts ASC
                    )
                    SELECT ts, lat, lon, speed, heading
                    FROM bucketed
                    ORDER BY ts ASC
                    LIMIT 900
                    """,
                    (mmsi_int, mmsi_int),
                )
                for r in cur.fetchall():
                    points.append(
                        {
                            "lat": r["lat"],
                            "lon": r["lon"],
                            "speed": r["speed"],
                            "heading": r["heading"],
                            "ts_utc": r["ts"].strftime("%Y-%m-%d %H:%M:%S UTC") if r.get("ts") else "—",
                        }
                    )
        conn.close()
    except Exception as e:
        print(f"[sdr-launcher] Track query error ({domain}:{target_id}): {e}", flush=True)

    return {
        "domain": domain,
        "target_id": target_id,
        "count": len(points),
        "points": points,
    }


def get_notable_tracks(max_targets: int = 12) -> dict:
    """
    Returns multi-point historical track polylines for the highest-scoring
    notable aircraft and vessels in spotter_highlights.
    """
    tracks = []
    try:
        conn = psycopg2.connect(PG_DSN)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT domain, target_id, name_or_reg, category_key, badge, score
                FROM spotter_highlights
                WHERE lat IS NOT NULL AND lon IS NOT NULL
                ORDER BY score DESC, last_seen DESC
                LIMIT %s
                """,
                (max_targets * 2,),
            )
            candidates = cur.fetchall()
        conn.close()

        for c in candidates:
            tr = get_target_track(c["domain"], c["target_id"], hours=48)
            if len(tr.get("points", [])) >= 2:
                tracks.append(
                    {
                        "domain": c["domain"],
                        "target_id": c["target_id"],
                        "name": c["name_or_reg"],
                        "category_key": c["category_key"],
                        "badge": c["badge"],
                        "points": tr["points"],
                    }
                )
            if len(tracks) >= max_targets:
                break
    except Exception as e:
        print(f"[sdr-launcher] Notable tracks error: {e}", flush=True)
    return {"tracks": tracks}


HTML_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SDR Station Console</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" crossorigin=""/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" crossorigin=""></script>
<style>
  @layer base, tokens, layout, components, utilities;

  @layer tokens {
    :root {
      color-scheme: dark;
      --canvas: oklch(0.13 0.006 255);
      --surface: oklch(0.16 0.007 255);
      --surface-raised: oklch(0.19 0.009 255);
      --surface-well: oklch(0.115 0.005 255);
      --line: oklch(1 0 0 / 0.08);
      --line-strong: oklch(1 0 0 / 0.15);
      --ink-strong: oklch(0.95 0.004 255);
      --ink: oklch(0.83 0.008 255);
      --ink-muted: oklch(0.62 0.012 255);
      --ink-faint: oklch(0.46 0.012 255);
      --accent: oklch(0.72 0.12 230);
      --accent-soft: oklch(0.72 0.12 230 / 0.13);
      --signal-green: oklch(0.73 0.15 152);
      --signal-green-soft: oklch(0.73 0.15 152 / 0.14);
      --signal-amber: oklch(0.77 0.14 75);
      --signal-amber-soft: oklch(0.77 0.14 75 / 0.15);
      --signal-rose: oklch(0.68 0.18 18);
      --font-sans: -apple-system, BlinkMacSystemFont, "Inter", "Segoe UI", system-ui, sans-serif;
      --font-mono: "JetBrains Mono", "SF Mono", ui-monospace, Menlo, Consolas, monospace;
      --radius-sm: 5px;
      --radius-md: 7px;
      --radius-lg: 10px;
      accent-color: var(--accent);
      scrollbar-color: oklch(0.32 0.01 255) var(--surface-well);
    }
  }

  @layer base {
    *, *::before, *::after { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--canvas);
      color: var(--ink);
      font-family: var(--font-sans);
      font-size: 0.875rem;
      line-height: 1.5;
      -webkit-font-smoothing: antialiased;
    }
    h1, h2, h3 {
      margin: 0;
      color: var(--ink-strong);
      font-weight: 600;
      letter-spacing: -0.015em;
      text-wrap: balance;
    }
    p { margin: 0; text-wrap: pretty; }
    a { color: inherit; text-decoration: none; }
    button, select, input {
      font: inherit;
      color: inherit;
    }
    .num {
      font-family: var(--font-mono);
      font-variant-numeric: tabular-nums;
    }
  }

  @layer layout {
    .shell {
      max-inline-size: 1360px;
      margin-inline: auto;
      padding-inline: clamp(1rem, 2.5vw, 1.75rem);
      padding-block-end: 3.5rem;
    }

    /* Top Chrome Bar (38-44px instrument bar) */
    .topbar {
      display: flex;
      flex-wrap: wrap;
      align-items: safe center;
      justify-content: space-between;
      gap: 0.75rem 1.25rem;
      padding-block: 0.75rem;
      border-block-end: 1px solid var(--line);
    }
    .brand-group {
      display: flex;
      align-items: baseline;
      gap: 0.75rem;
      flex-wrap: wrap;
    }
    .brand-title {
      font-size: 0.95rem;
      font-weight: 600;
      color: var(--ink-strong);
      display: inline-flex;
      align-items: center;
      gap: 0.5rem;
    }
    .brand-sub {
      font-size: 0.78rem;
      color: var(--ink-muted);
    }
    .telemetry- cluster, .status-strip {
      display: flex;
      flex-wrap: wrap;
      align-items: safe center;
      gap: 1rem;
      font-size: 0.78rem;
      color: var(--ink-muted);
    }
    .status-item {
      display: inline-flex;
      align-items: center;
      gap: 0.4rem;
    }
    .status-item strong {
      color: var(--ink-strong);
      font-weight: 500;
    }

    /* Unified Control & Telemetry Ribbon */
    .ribbon {
      display: grid;
      grid-template-columns: minmax(0, 1.25fr) minmax(0, 1fr);
      gap: 1px;
      background: var(--line);
      border: 1px solid var(--line);
      border-radius: var(--radius-lg);
      margin-block: 1rem 1.25rem;
      overflow: clip;
    }
    @media (max-width: 980px) {
      .ribbon { grid-template-columns: 1fr; }
    }
    .ribbon-cell {
      background: var(--surface);
      padding-block: 0.85rem;
      padding-inline: 1rem;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
      gap: 0.65rem;
    }

    /* Main Split Stage: Map + Notable Inspector */
    .stage-split {
      display: grid;
      grid-template-columns: minmax(0, 1.55fr) minmax(340px, 1fr);
      gap: 1rem;
      margin-block-end: 1.5rem;
      align-items: stretch;
    }
    @media (max-width: 1024px) {
      .stage-split { grid-template-columns: 1fr; }
    }

    /* Receiver Matrix using CSS Subgrid */
    .modes-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      gap: 0.75rem;
      margin-block-end: 1.5rem;
    }
    .mode-card {
      grid-row: span 3;
      display: grid;
      grid-template-rows: auto 1fr auto;
      grid-template-rows: subgrid;
      gap: 0.65rem;
      background: var(--surface);
      border: 1px solid var(--line);
      border-radius: var(--radius-lg);
      padding-block: 0.9rem;
      padding-inline: 1rem;
      transition: border-color 0.15s ease;
    }
    .mode-card[data-active="true"] {
      border-color: oklch(0.73 0.15 152 / 0.55);
      background: linear-gradient(180deg, oklch(0.175 0.015 152 / 0.35), var(--surface) 65%);
    }
  }

  @layer components {
    .dot {
      inline-size: 7px;
      block-size: 7px;
      border-radius: 50%;
      display: inline-block;
      flex: none;
      background: var(--ink-faint);
    }
    .dot.live { background: var(--signal-green); }
    .dot.amber { background: var(--signal-amber); }

    .icon-svg {
      inline-size: 14px;
      block-size: 14px;
      stroke: currentColor;
      stroke-width: 1.6;
      fill: none;
      flex: none;
      opacity: 0.78;
    }

    /* Segmented Controls (Stageshow / Radix style) */
    .segmented {
      display: inline-flex;
      flex-wrap: wrap;
      background: var(--surface-well);
      border: 1px solid var(--line);
      border-radius: var(--radius-md);
      padding: 2px;
      gap: 2px;
    }
    .seg-btn {
      background: transparent;
      border: none;
      color: var(--ink-muted);
      font-size: 0.76rem;
      font-weight: 500;
      padding-block: 0.28rem;
      padding-inline: 0.65rem;
      border-radius: var(--radius-sm);
      cursor: pointer;
      transition: background-color 0.12s ease, color 0.12s ease;
    }
    .seg-btn:hover:not(.active) { color: var(--ink-strong); }
    .seg-btn.active {
      background: var(--surface-raised);
      color: var(--ink-strong);
      box-shadow: 0 1px 2px oklch(0 0 0 / 0.35);
    }

    /* Controls & Buttons */
    .btn {
      min-block-size: 1.95rem;
      padding-block: 0.3rem;
      padding-inline: 0.75rem;
      border-radius: var(--radius-sm);
      font-size: 0.78rem;
      font-weight: 500;
      border: 1px solid var(--line-strong);
      background: var(--surface-raised);
      color: var(--ink-strong);
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 0.4rem;
      white-space: nowrap;
      flex: none;
      transition: background-color 0.12s ease, border-color 0.12s ease;
    }
    .btn:hover:not(:disabled) {
      background: oklch(0.23 0.01 255);
      border-color: oklch(1 0 0 / 0.24);
    }
    .btn:disabled {
      opacity: 0.55;
      cursor: default;
    }
    .btn-primary {
      background: var(--ink-strong);
      color: var(--canvas);
      border-color: transparent;
      font-weight: 600;
    }
    .btn-primary:hover:not(:disabled) {
      background: oklch(0.88 0.005 255);
    }
    .btn-subtle {
      background: transparent;
      border-color: var(--line);
      color: var(--ink-muted);
    }
    .btn-subtle:hover:not(:disabled) {
      color: var(--ink-strong);
      border-color: var(--line-strong);
      background: var(--surface-raised);
    }

    select.ctrl-select, input.ctrl-input {
      min-block-size: 1.95rem;
      background: var(--surface-well);
      color: var(--ink-strong);
      border: 1px solid var(--line-strong);
      border-radius: var(--radius-sm);
      padding-block: 0.25rem;
      padding-inline: 0.6rem;
      font-size: 0.78rem;
    }
    input.ctrl-input {
      field-sizing: content;
      min-inline-size: 180px;
    }

    /* Inline KPI Readout Grid inside Ribbon */
    .kpi-strip {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 0.75rem;
      align-items: baseline;
    }
    @media (max-width: 640px) {
      .kpi-strip { grid-template-columns: repeat(2, 1fr); }
    }
    .kpi-item {
      border-inline-start: 1px solid var(--line);
      padding-inline-start: 0.75rem;
    }
    .kpi-item:first-child {
      border-inline-start: none;
      padding-inline-start: 0;
    }
    .kpi-label {
      font-size: 0.7rem;
      color: var(--ink-muted);
    }
    .kpi-val {
      font-family: var(--font-mono);
      font-variant-numeric: tabular-nums;
      font-size: 1.2rem;
      font-weight: 600;
      color: var(--ink-strong);
      line-height: 1.25;
      margin-block-start: 0.1rem;
    }
    .kpi-sub {
      font-size: 0.7rem;
      color: var(--ink-faint);
      font-family: var(--font-mono);
      font-variant-numeric: tabular-nums;
    }

    /* Surface Panels */
    .panel {
      background: var(--surface);
      border: 1px solid var(--line);
      border-radius: var(--radius-lg);
      display: flex;
      flex-direction: column;
      overflow: clip;
    }
    .panel-bar {
      display: flex;
      flex-wrap: wrap;
      align-items: safe center;
      justify-content: space-between;
      gap: 0.5rem 0.75rem;
      padding-block: 0.65rem;
      padding-inline: 0.9rem;
      border-block-end: 1px solid var(--line);
      background: var(--surface);
    }
    .panel-title {
      font-size: 0.82rem;
      font-weight: 600;
      color: var(--ink-strong);
      display: inline-flex;
      align-items: center;
      gap: 0.45rem;
    }
    .panel-meta {
      font-size: 0.74rem;
      color: var(--ink-muted);
      font-family: var(--font-mono);
      font-variant-numeric: tabular-nums;
    }

    #combinedMap {
      inline-size: 100%;
      block-size: 470px;
      background: var(--surface-well);
    }
    .leaflet-tile-pane {
      filter: brightness(0.72) contrast(1.08) saturate(0.75);
    }
    .leaflet-popup-content-wrapper, .leaflet-popup-tip {
      background: var(--surface) !important;
      color: var(--ink) !important;
      border: 1px solid var(--line-strong);
      border-radius: var(--radius-md);
      box-shadow: 0 12px 28px oklch(0 0 0 / 0.6);
    }
    .map-tag {
      white-space: nowrap;
      font-family: var(--font-mono);
      font-size: 10px;
      font-weight: 600;
      padding: 1px 5px;
      border-radius: 4px;
      box-shadow: 0 2px 6px oklch(0 0 0 / 0.65);
    }
    .map-tag.ship-hi {
      background: oklch(0.22 0.04 75 / 0.95);
      color: oklch(0.92 0.08 75);
      border: 1px solid oklch(0.77 0.14 75 / 0.6);
    }
    .map-tag.plane-hi {
      background: oklch(0.22 0.05 18 / 0.95);
      color: oklch(0.92 0.06 18);
      border: 1px solid oklch(0.68 0.18 18 / 0.6);
    }

    /* Notable Inspector Rows (Stageshow Row discipline: 10-12px padding, 4px title-sub gap) */
    .inspector-list {
      display: flex;
      flex-direction: column;
      overflow-y: auto;
      max-block-size: 470px;
      scrollbar-gutter: stable;
      overscroll-behavior: contain;
    }
    .ins-group-label {
      padding-block: 0.4rem;
      padding-inline: 0.9rem;
      font-size: 0.7rem;
      font-weight: 600;
      color: var(--ink-muted);
      background: var(--surface-well);
      border-block-end: 1px solid var(--line);
      display: flex;
      justify-content: space-between;
    }
    .ins-row {
      padding-block: 0.65rem;
      padding-inline: 0.9rem;
      display: flex;
      flex-direction: column;
      gap: 0.25rem;
    }
    .ins-row:not(:last-child) {
      border-block-end: 1px solid var(--line);
    }
    .ins-head {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 0.5rem;
    }
    .ins-name {
      font-family: var(--font-mono);
      font-variant-numeric: tabular-nums;
      font-size: 0.82rem;
      font-weight: 600;
      color: var(--ink-strong);
    }
    .tag-spec {
      font-family: var(--font-mono);
      font-size: 0.68rem;
      color: var(--ink-muted);
      background: var(--surface-raised);
      border: 1px solid var(--line);
      padding-block: 0.1rem;
      padding-inline: 0.42rem;
      border-radius: 4px;
      white-space: nowrap;
    }
    .ins-desc {
      font-size: 0.76rem;
      color: var(--ink-muted);
      line-height: 1.38;
    }
    .ins-actions {
      display: flex;
      flex-wrap: wrap;
      gap: 0.35rem;
      margin-block-start: 0.2rem;
    }
    .action-chip {
      font-size: 0.69rem;
      font-weight: 500;
      color: var(--ink);
      background: var(--surface-well);
      border: 1px solid var(--line);
      padding-block: 0.12rem;
      padding-inline: 0.48rem;
      border-radius: 4px;
      cursor: pointer;
    }
    .action-chip:hover {
      border-color: var(--line-strong);
      color: var(--ink-strong);
      background: var(--surface-raised);
    }
    .action-chip.locate {
      color: var(--accent);
      border-color: oklch(0.72 0.12 230 / 0.35);
    }

    /* Section Header */
    .sec-bar {
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      align-items: baseline;
      gap: 0.5rem;
      margin-block: 1.25rem 0.65rem;
    }
    .sec-bar h2 {
      font-size: 0.88rem;
      font-weight: 600;
      color: var(--ink-strong);
    }
    .sec-bar span {
      font-size: 0.76rem;
      color: var(--ink-muted);
    }

    /* Mode Card Internals */
    .mode-top {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 0.5rem;
    }
    .mode-name {
      font-size: 0.86rem;
      font-weight: 600;
      color: var(--ink-strong);
    }
    .mode-freq {
      font-family: var(--font-mono);
      font-size: 0.7rem;
      color: var(--ink-muted);
      margin-block-start: 0.15rem;
    }
    .mode-state {
      font-family: var(--font-mono);
      font-size: 0.68rem;
      padding-block: 0.12rem;
      padding-inline: 0.45rem;
      border-radius: 4px;
      border: 1px solid var(--line);
      color: var(--ink-muted);
      display: inline-flex;
      align-items: center;
      gap: 0.35rem;
      white-space: nowrap;
    }
    .mode-state.on {
      color: var(--signal-green);
      background: var(--signal-green-soft);
      border-color: oklch(0.73 0.15 152 / 0.35);
    }
    .mode-body {
      font-size: 0.78rem;
      color: var(--ink-muted);
      line-height: 1.45;
    }
    .mode-foot {
      border-block-start: 1px solid var(--line);
      padding-block-start: 0.65rem;
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 0.5rem;
    }
    .mode-endpoint {
      font-family: var(--font-mono);
      font-size: 0.7rem;
      color: var(--ink-faint);
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    /* Unified Data Table */
    .table-scroll {
      max-block-size: 420px;
      overflow-y: auto;
      scrollbar-gutter: stable;
      overscroll-behavior: contain;
    }
    table {
      inline-size: 100%;
      border-collapse: collapse;
      font-size: 0.79rem;
    }
    th, td {
      text-align: start;
      padding-block: 0.55rem;
      padding-inline: 0.85rem;
      vertical-align: middle;
    }
    th {
      position: sticky;
      inset-block-start: 0;
      background: var(--surface-well);
      color: var(--ink-muted);
      font-size: 0.72rem;
      font-weight: 500;
      border-block-end: 1px solid var(--line);
      z-index: 2;
    }
    tbody tr:not(:last-child) td {
      border-block-end: 1px solid var(--line);
    }
    tbody tr:hover td {
      background: oklch(1 0 0 / 0.02);
    }

    /* Tunnel Share Bar */
    .tunnel-box {
      display: inline-flex;
      align-items: center;
      gap: 0.4rem;
      background: var(--surface);
      border: 1px solid var(--line);
      border-radius: var(--radius-sm);
      padding-block: 0.2rem;
      padding-inline: 0.5rem;
    }
    .tunnel-url {
      font-family: var(--font-mono);
      font-size: 0.74rem;
      color: var(--accent);
      max-inline-size: 260px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
  }
</style>
</head>
<body>
<div class="shell">
  <!-- TOP INSTRUMENT BAR -->
  <header class="topbar">
    <div class="brand-group">
      <div class="brand-title">
        <span class="dot live" id="usbDot"></span>
        <span>SDR Receiver Console</span>
      </div>
      <span class="brand-sub" id="usbName">Nooelec NESDR SMArt v5 · Raspberry Pi CM4 · TimescaleDB</span>
    </div>

    <div class="status-strip">
      <div class="status-item">CPU <strong class="num" id="cpuTemp">--</strong></div>
      <div class="status-item">LAN <strong id="lanIp">--</strong></div>
      <div class="status-item">Tailscale <strong id="tsIp">--</strong></div>

      <!-- CLOUDFLARE QUICK TUNNEL SHARE CONTROL -->
      <div class="tunnel-box" id="tunnelBox">
        <span class="dot" id="tunnelDot"></span>
        <span id="tunnelLabel" style="font-size:0.74rem;color:var(--ink-muted)">Public Share:</span>
        <a class="tunnel-url" id="tunnelUrlLink" href="#" target="_blank" rel="noopener" style="display:none"></a>
        <button class="action-chip" id="tunnelCopyBtn" style="display:none" onclick="copyTunnelUrl()">Copy URL</button>
        <button class="action-chip locate" id="tunnelToggleBtn" onclick="toggleTunnel()">Share via Cloudflare</button>
      </div>

      <a class="btn btn-subtle" href="https://github.com/smichpresel/sdr-station/blob/main/doc/README.md" target="_blank" rel="noopener">Documentation</a>
    </div>
  </header>

  <!-- UNIFIED CYCLER + TELEMETRY RIBBON -->
  <section class="ribbon" aria-label="Tuner schedule and telemetry">
    <div class="ribbon-cell">
      <div style="display:flex;justify-content:space-between;align-items:center;gap:0.75rem;flex-wrap:wrap">
        <div style="display:flex;align-items:center;gap:0.5rem">
          <span class="dot live" id="cyclerDot"></span>
          <strong style="font-size:0.84rem;color:var(--ink-strong)" id="cyclerHeadline">Dual-Band Auto-Cycler (1090 MHz ADS-B / 162 MHz AIS)</strong>
        </div>
        <div style="display:flex;align-items:center;gap:0.4rem;flex-wrap:wrap">
          <select class="ctrl-select num" id="cycleIntervalSelect" onchange="changeCycleInterval()" aria-label="Cycle interval">
            <option value="60">1m window</option>
            <option value="120" selected>2m window</option>
            <option value="180">3m window</option>
            <option value="300">5m window</option>
            <option value="600">10m window</option>
          </select>
          <button class="btn" id="cyclerToggleBtn" onclick="toggleCycler()">Pause Cycler</button>
          <button class="btn btn-subtle" onclick="switchMode('off')">Standby</button>
        </div>
      </div>
      <p style="font-size:0.76rem;color:var(--ink-muted)" id="cyclerSub">
        Alternating single RTL-SDR tuner between Mode-S aircraft reception and VHF maritime AIS reception.
      </p>
    </div>

    <div class="ribbon-cell">
      <div class="kpi-strip">
        <div class="kpi-item">
          <div class="kpi-label">ADS-B Track Points</div>
          <div class="kpi-val" id="stAdsbPos">0</div>
          <div class="kpi-sub" id="stAdsbAc">0 aircraft</div>
        </div>
        <div class="kpi-item">
          <div class="kpi-label">Peak Baro Altitude</div>
          <div class="kpi-val" id="stAdsbRange">—</div>
          <div class="kpi-sub" id="stAdsbAlt">RSSI —</div>
        </div>
        <div class="kpi-item">
          <div class="kpi-label">AIS Vessels Tracked</div>
          <div class="kpi-val" id="stAisVessels">0</div>
          <div class="kpi-sub" id="stAisNamed">0 named</div>
        </div>
        <div class="kpi-item">
          <div class="kpi-label">TimescaleDB Size</div>
          <div class="kpi-val" id="stDbSize">—</div>
          <div class="kpi-sub" id="stAisMsgs">0 AIS msgs</div>
        </div>
      </div>
    </div>
  </section>

  <!-- PRIMARY SPLIT STAGE: COMBINED RADAR MAP + NOTABLE CATCHES INSPECTOR -->
  <section class="stage-split" id="mapSection">
    <div class="panel">
      <div class="panel-bar">
        <div style="display:flex;align-items:center;gap:0.75rem;flex-wrap:wrap">
          <span class="panel-title">Combined Air &amp; Sea Plot</span>
          <div class="segmented" role="tablist" aria-label="Map layer filter">
            <button class="seg-btn active" id="fltAll" onclick="setMapFilter('all')">All Traffic</button>
            <button class="seg-btn" id="fltHighlights" onclick="setMapFilter('highlights')">Notable Only</button>
            <button class="seg-btn" id="fltSanctioned" onclick="setMapFilter('sanctioned')">Sanctioned &amp; Military</button>
            <button class="seg-btn" id="fltIncidents" onclick="setMapFilter('incidents')">AIS Incidents</button>
            <button class="seg-btn" id="fltShips" onclick="setMapFilter('ships')">Vessels</button>
            <button class="seg-btn" id="fltPlanes" onclick="setMapFilter('planes')">Aircraft</button>
          </div>
        </div>
        <div style="display:flex;align-items:center;gap:0.45rem;flex-wrap:wrap">
          <span class="panel-meta" id="mapCountBanner">Loading targets...</span>
          <button class="action-chip locate" id="btnNotableTracks" onclick="toggleNotableTracks()">Notable Trails</button>
          <button class="action-chip" id="btnClearTrack" style="display:none" onclick="clearTracks()">Clear Trail</button>
          <button class="action-chip" onclick="fitMapHighlights()">Fit Notable</button>
        </div>
      </div>
      <div id="combinedMap"></div>
      <div class="panel-bar" style="border-block-start:1px solid var(--line);border-block-end:none;padding-block:0.45rem">
        <div style="display:flex;gap:1rem;font-size:0.72rem;color:var(--ink-muted);flex-wrap:wrap">
          <span style="display:inline-flex;align-items:center;gap:0.35rem"><span class="dot" style="background:var(--signal-amber)"></span> Notable Vessel</span>
          <span style="display:inline-flex;align-items:center;gap:0.35rem"><span class="dot" style="background:var(--signal-rose)"></span> Notable Aircraft</span>
          <span style="display:inline-flex;align-items:center;gap:0.35rem"><span class="dot" style="background:var(--signal-green)"></span> AIS Vessel</span>
          <span style="display:inline-flex;align-items:center;gap:0.35rem"><span class="dot" style="background:var(--accent)"></span> ADS-B Aircraft</span>
        </div>
        <span class="panel-meta">1090 MHz Mode-S + 162 MHz AIS</span>
      </div>
    </div>

    <!-- RIGHT INSPECTOR: TODAY'S TOP NOTABLE AIRCRAFT & VESSELS -->
    <aside class="panel" aria-label="Notable catches of the day">
      <div class="panel-bar">
        <span class="panel-title">Notable Catches Today</span>
        <span class="panel-meta">Auto-ranked · Public DB links</span>
      </div>
      <div class="inspector-list">
        <div class="ins-group-label">
          <span>Top 3 Aircraft (1090 MHz)</span>
          <span>tar1090-db</span>
        </div>
        <div id="topPlanesContainer">
          <div class="ins-row"><span class="ins-desc">Loading aircraft records...</span></div>
        </div>
        <div class="ins-group-label" style="border-block-start:1px solid var(--line)">
          <span>Top 3 Vessels (162 MHz)</span>
          <span>AIS Static + Position</span>
        </div>
        <div id="topShipsContainer">
          <div class="ins-row"><span class="ins-desc">Loading vessel records...</span></div>
        </div>
      </div>
    </aside>
  </section>

  <!-- RECEIVER MODE MATRIX (SUBGRID) -->
  <div class="sec-bar">
    <h2>Receiver Demodulators &amp; Network Streams</h2>
    <span id="activeBanner">Checking active tuner lock...</span>
  </div>
  <div class="modes-grid" id="modesGrid"></div>

  <!-- UNIFIED TABBED LOGBOOK & TELEMETRY TABLES -->
  <section class="panel" id="logbookSection">
    <div class="panel-bar">
      <div style="display:flex;align-items:center;gap:0.75rem;flex-wrap:wrap">
        <div class="segmented" role="tablist" aria-label="Dataset view">
          <button class="seg-btn active" id="tabBtnLogbook" onclick="switchTableTab('logbook')">Saved Notable Logbook</button>
          <button class="seg-btn" id="tabBtnSts" onclick="switchTableTab('sts')">STS &amp; Loitering</button>
          <button class="seg-btn" id="tabBtnAircraft" onclick="switchTableTab('aircraft')">Recent Aircraft (adsb_flights)</button>
          <button class="seg-btn" id="tabBtnVessels" onclick="switchTableTab('vessels')">Recent Vessels (ais_state)</button>
        </div>
        <span class="panel-meta" id="logbookCountLabel"></span>
      </div>

      <div style="display:flex;align-items:center;gap:0.5rem;flex-wrap:wrap">
        <div class="segmented" id="logSubFilters">
          <button class="seg-btn active" id="logFltAll" onclick="setLogFilter('all')">All</button>
          <button class="seg-btn" id="logFltSanctioned" onclick="setLogFilter('sanctioned')">Sanctioned &amp; Shadow Fleet</button>
          <button class="seg-btn" id="logFltMilitary" onclick="setLogFilter('military')">RU / IR / Military Air</button>
          <button class="seg-btn" id="logFltIncidents" onclick="setLogFilter('incidents')">AIS Incidents</button>
          <button class="seg-btn" id="logFltShip" onclick="setLogFilter('ship')">Ships</button>
          <button class="seg-btn" id="logFltAircraft" onclick="setLogFilter('aircraft')">Aircraft</button>
          <button class="seg-btn" id="logFltNotable" onclick="setLogFilter('notable')">400m ULCS &amp; Quadjets</button>
        </div>
        <input class="ctrl-input" id="logSearchInput" type="search" placeholder="Filter by name, MMSI, ICAO, type..." oninput="renderActiveTable()">
        <a class="btn btn-subtle" href="/api/highlights/export.csv">Logbook CSV</a>
        <a class="btn btn-subtle" href="/api/adsb/export.csv">ADS-B CSV</a>
        <a class="btn btn-subtle" href="/api/ais/export.csv">AIS CSV</a>
      </div>
    </div>

    <div class="table-scroll">
      <table id="logbookTable">
        <thead id="unifiedTableHead">
          <tr>
            <th>Domain</th>
            <th>Identifier / Name</th>
            <th>Classification</th>
            <th>Specifications &amp; Notes</th>
            <th>Last Seen</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody id="unifiedTableBody">
          <tr><td colspan="6">Loading records from TimescaleDB...</td></tr>
        </tbody>
      </table>
    </div>
  </section>
</div>

<script>
const host = window.location.hostname;
let modesMeta = {};
let cyclerState = {enabled: false, interval_sec: 120};
let tunnelState = {enabled: false, url: null};
let switching = false;

let map = null;
let mapLayerGroup = null;
let trackLayerGroup = null;
let notableTracksActive = false;
let markerIndex = {};
let cachedMapData = {station: {}, aircraft: [], ships: [], highlights_log: []};
let cachedAdsb = {};
let cachedAis = {};
let cachedSts = [];
let currentMapFilter = 'all';
let currentLogFilter = 'all';
let currentTableTab = 'logbook';

function initMap() {
  if (map) return;
  map = L.map('combinedMap').setView([1.22, 103.80], 10);
  L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '&copy; OpenStreetMap contributors',
    maxZoom: 18
  }).addTo(map);
  trackLayerGroup = L.layerGroup().addTo(map);
  mapLayerGroup = L.layerGroup().addTo(map);
}

function drawPolylineTrack(domain, name, points, fitBounds = false) {
  if (!trackLayerGroup || !points || points.length < 2) return;
  const latlngs = points.map(pt => [pt.lat, pt.lon]);
  const color = domain === 'ship' ? '#f59e0b' : '#38bdf8';
  // Outer dark casing for high contrast over map tiles
  L.polyline(latlngs, {
    color: '#090d16',
    weight: 4.5,
    opacity: 0.75
  }).addTo(trackLayerGroup);
  // Inner semantic trajectory line
  const line = L.polyline(latlngs, {
    color: color,
    weight: 2.4,
    opacity: 0.92,
    dashArray: domain === 'aircraft' ? '6, 4' : null
  }).addTo(trackLayerGroup);
  line.bindTooltip(`${name} (${points.length} track pts)`, {sticky: true});

  // Draw historical waypoint dots
  const step = Math.max(1, Math.floor(points.length / 25));
  points.forEach((pt, idx) => {
    if (idx !== 0 && idx !== points.length - 1 && idx % step !== 0) return;
    const wp = L.circleMarker([pt.lat, pt.lon], {
      radius: idx === 0 || idx === points.length - 1 ? 3.8 : 2.4,
      color: '#090d16',
      weight: 1,
      fillColor: color,
      fillOpacity: 0.95
    });
    const meta = domain === 'ship'
      ? `${pt.speed ?? '—'} kt · ${pt.heading ?? '—'}°`
      : `${pt.alt_baro ?? '—'} ft · ${pt.speed ?? '—'} kt`;
    wp.bindTooltip(`<b>${name}</b><br>${pt.ts_utc}<br>${meta}`, {direction: 'top'});
    wp.addTo(trackLayerGroup);
  });

  document.getElementById('btnClearTrack').style.display = 'inline-flex';
  if (fitBounds && map && latlngs.length > 1) {
    map.fitBounds(latlngs, {padding: [45, 45], maxZoom: 12});
  }
}

async function showTargetTrack(domain, targetId, label, fitBounds = false) {
  initMap();
  notableTracksActive = false;
  document.getElementById('btnNotableTracks').classList.remove('active');
  trackLayerGroup.clearLayers();
  try {
    const res = await fetch(`/api/track?domain=${encodeURIComponent(domain)}&id=${encodeURIComponent(targetId)}&hours=48`);
    const data = await res.json();
    const pts = data.points || [];
    if (pts.length >= 2) {
      drawPolylineTrack(domain, label || targetId, pts, fitBounds);
    }
  } catch (e) {
    console.error('Track fetch error:', e);
  }
}

async function toggleNotableTracks() {
  initMap();
  if (notableTracksActive) {
    clearTracks();
    return;
  }
  notableTracksActive = true;
  trackLayerGroup.clearLayers();
  const btn = document.getElementById('btnNotableTracks');
  btn.textContent = 'Loading Trails...';
  try {
    const res = await fetch('/api/tracks/notable');
    const data = await res.json();
    const tracks = data.tracks || [];
    tracks.forEach(tr => {
      drawPolylineTrack(tr.domain, tr.name, tr.points, false);
    });
    btn.textContent = `Notable Trails (${tracks.length})`;
    document.getElementById('btnClearTrack').style.display = 'inline-flex';
  } catch (e) {
    btn.textContent = 'Notable Trails';
    notableTracksActive = false;
  }
}

function clearTracks() {
  if (trackLayerGroup) trackLayerGroup.clearLayers();
  notableTracksActive = false;
  document.getElementById('btnNotableTracks').textContent = 'Notable Trails';
  document.getElementById('btnClearTrack').style.display = 'none';
}

function buildPopupHtml(domain, item) {
  const linksHtml = (item.links || []).map(l =>
    `<a class="action-chip" href="${l.url}" target="_blank" rel="noopener">${l.label}</a>`
  ).join(' ');
  const trackLabel = domain === 'ship' ? (item.shipname || item.mmsi) : (item.callsign !== '—' ? item.callsign : item.hex);
  const trackId = domain === 'ship' ? item.mmsi : item.hex;
  const trackBtn = `<button class="action-chip locate" onclick="showTargetTrack('${domain}', '${trackId}', '${String(trackLabel).replace(/'/g, "")}', true)">Draw Track Trail</button>`;
  if (domain === 'ship') {
    return `
      <div style="min-inline-size:230px">
        <div class="num" style="font-weight:600;font-size:0.88rem;color:var(--ink-strong);margin-block-end:4px">${item.shipname}</div>
        <div style="margin-block-end:6px"><span class="tag-spec">${item.badge}</span></div>
        <div style="font-size:0.76rem;color:var(--ink);margin-block-end:6px">${item.reason}</div>
        <div class="num" style="font-size:0.72rem;color:var(--ink-muted);margin-block-end:8px">
          MMSI ${item.mmsi}${item.imo ? ' · IMO ' + item.imo : ''}<br>
          Hull ${item.len_m || '—'}m × ${item.beam_m || '—'}m · Draught ${item.draught || '—'}m<br>
          Speed ${item.speed ?? '—'} kt · Course ${item.course ?? '—'}°
        </div>
        <div style="display:flex;gap:4px;flex-wrap:wrap">${trackBtn} ${linksHtml}</div>
      </div>
    `;
  } else {
    const title = item.callsign && item.callsign !== '—' ? `${item.callsign} (${item.reg})` : `${item.hex} (${item.reg})`;
    return `
      <div style="min-inline-size:230px">
        <div class="num" style="font-weight:600;font-size:0.88rem;color:var(--ink-strong);margin-block-end:4px">${title}</div>
        <div style="margin-block-end:6px"><span class="tag-spec">${item.badge}</span></div>
        <div style="font-size:0.76rem;color:var(--ink);margin-block-end:6px">${item.reason}</div>
        <div class="num" style="font-size:0.72rem;color:var(--ink-muted);margin-block-end:8px">
          ICAO ${item.hex} · ${item.type_desc}<br>
          Alt ${item.alt_baro ?? '—'} ft · Speed ${item.gs ?? '—'} kt · Track ${item.track ?? '—'}°
        </div>
        <div style="display:flex;gap:4px;flex-wrap:wrap">${trackBtn} ${linksHtml}</div>
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

  const topShipIds = new Set(
    ships.filter(s => s.is_interesting).sort((a, b) => b.score - a.score).slice(0, 5).map(s => s.id)
  );
  if (['all', 'ships', 'highlights', 'sanctioned', 'incidents'].includes(currentMapFilter)) {
    ships.forEach(s => {
      if (currentMapFilter === 'highlights' && !s.is_interesting) return;
      if (currentMapFilter === 'sanctioned' && !['sanctioned_vessel', 'shadow_fleet'].includes(s.cat_key)) return;
      if (currentMapFilter === 'incidents' && s.cat_key !== 'ais_incident') return;
      shownShips++;
      if (s.is_interesting) hiCount++;

      let marker;
      if (s.is_interesting) {
        const showLabel = topShipIds.has(s.id);
        marker = L.circleMarker([s.lat, s.lon], {
          radius: 6.5,
          color: '#fde68a',
          weight: 1.8,
          fillColor: '#f59e0b',
          fillOpacity: 0.92
        });
        if (showLabel) {
          marker.bindTooltip(`${s.shipname}`, {
            permanent: true,
            direction: 'right',
            offset: [7, 0],
            className: 'map-tag ship-hi'
          });
        } else {
          marker.bindTooltip(`<b>${s.shipname}</b><br>${s.badge}`, {direction: 'top'});
        }
      } else {
        marker = L.circleMarker([s.lat, s.lon], {
          radius: 3.6,
          color: '#052e16',
          weight: 1,
          fillColor: '#22c55e',
          fillOpacity: 0.78
        });
        marker.bindTooltip(`${s.shipname} (${s.len_m || '—'}m)`, {direction: 'top'});
      }
      marker.bindPopup(buildPopupHtml('ship', s));
      marker.on('click', () => showTargetTrack('ship', s.id, s.shipname, false));
      marker.addTo(mapLayerGroup);
      markerIndex['ship:' + s.id] = marker;
    });
  }

  const topPlaneIds = new Set(
    planes.filter(p => p.is_interesting).sort((a, b) => b.score - a.score).slice(0, 5).map(p => p.id)
  );
  if (['all', 'planes', 'highlights', 'sanctioned'].includes(currentMapFilter)) {
    planes.forEach(p => {
      if (currentMapFilter === 'highlights' && !p.is_interesting) return;
      if (currentMapFilter === 'sanctioned' && !['sanctioned_aviation', 'military_gov'].includes(p.cat_key)) return;
      shownPlanes++;
      if (p.is_interesting) hiCount++;

      let marker;
      const lbl = p.callsign !== '—' ? p.callsign : (p.reg !== '—' ? p.reg : p.hex);
      if (p.is_interesting) {
        const showLabel = topPlaneIds.has(p.id);
        marker = L.circleMarker([p.lat, p.lon], {
          radius: 6.5,
          color: '#fecdd3',
          weight: 1.8,
          fillColor: '#f43f5e',
          fillOpacity: 0.92
        });
        if (showLabel) {
          marker.bindTooltip(`${lbl}`, {
            permanent: true,
            direction: 'right',
            offset: [7, 0],
            className: 'map-tag plane-hi'
          });
        } else {
          marker.bindTooltip(`<b>${lbl} (${p.reg})</b><br>${p.badge}`, {direction: 'top'});
        }
      } else {
        marker = L.circleMarker([p.lat, p.lon], {
          radius: 4.0,
          color: '#082f49',
          weight: 1,
          fillColor: '#38bdf8',
          fillOpacity: 0.85
        });
        marker.bindTooltip(`${lbl} (${p.type_code || p.hex})`, {direction: 'top'});
      }
      marker.bindPopup(buildPopupHtml('aircraft', p));
      marker.on('click', () => showTargetTrack('aircraft', p.id, lbl, false));
      marker.addTo(mapLayerGroup);
      markerIndex['aircraft:' + p.id] = marker;
    });
  }

  document.getElementById('mapCountBanner').textContent =
    `${shownPlanes} aircraft · ${shownShips} vessels (${hiCount} notable)`;
}

function setMapFilter(flt) {
  currentMapFilter = flt;
  ['All', 'Highlights', 'Sanctioned', 'Incidents', 'Ships', 'Planes'].forEach(k => {
    const el = document.getElementById('flt' + k);
    if (el) el.classList.toggle('active', k.toLowerCase() === flt);
  });
  renderCombinedMap();
}

function fitMapHighlights() {
  if (!map) return;
  const pts = [];
  (cachedMapData.ships || []).forEach(s => { if (s.is_interesting) pts.push([s.lat, s.lon]); });
  (cachedMapData.aircraft || []).forEach(p => { if (p.is_interesting) pts.push([p.lat, p.lon]); });
  if (pts.length > 0) {
    map.fitBounds(pts, {padding: [40, 40], maxZoom: 11});
  }
}

function locateOnMap(domain, targetId, lat, lon) {
  document.getElementById('mapSection').scrollIntoView({behavior: 'smooth', block: 'start'});
  if (currentMapFilter !== 'all') {
    setMapFilter('all');
  }
  showTargetTrack(domain, targetId, targetId, false);
  const key = domain + ':' + targetId;
  const m = markerIndex[key];
  if (m && map) {
    const ll = m.getLatLng();
    map.setView(ll, 12, {animate: true});
    setTimeout(() => m.openPopup(), 250);
  } else if (lat && lon && map) {
    map.setView([lat, lon], 12, {animate: true});
  }
}

async function showStsPairTracks(mmsiA, nameA, mmsiB, nameB, lat, lon) {
  document.getElementById('mapSection').scrollIntoView({behavior: 'smooth', block: 'start'});
  initMap();
  trackLayerGroup.clearLayers();
  if (lat && lon && map) {
    map.setView([lat, lon], 13, {animate: true});
  }
  try {
    const res1 = await fetch(`/api/track?domain=ship&id=${encodeURIComponent(mmsiA)}&hours=48`);
    const d1 = await res1.json();
    if ((d1.points || []).length >= 2) {
      drawPolylineTrack('ship', nameA || mmsiA, d1.points, false);
    }
    if (mmsiB) {
      const res2 = await fetch(`/api/track?domain=ship&id=${encodeURIComponent(mmsiB)}&hours=48`);
      const d2 = await res2.json();
      if ((d2.points || []).length >= 2) {
        drawPolylineTrack('aircraft', nameB || mmsiB, d2.points, false);
      }
    }
  } catch (e) {
    console.error('STS track error:', e);
  }
}

async function switchTableTab(tab) {
  currentTableTab = tab;
  document.getElementById('tabBtnLogbook').classList.toggle('active', tab === 'logbook');
  document.getElementById('tabBtnSts').classList.toggle('active', tab === 'sts');
  document.getElementById('tabBtnAircraft').classList.toggle('active', tab === 'aircraft');
  document.getElementById('tabBtnVessels').classList.toggle('active', tab === 'vessels');
  document.getElementById('logSubFilters').style.display = tab === 'logbook' ? 'inline-flex' : 'none';
  if (tab === 'sts' && cachedSts.length === 0) {
    try {
      const res = await fetch('/api/sts');
      const data = await res.json();
      cachedSts = data.events || [];
    } catch (e) {}
  }
  renderActiveTable();
}

function setLogFilter(flt) {
  currentLogFilter = flt;
  document.getElementById('logFltAll').classList.toggle('active', flt === 'all');
  document.getElementById('logFltSanctioned').classList.toggle('active', flt === 'sanctioned');
  document.getElementById('logFltMilitary').classList.toggle('active', flt === 'military');
  document.getElementById('logFltIncidents').classList.toggle('active', flt === 'incidents');
  document.getElementById('logFltShip').classList.toggle('active', flt === 'ship');
  document.getElementById('logFltAircraft').classList.toggle('active', flt === 'aircraft');
  document.getElementById('logFltNotable').classList.toggle('active', flt === 'notable');
  renderActiveTable();
}

function renderActiveTable() {
  const thead = document.getElementById('unifiedTableHead');
  const tbody = document.getElementById('unifiedTableBody');
  const q = (document.getElementById('logSearchInput').value || '').trim().toLowerCase();

  if (currentTableTab === 'logbook') {
    thead.innerHTML = `
      <tr>
        <th>Domain</th>
        <th>Identifier / Name</th>
        <th>Classification</th>
        <th>Specifications &amp; Notes</th>
        <th>Last Seen</th>
        <th>Actions</th>
      </tr>
    `;
    const rows = cachedMapData.highlights_log || [];
    const filtered = rows.filter(r => {
      if (currentLogFilter === 'sanctioned' && !['sanctioned_vessel', 'shadow_fleet', 'sanctioned_aviation'].includes(r.category_key)) return false;
      if (currentLogFilter === 'military' && !['sanctioned_aviation', 'military_gov'].includes(r.category_key)) return false;
      if (currentLogFilter === 'incidents' && r.category_key !== 'ais_incident') return false;
      if (currentLogFilter === 'ship' && r.domain !== 'ship') return false;
      if (currentLogFilter === 'aircraft' && r.domain !== 'aircraft') return false;
      if (currentLogFilter === 'notable' && !['megaship_400m', 'quadjet', 'bizjet', 'vlcc_supertanker'].includes(r.category_key)) return false;
      if (q) {
        const hay = `${r.name_or_reg} ${r.target_id} ${r.secondary_id} ${r.badge} ${r.reason}`.toLowerCase();
        if (!hay.includes(q)) return false;
      }
      return true;
    });
    document.getElementById('logbookCountLabel').textContent = `${filtered.length} of ${rows.length} entries`;
    if (filtered.length === 0) {
      tbody.innerHTML = '<tr><td colspan="6" style="color:var(--ink-muted)">No matching entries in logbook.</td></tr>';
      return;
    }
    tbody.innerHTML = filtered.map(r => {
      const domainLabel = r.domain === 'ship' ? 'Vessel' : 'Aircraft';
      const mapBtn = (r.lat && r.lon)
        ? `<button class="action-chip locate" onclick="locateOnMap('${r.domain}', '${r.target_id}', ${r.lat}, ${r.lon})">Locate</button>`
        : '';
      const pubLinks = (r.links || []).slice(0, 2).map(l =>
        `<a class="action-chip" href="${l.url}" target="_blank" rel="noopener">${l.label}</a>`
      ).join(' ');
      return `
        <tr>
          <td class="num" style="color:var(--ink-muted)">${domainLabel}</td>
          <td>
            <div class="num" style="font-weight:600;color:var(--ink-strong)">${r.name_or_reg}</div>
            <div class="num" style="font-size:0.7rem;color:var(--ink-faint)">${r.secondary_id || r.target_id}</div>
          </td>
          <td><span class="tag-spec">${r.badge}</span></td>
          <td>
            <div style="color:var(--ink)">${r.reason}</div>
            <div class="num" style="font-size:0.7rem;color:var(--ink-muted)">${r.metric_summary || ''}</div>
          </td>
          <td class="num" style="font-size:0.73rem;color:var(--ink-muted)">${r.last_seen_utc}</td>
          <td><div style="display:flex;gap:4px;flex-wrap:wrap">${mapBtn} ${pubLinks}</div></td>
        </tr>
      `;
    }).join('');
  } else if (currentTableTab === 'sts') {
    thead.innerHTML = `
      <tr>
        <th>Anomaly Type</th>
        <th>Primary &amp; Partner Vessel</th>
        <th>Classification</th>
        <th>Kinematics &amp; Proximity Details</th>
        <th>Last Seen</th>
        <th>Actions</th>
      </tr>
    `;
    const evs = (cachedSts || []).filter(e => {
      if (!q) return true;
      return `${e.primary_name} ${e.primary_mmsi} ${e.secondary_name || ''} ${e.secondary_mmsi || ''} ${e.badge} ${e.summary}`.toLowerCase().includes(q);
    });
    document.getElementById('logbookCountLabel').textContent = `${evs.length} STS & loitering events`;
    if (evs.length === 0) {
      tbody.innerHTML = '<tr><td colspan="6" style="color:var(--ink-muted)">No STS rendezvous or offshore loitering events matching filter.</td></tr>';
      return;
    }
    tbody.innerHTML = evs.map(e => {
      const typeLabel = e.event_type === 'sts_transfer' ? 'STS Rendezvous' : 'Offshore Loitering';
      const title = e.secondary_name ? `${e.primary_name} ↔ ${e.secondary_name}` : e.primary_name;
      const subId = e.secondary_mmsi ? `MMSI ${e.primary_mmsi} / ${e.secondary_mmsi}` : `MMSI ${e.primary_mmsi}`;
      const pNameClean = String(e.primary_name || '').replace(/'/g, "");
      const sNameClean = String(e.secondary_name || '').replace(/'/g, "");
      const traceBtn = `<button class="action-chip locate" onclick="showStsPairTracks('${e.primary_mmsi}', '${pNameClean}', '${e.secondary_mmsi || ''}', '${sNameClean}', ${e.lat || 'null'}, ${e.lon || 'null'})">Locate &amp; Draw Trails</button>`;
      return `
        <tr>
          <td class="num" style="color:var(--signal-amber)">${typeLabel}</td>
          <td>
            <div class="num" style="font-weight:600;color:var(--ink-strong)">${title}</div>
            <div class="num" style="font-size:0.7rem;color:var(--ink-faint)">${subId}</div>
          </td>
          <td><span class="tag-spec">${e.badge}</span></td>
          <td>${e.summary}</td>
          <td class="num" style="font-size:0.73rem;color:var(--ink-muted)">${e.last_seen_utc}</td>
          <td><div style="display:flex;gap:4px;flex-wrap:wrap">${traceBtn}</div></td>
        </tr>
      `;
    }).join('');
  } else if (currentTableTab === 'aircraft') {
    thead.innerHTML = `
      <tr>
        <th>ICAO Hex</th>
        <th>Callsign</th>
        <th>Registration &amp; Type</th>
        <th>Altitude Band (ft)</th>
        <th>Max Speed</th>
        <th>Lookup</th>
      </tr>
    `;
    const flights = (cachedAdsb.recent_flights || []).filter(f => {
      if (!q) return true;
      return `${f.hex} ${f.callsign} ${f.reg} ${f.type_code} ${f.type_desc}`.toLowerCase().includes(q);
    });
    document.getElementById('logbookCountLabel').textContent = `${flights.length} recent flights`;
    tbody.innerHTML = flights.map(f => {
      const lnk = (f.links || []).map(l => `<a class="action-chip" href="${l.url}" target="_blank">${l.label}</a>`).join(' ');
      return `
        <tr>
          <td class="num" style="font-weight:600;color:var(--ink-strong)">${(f.hex || '').toUpperCase()}</td>
          <td class="num">${f.callsign || '—'}</td>
          <td><span class="num" style="color:var(--ink-strong)">${f.reg !== '—' ? f.reg : ''}</span> <span style="color:var(--ink-muted)">${f.type_desc || f.type_code || '—'}</span></td>
          <td class="num">${f.min_alt ?? '—'} – ${f.max_alt ?? '—'}</td>
          <td class="num">${f.max_gs ? f.max_gs + ' kt' : '—'}</td>
          <td><div style="display:flex;gap:4px;flex-wrap:wrap">${lnk}</div></td>
        </tr>
      `;
    }).join('');
  } else {
    thead.innerHTML = `
      <tr>
        <th>MMSI</th>
        <th>Vessel Name</th>
        <th>Dimensions</th>
        <th>Draught</th>
        <th>Destination</th>
        <th>Speed &amp; Lookup</th>
      </tr>
    `;
    const vessels = (cachedAis.recent_vessels || []).filter(v => {
      if (!q) return true;
      return `${v.mmsi} ${v.shipname} ${v.destination}`.toLowerCase().includes(q);
    });
    document.getElementById('logbookCountLabel').textContent = `${vessels.length} recent vessels`;
    tbody.innerHTML = vessels.map(v => {
      const lnk = (v.links || []).slice(0, 2).map(l => `<a class="action-chip" href="${l.url}" target="_blank">${l.label}</a>`).join(' ');
      return `
        <tr>
          <td class="num">${v.mmsi}</td>
          <td class="num" style="font-weight:600;color:var(--ink-strong)">${v.shipname}</td>
          <td class="num">${v.len_m ? v.len_m + 'm × ' + (v.beam_m || '—') + 'm' : '—'}</td>
          <td class="num">${v.draught ? v.draught + 'm' : '—'}</td>
          <td class="num" style="color:var(--ink-muted)">${v.destination}</td>
          <td><div style="display:flex;gap:4px;flex-wrap:wrap"><span class="num" style="margin-inline-end:6px">${v.speed ?? '—'} kt</span>${lnk}</div></td>
        </tr>
      `;
    }).join('');
  }
}

function renderTunnel(tun) {
  tunnelState = tun || {enabled: false};
  const dot = document.getElementById('tunnelDot');
  const lbl = document.getElementById('tunnelLabel');
  const link = document.getElementById('tunnelUrlLink');
  const copyBtn = document.getElementById('tunnelCopyBtn');
  const toggleBtn = document.getElementById('tunnelToggleBtn');

  if (tun && tun.enabled) {
    dot.className = 'dot live';
    if (tun.url) {
      lbl.textContent = 'Tunnel:';
      link.style.display = 'inline';
      link.href = tun.url;
      link.textContent = tun.url.replace('https://', '');
      copyBtn.style.display = 'inline-flex';
    } else {
      lbl.textContent = tun.status || 'Starting tunnel...';
      link.style.display = 'none';
      copyBtn.style.display = 'none';
    }
    toggleBtn.textContent = 'Stop';
  } else {
    dot.className = 'dot';
    lbl.textContent = 'Quick Tunnel:';
    link.style.display = 'none';
    copyBtn.style.display = 'none';
    toggleBtn.textContent = 'Share via Cloudflare';
  }
}

async function toggleTunnel() {
  const nextState = !(tunnelState && tunnelState.enabled);
  document.getElementById('tunnelLabel').textContent = nextState ? 'Provisioning trycloudflare.com...' : 'Stopping...';
  const res = await fetch('/api/tunnel', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({enabled: nextState, port: 8080})
  });
  const data = await res.json();
  renderTunnel(data.tunnel);
}

function copyTunnelUrl() {
  if (tunnelState && tunnelState.url) {
    navigator.clipboard.writeText(tunnelState.url).catch(() => {});
    const btn = document.getElementById('tunnelCopyBtn');
    btn.textContent = 'Copied';
    setTimeout(() => { btn.textContent = 'Copy URL'; }, 1500);
  }
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
  cachedAdsb = data.adsb || {};
  cachedAis = data.ais || {};
  const sys = data.system;
  cyclerState = sys.cycler || {enabled: false, interval_sec: 120};

  document.getElementById('usbName').textContent = sys.usb_connected
    ? 'Nooelec NESDR SMArt v5 · Raspberry Pi CM4 · TimescaleDB'
    : 'USB SDR Unplugged · Raspberry Pi CM4';
  document.getElementById('usbDot').className = 'dot ' + (sys.usb_connected ? 'live' : 'amber');
  document.getElementById('cpuTemp').textContent = sys.cpu_temp_c ? sys.cpu_temp_c + '°C' : 'N/A';
  document.getElementById('lanIp').textContent = sys.lan_connected ? 'Connected' : 'Offline';
  const tsEl = document.getElementById('tsIp');
  if (sys.tailscale_connected) {
    tsEl.textContent = 'Connected';
  } else if (sys.tailscale_auth_url) {
    tsEl.innerHTML = `<a href="${sys.tailscale_auth_url}" target="_blank" style="color:var(--accent)">Authenticate</a>`;
  } else {
    tsEl.textContent = sys.tailscale_status;
  }

  renderTunnel(sys.tunnel);
  renderCycler(cyclerState, sys.active_mode);

  const activeName = sys.active_mode !== 'off' && modesMeta[sys.active_mode]
    ? modesMeta[sys.active_mode].title
    : 'Standby (Tuner Idle)';
  document.getElementById('activeBanner').textContent = 'Active Hardware Lock: ' + activeName;

  renderModes(modesMeta, sys.modes_status);
  renderHighlights(cachedAdsb, cachedAis);
  renderAnalytics(cachedAdsb, cachedAis);
  if (currentTableTab !== 'logbook') {
    renderActiveTable();
  }
}

async function fetchMapAndLog() {
  try {
    const res = await fetch('/api/map');
    cachedMapData = await res.json();
    renderCombinedMap();
    if (currentTableTab === 'logbook') {
      renderActiveTable();
    }
  } catch (e) {
    console.error('Map fetch error:', e);
  }
}

function renderCycler(cyc, activeMode) {
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
    dot.className = 'dot live';
    head.textContent = `Dual-Band Auto-Cycler (${mins}m window · Active)`;
    sub.innerHTML = `Active receiver: <strong class="num" style="color:var(--ink-strong)">${(activeMode || cyc.current_target).toUpperCase()}</strong> · Next switch to <strong class="num" style="color:var(--ink-strong)">${(cyc.next_target || '').toUpperCase()}</strong> in <span class="num">${cyc.seconds_remaining ?? '--'}s</span> (${cyc.cycles_completed || 0} cycles completed)`;
    btn.textContent = 'Pause Cycler';
    btn.className = 'btn';
  } else {
    dot.className = 'dot amber';
    head.textContent = `Dual-Band Auto-Cycler (Paused · Locked to ${activeMode.toUpperCase()})`;
    sub.textContent = `Tuner dedicated to single receiver. Enable cycler to alternate 1090 MHz ADS-B and 162 MHz AIS every ${mins} minutes.`;
    btn.textContent = 'Resume Auto-Cycler';
    btn.className = 'btn btn-primary';
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
    planesBox.innerHTML = '<div class="ins-row"><span class="ins-desc">No notable aircraft recorded yet.</span></div>';
  } else {
    planesBox.innerHTML = planes.map(p => `
      <div class="ins-row">
        <div class="ins-head">
          <span class="ins-name">${p.callsign !== '—' ? p.callsign : p.hex} (${p.reg})</span>
          <span class="tag-spec">${p.badge}</span>
        </div>
        <div class="ins-desc">${p.reason}</div>
        <div class="ins-actions">
          <button class="action-chip locate" onclick="locateOnMap('aircraft', '${p.hex}', null, null)">Locate</button>
          ${(p.links || []).map(l => `<a class="action-chip" href="${l.url}" target="_blank" rel="noopener">${l.label}</a>`).join('')}
        </div>
      </div>
    `).join('');
  }

  const shipsBox = document.getElementById('topShipsContainer');
  const ships = ais.top_interesting_ships || [];
  if (ships.length === 0) {
    shipsBox.innerHTML = '<div class="ins-row"><span class="ins-desc">No notable vessels recorded yet.</span></div>';
  } else {
    shipsBox.innerHTML = ships.map(s => `
      <div class="ins-row">
        <div class="ins-head">
          <span class="ins-name">${s.shipname}</span>
          <span class="tag-spec">${s.badge}</span>
        </div>
        <div class="ins-desc">${s.reason}</div>
        <div class="ins-actions">
          ${(s.lat && s.lon) ? `<button class="action-chip locate" onclick="locateOnMap('ship', '${s.mmsi}', ${s.lat}, ${s.lon})">Locate</button>` : `<span class="action-chip" style="opacity:0.6;cursor:default">Static Only</span>`}
          ${(s.links || []).map(l => `<a class="action-chip" href="${l.url}" target="_blank" rel="noopener">${l.label}</a>`).join('')}
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
    card.className = 'mode-card';
    card.setAttribute('data-active', isAct ? 'true' : 'false');
    card.innerHTML = `
      <div class="mode-top">
        <div>
          <div class="mode-name">${m.title}</div>
          <div class="mode-freq">${m.badge} · ${m.stack}</div>
        </div>
        <span class="mode-state ${isAct ? 'on' : ''}">
          <span class="dot ${isAct ? 'live' : ''}"></span>
          ${isAct ? 'Active' : 'Standby'}
        </span>
      </div>
      <div class="mode-body">${m.description}</div>
      <div class="mode-foot">
        <span class="mode-endpoint" title="${m.PortLabel}">${m.PortLabel}</span>
        <div style="display:flex;gap:0.4rem">
          ${isAct
            ? `<button class="btn" disabled>Assigned</button>`
            : `<button class="btn" onclick="switchMode('${id}')">Lock Tuner</button>`
          }
          ${url
            ? `<a class="btn btn-primary" href="${url}" target="_blank" rel="noopener">Open UI</a>`
            : `<button class="btn btn-subtle" onclick="copyEndpoint('${id}', ${m.port})">Copy :${m.port}</button>`
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
  alert('Connect your SDR client (SDR++ or Gqrx) to: ' + txt);
}

async function switchMode(modeId) {
  if (switching) return;
  switching = true;
  document.getElementById('activeBanner').textContent = 'Assigning hardware tuner to ' + modeId.toUpperCase() + '...';
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
  document.getElementById('stAdsbAc').textContent = `${adsb.unique_aircraft || 0} aircraft · ${adsb.total_flights || 0} flights`;
  document.getElementById('stAdsbRange').textContent = adsb.max_altitude_ft ? `${adsb.max_altitude_ft.toLocaleString()} ft` : '—';
  document.getElementById('stAdsbAlt').textContent = `${adsb.avg_rssi_dbfs ?? '—'} dBFS · ${adsb.max_speed_kt ?? '—'} kt`;

  document.getElementById('stAisVessels').textContent = (ais.unique_vessels || 0).toLocaleString();
  document.getElementById('stAisNamed').textContent = `${ais.named_vessels || 0} named vessels`;
  document.getElementById('stDbSize').textContent = ais.timescaledb_size || '—';
  document.getElementById('stAisMsgs').textContent = (ais.total_messages || 0).toLocaleString() + ' AIS msgs';
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
        elif parsed.path == "/api/track":
            qs = parse_qs(parsed.query)
            domain = (qs.get("domain", [""])[0]).strip()
            target_id = (qs.get("id", [""])[0]).strip()
            hours = int(qs.get("hours", ["48"])[0] or "48")
            self._send_json(get_target_track(domain, target_id, hours=hours))
        elif parsed.path == "/api/tracks/notable":
            self._send_json(get_notable_tracks(max_targets=14))
        elif parsed.path == "/api/sts":
            qs = parse_qs(parsed.query)
            force = qs.get("force", ["0"])[0] in ("1", "true")
            events = run_sts_detection(force=force)
            self._send_json({"count": len(events), "events": events})
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
        elif parsed.path == "/api/tunnel":
            en = bool(body.get("enabled", False))
            target_port = int(body.get("port", PORT))
            if en:
                t_status = TUNNEL.start(target_port=target_port)
            else:
                t_status = TUNNEL.stop()
            self._send_json({"ok": True, "tunnel": t_status})
        else:
            self.send_error(HTTPStatus.NOT_FOUND)


def main():
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), LauncherHandler)
    print(f"[sdr-launcher] Listening on 0.0.0.0:{PORT}", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
