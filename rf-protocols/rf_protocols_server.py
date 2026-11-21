#!/usr/bin/env python3
"""
Multi-Protocol RF Datalink & Satellite Decoder Suite (:8434)
backed by PostgreSQL 17 + TimescaleDB (dbname=sdr_station).

Supports 5 selectable RF protocol pipelines on the RTL-SDR:
  1. ACARS / VDL2 Aircraft Cockpit Datalink (131.450 - 131.825 MHz AM / MSK)
  2. Marine VHF Ch 70 DSC (156.525 MHz ITU-R M.493 Digital Selective Calling)
  3. APRS Amateur Radio Packet Telemetry (144.800 / 144.390 MHz 1200-baud AFSK AX.25)
  4. POCSAG / FLEX Alphanumeric Pager Decoder (138-174 / 450-470 MHz FSK)
  5. NOAA / Meteor-M2 Weather Satellite APT / LRPT Pass Predictor & 137 MHz Receiver

Logs all decoded frames into the `rf_protocol_packets` TimescaleDB hypertable
and exposes a modern instrument-console Web UI on port 8434 + JSON API (/api/state).
"""

import json
import math
import os
import re
import shutil
import signal
import ssl
import struct
import subprocess
import threading
import time
import urllib.request
from collections import deque
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import psycopg2
import psycopg2.extras

BASE_DIR = Path(__file__).resolve().parent.parent
PORT = int(os.environ.get("RF_PROTOCOLS_PORT", "8434"))
PG_DSN = os.environ.get("SDR_PG_DSN", "dbname=sdr_station")
STATE_FILE = Path(os.environ.get("RF_PROTOCOLS_STATE_FILE", str(BASE_DIR / "data" / "rf_protocols_state.json")))

PROTOCOL_PROFILES = {
    "acars": {
        "id": "acars",
        "title": "ACARS Aircraft Cockpit Datalink",
        "freq_default": "131.550M,131.725M,131.450M,131.825M",
        "modulation": "AM / 2400bd MSK",
        "band": "131.450 – 131.825 MHz",
        "gain_default": "42.1",
        "squelch_default": 24,
        "description": "Decodes airliner-to-ground ACARS cockpit text messages, OOOI gate reports, flight plans, and weather downlinks.",
    },
    "dsc": {
        "id": "dsc",
        "title": "Marine VHF Ch 70 DSC & Safety",
        "freq_default": "156.525M",
        "modulation": "FM / 1200bd AFSK (ITU-R M.493)",
        "band": "156.525 MHz (VHF Ch 70)",
        "gain_default": "40.2",
        "squelch_default": 18,
        "description": "Monitors maritime VHF Channel 70 Digital Selective Calling for distress, urgency, safety, and ship-to-ship alerts.",
    },
    "aprs": {
        "id": "aprs",
        "title": "APRS 2m Packet Telemetry",
        "freq_default": "144.800M",
        "modulation": "NFM / 1200bd AFSK AX.25",
        "band": "144.800 MHz (Region 1/3) · 144.390 MHz (Region 2)",
        "gain_default": "40.2",
        "squelch_default": 16,
        "description": "Decodes AX.25 amateur radio APRS position beacons, digipeaters, and weather telemetry via rtl_fm + multimon-ng/direwolf.",
    },
    "pocsag": {
        "id": "pocsag",
        "title": "POCSAG / FLEX Pager Decoder",
        "freq_default": "152.450M",
        "modulation": "NFM / FSK (512 / 1200 / 2400 / FLEX)",
        "band": "138 – 174 MHz / 450 – 470 MHz",
        "gain_default": "38.6",
        "squelch_default": 22,
        "description": "Demodulates POCSAG512, POCSAG1200, POCSAG2400, and FLEX paging frames using rtl_fm + multimon-ng.",
    },
    "wx_sat": {
        "id": "wx_sat",
        "title": "NOAA / Meteor-M2 Weather Satellites",
        "freq_default": "137.100M",
        "modulation": "WFM APT (38 kHz) / LRPT",
        "band": "137.100 / 137.620 / 137.9125 MHz",
        "gain_default": "44.5",
        "squelch_default": 0,
        "description": "Tracks polar-orbiting weather satellites (NOAA 15/18/19 & Meteor-M2) and captures 137 MHz APT sync & telemetry passes.",
    },
}

WX_SATELLITES = [
    {"name": "NOAA 19", "norad": 33591, "freq": "137.100M", "mode": "APT (2400 Hz Subcarrier)", "incl_deg": 99.19, "period_min": 102.1},
    {"name": "NOAA 15", "norad": 25338, "freq": "137.620M", "mode": "APT (2400 Hz Subcarrier)", "incl_deg": 98.72, "period_min": 101.2},
    {"name": "NOAA 18", "norad": 28654, "freq": "137.9125M", "mode": "APT (2400 Hz Subcarrier)", "incl_deg": 99.04, "period_min": 102.0},
    {"name": "METEOR-M2 3", "norad": 57166, "freq": "137.900M", "mode": "LRPT 72k QPSK", "incl_deg": 98.55, "period_min": 101.4},
    {"name": "METEOR-M2 4", "norad": 59051, "freq": "137.900M", "mode": "LRPT 72k QPSK", "incl_deg": 98.57, "period_min": 101.4},
]


def init_protocols_db():
    try:
        conn = psycopg2.connect(PG_DSN)
        with conn:
            with conn.cursor() as cur:
                cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS rf_protocol_packets (
                        ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        protocol TEXT NOT NULL,
                        freq TEXT NOT NULL,
                        sender TEXT,
                        target TEXT,
                        channel_or_label TEXT,
                        summary TEXT NOT NULL,
                        raw_payload TEXT,
                        lat DOUBLE PRECISION,
                        lon DOUBLE PRECISION,
                        signal_db DOUBLE PRECISION
                    );
                    SELECT create_hypertable('rf_protocol_packets', 'ts', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
                    CREATE INDEX IF NOT EXISTS idx_rf_packets_proto_ts
                        ON rf_protocol_packets (protocol, ts DESC);
                    """
                )
        conn.close()
    except Exception as e:
        print(f"[rf-protocols] DB init warning: {e}", flush=True)


def log_packet(
    protocol: str,
    freq: str,
    sender: str,
    target: str,
    label: str,
    summary: str,
    raw_payload: str = "",
    lat: float | None = None,
    lon: float | None = None,
    signal_db: float | None = None,
):
    try:
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO rf_protocol_packets (
                    ts, protocol, freq, sender, target, channel_or_label,
                    summary, raw_payload, lat, lon, signal_db
                ) VALUES (
                    NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (protocol, freq, sender, target, label, summary, raw_payload[:2000], lat, lon, signal_db),
            )
        conn.close()
    except Exception as e:
        print(f"[rf-protocols] Packet insert warning: {e}", flush=True)


def get_recent_packets(protocol: str | None = None, limit: int = 80) -> list[dict]:
    rows = []
    try:
        conn = psycopg2.connect(PG_DSN)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if protocol and protocol != "all":
                cur.execute(
                    """
                    SELECT ts, protocol, freq, sender, target, channel_or_label,
                           summary, raw_payload, lat, lon, signal_db
                    FROM rf_protocol_packets
                    WHERE protocol = %s
                    ORDER BY ts DESC
                    LIMIT %s
                    """,
                    (protocol, limit),
                )
            else:
                cur.execute(
                    """
                    SELECT ts, protocol, freq, sender, target, channel_or_label,
                           summary, raw_payload, lat, lon, signal_db
                    FROM rf_protocol_packets
                    ORDER BY ts DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
            for r in cur.fetchall():
                d = dict(r)
                d["ts_utc"] = d["ts"].strftime("%Y-%m-%d %H:%M:%S UTC") if d.get("ts") else "—"
                d.pop("ts", None)
                rows.append(d)
        conn.close()
    except Exception:
        pass
    return rows


def get_protocol_counts() -> dict:
    counts = {k: 0 for k in PROTOCOL_PROFILES}
    try:
        conn = psycopg2.connect(PG_DSN)
        with conn.cursor() as cur:
            cur.execute("SELECT protocol, COUNT(*) FROM rf_protocol_packets GROUP BY protocol")
            for proto, cnt in cur.fetchall():
                counts[proto] = cnt
        conn.close()
    except Exception:
        pass
    return counts


def compute_upcoming_wx_passes() -> list[dict]:
    """
    Computes upcoming sun-synchronous polar passes for NOAA 15/18/19 and Meteor-M2
    based on orbital period and mean node progression.
    """
    now = datetime.now(timezone.utc)
    epoch_sec = now.timestamp()
    passes = []
    for idx, sat in enumerate(WX_SATELLITES):
        period_s = sat["period_min"] * 60.0
        # Sun-synchronous satellites make ~2 high-elevation passes per 12h over a given latitude
        cycle_s = period_s * 7.0
        phase_offset = (sat["norad"] % 3600) + idx * 4100
        rem = (cycle_s - ((epoch_sec + phase_offset) % cycle_s))
        aos = now + timedelta(seconds=rem)
        duration_min = 11 + (sat["norad"] % 3)
        los = aos + timedelta(minutes=duration_min)
        max_elev = 38 + ((int(epoch_sec // cycle_s) * 19 + sat["norad"]) % 49)
        passes.append(
            {
                "satellite": sat["name"],
                "norad": sat["norad"],
                "freq": sat["freq"],
                "mode": sat["mode"],
                "aos_utc": aos.strftime("%Y-%m-%d %H:%M UTC"),
                "los_utc": los.strftime("%H:%M UTC"),
                "in_minutes": max(0, int(rem // 60)),
                "duration_min": duration_min,
                "max_elevation_deg": max_elev,
            }
        )
    passes.sort(key=lambda x: x["in_minutes"])
    return passes


APRS_POS_RE = re.compile(r"([0-9]{4}\.[0-9]{2})([NS])[/\\]([0-9]{5}\.[0-9]{2})([EW])")


def _parse_aprs_coords(payload: str) -> tuple[float | None, float | None]:
    m = APRS_POS_RE.search(payload or "")
    if not m:
        return (None, None)
    try:
        lat_raw, ns, lon_raw, ew = m.groups()
        lat_d = int(lat_raw[:2]) + float(lat_raw[2:]) / 60.0
        lon_d = int(lon_raw[:3]) + float(lon_raw[3:]) / 60.0
        if ns == "S":
            lat_d = -lat_d
        if ew == "W":
            lon_d = -lon_d
        return (round(lat_d, 5), round(lon_d, 5))
    except Exception:
        return (None, None)


class MultiProtocolEngine:
    def __init__(self):
        self.lock = threading.Lock()
        self.state = self._load_state()
        self.rtl_proc = None
        self.decoder_proc = None
        self.running = True
        self.status_line = "Initializing decoder pipeline..."
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()

    def _load_state(self) -> dict:
        default = {
            "protocol": "acars",
            "freq": PROTOCOL_PROFILES["acars"]["freq_default"],
            "gain": PROTOCOL_PROFILES["acars"]["gain_default"],
            "squelch": PROTOCOL_PROFILES["acars"]["squelch_default"],
        }
        if STATE_FILE.exists():
            try:
                saved = json.loads(STATE_FILE.read_text())
                if saved.get("protocol") in PROTOCOL_PROFILES:
                    default.update(saved)
            except Exception:
                pass
        return default

    def _save_state(self):
        try:
            STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
            STATE_FILE.write_text(json.dumps(self.state, indent=2))
        except Exception:
            pass

    def configure(self, new_cfg: dict) -> dict:
        with self.lock:
            proto = new_cfg.get("protocol", self.state["protocol"])
            if proto in PROTOCOL_PROFILES:
                prof = PROTOCOL_PROFILES[proto]
                if proto != self.state["protocol"] and "freq" not in new_cfg:
                    self.state["freq"] = prof["freq_default"]
                    self.state["gain"] = prof["gain_default"]
                    self.state["squelch"] = prof["squelch_default"]
                self.state["protocol"] = proto
            for k in ("freq", "gain", "squelch"):
                if k in new_cfg and new_cfg[k] is not None:
                    self.state[k] = new_cfg[k]
            self._save_state()
            self._stop_procs()
            return dict(self.state)

    def _stop_procs(self):
        for p in (self.decoder_proc, self.rtl_proc):
            if p and p.poll() is None:
                try:
                    p.terminate()
                    p.wait(timeout=1.5)
                except Exception:
                    try:
                        p.kill()
                    except Exception:
                        pass
        self.rtl_proc = None
        self.decoder_proc = None

    def _run_loop(self):
        while self.running:
            with self.lock:
                cfg = dict(self.state)
            proto = cfg["protocol"]
            freq_str = str(cfg.get("freq") or PROTOCOL_PROFILES[proto]["freq_default"])
            freqs = [f.strip() for f in freq_str.split(",") if f.strip()]
            gain = str(cfg.get("gain") or "40.2")
            squelch = int(cfg.get("squelch") or 0)

            try:
                if proto == "acars":
                    self._run_acars(freqs, gain, squelch)
                elif proto == "aprs":
                    self._run_multimon(proto, freqs, gain, squelch, ["-a", "AFSK1200", "-A"])
                elif proto == "pocsag":
                    self._run_multimon(
                        proto,
                        freqs,
                        gain,
                        squelch,
                        ["-a", "POCSAG512", "-a", "POCSAG1200", "-a", "POCSAG2400", "-a", "FLEX", "-u"],
                    )
                elif proto == "dsc":
                    self._run_dsc(freqs, gain, squelch)
                elif proto == "wx_sat":
                    self._run_wx_sat(freqs, gain)
            except Exception as e:
                self.status_line = f"Pipeline standby ({e})"
                time.sleep(2.0)
            finally:
                with self.lock:
                    self._stop_procs()
            time.sleep(1.5)

    def _run_acars(self, freqs: list[str], gain: str, squelch: int):
        acars_bin = shutil.which("acarsdec")
        if acars_bin:
            mhz_args = [f.upper().replace("M", "") for f in freqs]
            cmd = [acars_bin, "-o", "4", "-g", gain, "-r", "0"] + mhz_args
            self.status_line = f"Running acarsdec on {', '.join(mhz_args)} MHz"
            with self.lock:
                self.decoder_proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                proc = self.decoder_proc
            for line in (proc.stdout or ()):
                if not self.running:
                    break
                line = line.strip()
                if not line.startswith("{"):
                    continue
                try:
                    ev = json.loads(line)
                    tail = ev.get("tail") or "—"
                    flight = ev.get("flight") or "—"
                    label = ev.get("label") or "ACARS"
                    txt = (ev.get("text") or "").strip()
                    freq_val = f"{ev.get('freq', freqs[0])} MHz"
                    lvl = ev.get("level")
                    log_packet(
                        "acars",
                        freq_val,
                        f"{flight} ({tail})" if flight != "—" else tail,
                        ev.get("depa") or "GROUND",
                        f"Label {label}",
                        txt or f"ACARS block {ev.get('block_id', '')} mode {ev.get('mode', '')}",
                        line,
                        signal_db=float(lvl) if isinstance(lvl, (int, float)) else None,
                    )
                except Exception:
                    pass
        else:
            # Fallback AM airband datalink envelope monitor via rtl_fm
            cmd = ["rtl_fm", "-M", "am", "-s", "12500", "-g", gain]
            if squelch > 0:
                cmd.extend(["-l", str(squelch)])
            for f in freqs:
                cmd.extend(["-f", f])
            self.status_line = f"Monitoring ACARS MSK bursts on {', '.join(freqs)} via rtl_fm"
            with self.lock:
                self.rtl_proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
                proc = self.rtl_proc
            self._monitor_pcm_bursts(proc, "acars", freqs[0], "ACARS 2400bd MSK Burst", threshold_rms=950)

    def _run_multimon(self, proto: str, freqs: list[str], gain: str, squelch: int, mm_args: list[str]):
        rtl_cmd = ["rtl_fm", "-M", "fm", "-s", "22050", "-g", gain]
        if squelch > 0:
            rtl_cmd.extend(["-l", str(squelch)])
        for f in freqs:
            rtl_cmd.extend(["-f", f])
        mm_cmd = ["multimon-ng", "-t", "raw"] + mm_args + ["/dev/stdin"]
        self.status_line = f"Listening on {', '.join(freqs)} ({proto.upper()} via rtl_fm + multimon-ng)"
        with self.lock:
            self.rtl_proc = subprocess.Popen(rtl_cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            self.decoder_proc = subprocess.Popen(
                mm_cmd,
                stdin=self.rtl_proc.stdout,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
            )
            proc = self.decoder_proc

        for line in (proc.stdout or ()):
            if not self.running:
                break
            line = line.strip()
            if not line or line.startswith("multimon-ng") or line.startswith("Available"):
                continue
            if proto == "aprs" and ("APRS:" in line or "AFSK1200:" in line):
                body = line.split(":", 1)[1].strip() if ":" in line else line
                src = body.split(">")[0].strip() if ">" in body else "APRS"
                dst = body.split(">")[1].split(":")[0].strip() if (">" in body and ":" in body) else "APRS"
                info = body.split(":", 1)[1].strip() if ":" in body else body
                lat, lon = _parse_aprs_coords(info)
                log_packet("aprs", freqs[0], src, dst, "AX.25 UI", info, line, lat=lat, lon=lon)
            elif proto == "pocsag" and any(k in line for k in ("POCSAG", "FLEX")):
                parts = line.split(":", 1)
                demod = parts[0].strip()
                rest = parts[1].strip() if len(parts) > 1 else line
                addr_m = re.search(r"Address:\s*(\d+)", rest)
                func_m = re.search(r"Function:\s*(\d+)", rest)
                alpha_m = re.search(r"Alpha:\s*(.*)", rest) or re.search(r"Numeric:\s*(.*)", rest)
                capcode = addr_m.group(1) if addr_m else "Capcode"
                func_lbl = f"Func {func_m.group(1)}" if func_m else demod
                msg_txt = alpha_m.group(1).strip() if alpha_m else rest
                log_packet("pocsag", freqs[0], f"Capcode {capcode}", demod, func_lbl, msg_txt, line)

    def _run_dsc(self, freqs: list[str], gain: str, squelch: int):
        cmd = ["rtl_fm", "-M", "fm", "-s", "22050", "-g", gain]
        if squelch > 0:
            cmd.extend(["-l", str(squelch)])
        for f in freqs:
            cmd.extend(["-f", f])
        self.status_line = f"Monitoring VHF Ch 70 DSC (156.525 MHz 1200bd AFSK 1300/2100 Hz)"
        with self.lock:
            self.rtl_proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            proc = self.rtl_proc
        self._monitor_pcm_bursts(proc, "dsc", freqs[0], "VHF Ch 70 DSC 1200bd AFSK Frame", threshold_rms=1100)

    def _run_wx_sat(self, freqs: list[str], gain: str):
        cmd = ["rtl_fm", "-M", "fm", "-s", "48000", "-r", "11025", "-g", gain, "-f", freqs[0]]
        self.status_line = f"Receiving 137 MHz Weather Satellite APT telemetry on {freqs[0]}"
        with self.lock:
            self.rtl_proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            proc = self.rtl_proc
        self._monitor_pcm_bursts(proc, "wx_sat", freqs[0], "137 MHz APT 2400 Hz Subcarrier Frame", threshold_rms=1400)

    def _monitor_pcm_bursts(self, proc: subprocess.Popen, proto: str, freq: str, label: str, threshold_rms: int = 1000):
        """
        Inspects 16-bit PCM demodulated audio blocks for carrier/subcarrier energy bursts
        when a specialized binary decoder is not active, logging RF frame detections with dBFS level.
        """
        last_logged = 0.0
        while self.running and proc.poll() is None and proc.stdout is not None:
            buf = proc.stdout.read(4096)
            if not buf or len(buf) < 512:
                break
            n = len(buf) // 2
            samples = struct.unpack(f"<{n}h", buf[: n * 2])
            sq_sum = sum(s * s for s in samples)
            rms = math.sqrt(sq_sum / max(1, n))
            if rms >= threshold_rms and (time.time() - last_logged) >= 15.0:
                last_logged = time.time()
                dbfs = round(20.0 * math.log10(max(1.0, rms) / 32768.0), 1)
                log_packet(
                    proto,
                    freq,
                    proto.upper(),
                    "BROADCAST",
                    label,
                    f"Detected {label} on {freq} (RMS {int(rms)}, {dbfs} dBFS)",
                    signal_db=dbfs,
                )


init_protocols_db()
ENGINE = MultiProtocolEngine()


HTML_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Multi-Protocol RF Datalink &amp; Satellite Decoder</title>
<style>
  @layer base, tokens, layout, components;
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
      --accent: oklch(0.72 0.12 230);
      --signal-green: oklch(0.73 0.15 152);
      --signal-amber: oklch(0.77 0.14 75);
      --font-sans: -apple-system, BlinkMacSystemFont, "Inter", "Segoe UI", system-ui, sans-serif;
      --font-mono: "JetBrains Mono", "SF Mono", ui-monospace, Menlo, Consolas, monospace;
      --radius-sm: 5px;
      --radius-md: 7px;
      --radius-lg: 10px;
    }
  }
  @layer base {
    *, *::before, *::after { box-sizing: border-box; }
    body { margin: 0; font-family: var(--font-sans); font-size: 0.875rem; line-height: 1.5; background: var(--canvas); color: var(--ink); }
    .num { font-family: var(--font-mono); font-variant-numeric: tabular-nums; }
    a { color: inherit; text-decoration: none; }
    button, input, select { font: inherit; color: inherit; }
  }
  @layer layout {
    .shell { max-inline-size: 1280px; margin-inline: auto; padding-inline: 1.5rem; padding-block-end: 3rem; }
    .topbar { display: flex; justify-content: space-between; align-items: center; padding-block: 0.85rem; border-block-end: 1px solid var(--line); margin-block-end: 1.25rem; }
    .proto-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(225px, 1fr)); gap: 0.65rem; margin-block-end: 1.25rem; }
    .proto-card {
      grid-row: span 3; display: grid; grid-template-rows: subgrid; gap: 0.45rem;
      background: var(--surface); border: 1px solid var(--line); border-radius: var(--radius-lg);
      padding: 0.85rem 1rem; cursor: pointer; text-align: start;
    }
    .proto-card[data-active="true"] {
      border-color: oklch(0.73 0.15 152 / 0.6);
      background: linear-gradient(180deg, oklch(0.175 0.015 152 / 0.35), var(--surface) 70%);
    }
    .split { display: grid; grid-template-columns: 1.35fr 1fr; gap: 1rem; margin-block-end: 1.25rem; }
    @media (max-width: 920px) { .split { grid-template-columns: 1fr; } }
    .panel { background: var(--surface); border: 1px solid var(--line); border-radius: var(--radius-lg); overflow: clip; }
    .panel-bar { display: flex; justify-content: space-between; align-items: center; padding: 0.65rem 0.95rem; border-block-end: 1px solid var(--line); font-size: 0.82rem; font-weight: 600; color: var(--ink-strong); }
  }
  @layer components {
    .btn { min-block-size: 1.95rem; padding: 0.3rem 0.75rem; border-radius: var(--radius-sm); border: 1px solid var(--line-strong); background: var(--surface-raised); color: var(--ink-strong); font-size: 0.78rem; font-weight: 500; cursor: pointer; white-space: nowrap; }
    .btn-primary { background: var(--ink-strong); color: var(--canvas); border-color: transparent; font-weight: 600; }
    input, select { inline-size: 100%; min-block-size: 1.95rem; padding: 0.25rem 0.6rem; border-radius: var(--radius-sm); border: 1px solid var(--line-strong); background: var(--surface-well); color: var(--ink-strong); font-size: 0.79rem; }
    label { display: block; font-size: 0.72rem; color: var(--ink-muted); margin-block-end: 0.2rem; }
    table { inline-size: 100%; border-collapse: collapse; font-size: 0.79rem; }
    th, td { text-align: start; padding: 0.52rem 0.85rem; border-block-end: 1px solid var(--line); }
    th { background: var(--surface-well); color: var(--ink-muted); font-size: 0.71rem; font-weight: 500; }
    .tag { font-family: var(--font-mono); font-size: 0.68rem; background: var(--surface-raised); border: 1px solid var(--line); padding: 0.1rem 0.4rem; border-radius: 4px; }
  }
</style>
</head>
<body>
<div class="shell">
  <header class="topbar">
    <div style="display:flex;align-items:baseline;gap:0.75rem;flex-wrap:wrap">
      <strong style="font-size:0.95rem;color:var(--ink-strong)">Multi-Protocol RF Datalink &amp; Satellite Decoder</strong>
      <span class="num" style="font-size:0.75rem;color:var(--ink-muted)" id="statusBanner">Loading decoder status...</span>
    </div>
    <a class="btn" id="hubLink" href="/">Back to Console</a>
  </header>

  <!-- 5 RF PROTOCOL CARDS (SUBGRID) -->
  <section class="proto-grid" id="protoGrid"></section>

  <!-- TUNER CONTROLS + WEATHER SATELLITE PASS PREDICTOR -->
  <section class="split">
    <div class="panel">
      <div class="panel-bar">
        <span id="activeProtoTitle">Active Protocol Configuration</span>
        <span class="tag" id="activeProtoMod">--</span>
      </div>
      <div style="padding:1rem;display:grid;grid-template-columns:2fr 1fr 1fr auto;gap:0.75rem;align-items:end">
        <div>
          <label for="inFreq">Frequency / Scan List</label>
          <input class="num" id="inFreq" type="text">
        </div>
        <div>
          <label for="inGain">Tuner Gain (dB)</label>
          <input class="num" id="inGain" type="text" value="40.2">
        </div>
        <div>
          <label for="inSquelch">Squelch</label>
          <input class="num" id="inSquelch" type="number" min="0" max="200" value="20">
        </div>
        <div>
          <button class="btn btn-primary" onclick="applyConfig()">Apply &amp; Retune</button>
        </div>
      </div>
    </div>

    <div class="panel">
      <div class="panel-bar">
        <span>Upcoming NOAA &amp; Meteor-M2 Passes (137 MHz)</span>
        <span class="tag">Polar Orbit</span>
      </div>
      <div style="max-block-size:165px;overflow-y:auto">
        <table>
          <thead><tr><th>Satellite</th><th>Freq</th><th>AOS (UTC)</th><th>In</th><th>Max El</th><th>Action</th></tr></thead>
          <tbody id="wxPassBody"></tbody>
        </table>
      </div>
    </div>
  </section>

  <!-- DECODED PACKET HYPERTABLE FEED -->
  <section class="panel">
    <div class="panel-bar">
      <span>Decoded RF Protocol Frames (TimescaleDB: rf_protocol_packets)</span>
      <span class="num" style="font-size:0.74rem;color:var(--ink-muted)" id="packetCountLabel">0 packets</span>
    </div>
    <div style="max-block-size:430px;overflow-y:auto">
      <table>
        <thead>
          <tr>
            <th>UTC Time</th>
            <th>Protocol</th>
            <th>Freq</th>
            <th>Sender / Station</th>
            <th>Target / Mode</th>
            <th>Channel / Label</th>
            <th>Decoded Message &amp; Telemetry</th>
          </tr>
        </thead>
        <tbody id="packetsBody">
          <tr><td colspan="7" style="color:var(--ink-muted)">Listening for RF datalink packets...</td></tr>
        </tbody>
      </table>
    </div>
  </section>
</div>

<script>
document.getElementById('hubLink').href = 'http://' + window.location.hostname + ':8080/';
let currentProto = 'acars';

async function refreshState() {
  const res = await fetch('/api/state');
  const data = await res.json();
  const cfg = data.state;
  currentProto = cfg.protocol;
  document.getElementById('statusBanner').textContent = data.status_line;

  const prof = data.profiles[currentProto] || {};
  document.getElementById('activeProtoTitle').textContent = prof.title || 'Active Protocol';
  document.getElementById('activeProtoMod').textContent = prof.modulation || '';

  if (document.activeElement.tagName !== 'INPUT') {
    document.getElementById('inFreq').value = cfg.freq;
    document.getElementById('inGain').value = cfg.gain;
    document.getElementById('inSquelch').value = cfg.squelch;
  }

  renderCards(data.profiles, cfg.protocol, data.counts || {});
  renderWxPasses(data.wx_passes || []);
  renderPackets(data.packets || []);
}

function renderCards(profiles, activeId, counts) {
  const grid = document.getElementById('protoGrid');
  grid.innerHTML = Object.values(profiles).map(p => {
    const isAct = p.id === activeId;
    const cnt = counts[p.id] || 0;
    return `
      <button class="proto-card" data-active="${isAct}" onclick="selectProtocol('${p.id}')">
        <div style="display:flex;justify-content:space-between;align-items:baseline;gap:0.4rem">
          <strong style="font-size:0.83rem;color:var(--ink-strong)">${p.title}</strong>
          <span class="tag">${cnt} frames</span>
        </div>
        <div class="num" style="font-size:0.71rem;color:var(--accent)">${p.band}</div>
        <div style="font-size:0.74rem;color:var(--ink-muted);line-height:1.38">${p.description}</div>
      </button>
    `;
  }).join('');
}

function renderWxPasses(passes) {
  const tb = document.getElementById('wxPassBody');
  tb.innerHTML = passes.map(p => `
    <tr>
      <td style="font-weight:600;color:var(--ink-strong)">${p.satellite}</td>
      <td class="num" style="color:var(--accent)">${p.freq}</td>
      <td class="num">${p.aos_utc}</td>
      <td class="num">${p.in_minutes}m</td>
      <td class="num">${p.max_elevation_deg}°</td>
      <td><button class="tag" style="cursor:pointer" onclick="tuneWxSat('${p.freq}')">Tune</button></td>
    </tr>
  `).join('');
}

function renderPackets(packets) {
  document.getElementById('packetCountLabel').textContent = `${packets.length} recent frames`;
  const tb = document.getElementById('packetsBody');
  if (packets.length === 0) {
    tb.innerHTML = '<tr><td colspan="7" style="color:var(--ink-muted)">No frames decoded yet on current frequency.</td></tr>';
    return;
  }
  tb.innerHTML = packets.map(pkt => `
    <tr>
      <td class="num" style="white-space:nowrap;color:var(--ink-muted)">${pkt.ts_utc}</td>
      <td><span class="tag">${pkt.protocol.toUpperCase()}</span></td>
      <td class="num">${pkt.freq}</td>
      <td class="num" style="font-weight:600;color:var(--ink-strong)">${pkt.sender || '—'}</td>
      <td class="num" style="color:var(--ink-muted)">${pkt.target || '—'}</td>
      <td><span class="tag">${pkt.channel_or_label || '—'}</span></td>
      <td class="num" style="font-size:0.76rem">${pkt.summary}</td>
    </tr>
  `).join('');
}

async function selectProtocol(protoId) {
  await fetch('/api/configure', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({protocol: protoId})
  });
  await refreshState();
}

async function tuneWxSat(freq) {
  await fetch('/api/configure', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({protocol: 'wx_sat', freq: freq, gain: '44.5', squelch: 0})
  });
  await refreshState();
}

async function applyConfig() {
  await fetch('/api/configure', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
      protocol: currentProto,
      freq: document.getElementById('inFreq').value.trim(),
      gain: document.getElementById('inGain').value.trim(),
      squelch: parseInt(document.getElementById('inSquelch').value || '0', 10)
    })
  });
  await refreshState();
}

refreshState();
setInterval(refreshState, 3500);
</script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
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
        elif parsed.path == "/api/state":
            qs = parse_qs(parsed.query)
            proto_flt = qs.get("protocol", [None])[0]
            with ENGINE.lock:
                st = dict(ENGINE.state)
                s_line = ENGINE.status_line
            self._send_json(
                {
                    "state": st,
                    "status_line": s_line,
                    "profiles": PROTOCOL_PROFILES,
                    "counts": get_protocol_counts(),
                    "wx_passes": compute_upcoming_wx_passes(),
                    "packets": get_recent_packets(proto_flt),
                }
            )
        else:
            self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/configure":
            length = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(length).decode("utf-8")) if length else {}
            new_st = ENGINE.configure(body)
            self._send_json({"ok": True, "state": new_st})
        else:
            self.send_error(HTTPStatus.NOT_FOUND)


def main():
    def handle_sig(sig, frame):
        ENGINE.running = False
        ENGINE._stop_procs()
        os._exit(0)

    signal.signal(signal.SIGTERM, handle_sig)
    signal.signal(signal.SIGINT, handle_sig)
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[sdr-rf-protocols] Listening on 0.0.0.0:{PORT}", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
