#!/usr/bin/env python3
"""
433.92 MHz / ISM Band Sensor & Telemetry Decoder Web UI (rtl_433).
Decodes wireless weather stations, automotive tire pressure sensors (TPMS),
doorbells, security sensors, and utility meters, logging to JSONL and serving
a real-time web interface on port 8433.
"""

import json
import os
import signal
import subprocess
import threading
import time
from collections import deque
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

BASE_DIR = Path(__file__).resolve().parent.parent
PORT = int(os.environ.get("RTL433_PORT", "8433"))
LOG_FILE = Path(os.environ.get("RTL433_LOG_FILE", str(BASE_DIR / "data" / "ism" / "rtl433_events.jsonl")))
FREQ = os.environ.get("RTL433_FREQ", "433.92M")

RECENT_EVENTS = deque(maxlen=200)
DEVICE_SUMMARY = {}
LOCK = threading.Lock()
PROC = None
RUNNING = True


def load_existing():
    if not LOG_FILE.exists():
        return
    try:
        lines = LOG_FILE.read_text().splitlines()[-200:]
        for line in lines:
            if not line.strip():
                continue
            ev = json.loads(line)
            RECENT_EVENTS.appendleft(ev)
            model = ev.get("model", "Unknown")
            dev_id = str(ev.get("id", ev.get("channel", "—")))
            key = f"{model}:{dev_id}"
            DEVICE_SUMMARY[key] = {
                "model": model,
                "id": dev_id,
                "last_seen": ev.get("time", ""),
                "count": DEVICE_SUMMARY.get(key, {}).get("count", 0) + 1,
                "last_data": ev,
            }
    except Exception:
        pass


def reader_loop():
    global PROC
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    load_existing()
    cmd = ["rtl_433", "-d", "0", "-f", FREQ, "-F", "json", "-M", "time:utc", "-M", "level"]
    while RUNNING:
        try:
            PROC = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
            for line in PROC.stdout:
                if not RUNNING:
                    break
                line = line.strip()
                if not line.startswith("{"):
                    continue
                try:
                    ev = json.loads(line)
                except Exception:
                    continue
                with LOCK:
                    RECENT_EVENTS.appendleft(ev)
                    model = ev.get("model", "Unknown")
                    dev_id = str(ev.get("id", ev.get("channel", "—")))
                    key = f"{model}:{dev_id}"
                    DEVICE_SUMMARY[key] = {
                        "model": model,
                        "id": dev_id,
                        "last_seen": ev.get("time", ""),
                        "count": DEVICE_SUMMARY.get(key, {}).get("count", 0) + 1,
                        "last_data": ev,
                    }
                with open(LOG_FILE, "a") as f:
                    f.write(line + "\n")
        except Exception as e:
            print(f"[rtl433-web] error: {e}", flush=True)
        time.sleep(2.0)


HTML_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SDR Station — 433.92 MHz ISM / Weather / TPMS Decoder</title>
<style>
  :root { --bg: #0b101b; --card: #131c2e; --border: #23324d; --text: #e2e8f0; --muted: #94a3b8; --accent: #a855f7; }
  * { box-sizing: border-box; }
  body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: var(--bg); color: var(--text); }
  .wrap { max-width: 1080px; margin: 0 auto; padding: 20px; }
  header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--border); padding-bottom: 16px; margin-bottom: 20px; }
  h1 { margin: 0; font-size: 1.35rem; }
  .back-btn { color: #38bdf8; text-decoration: none; font-size: 0.9rem; border: 1px solid var(--border); padding: 6px 12px; border-radius: 8px; background: var(--card); }
  .card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 16px; margin-bottom: 18px; }
  table { width: 100%; border-collapse: collapse; font-size: 0.88rem; }
  th, td { text-align: left; padding: 8px 10px; border-bottom: 1px solid var(--border); }
  th { color: var(--muted); font-weight: 600; }
  pre { margin: 0; font-family: monospace; font-size: 0.8rem; color: #cbd5e1; white-space: pre-wrap; word-break: break-all; }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>📡 433.92 MHz ISM & Sensor Decoder (rtl_433)</h1>
    <a class="back-btn" id="hubLink" href="/">← Back to Station Launcher</a>
  </header>
  <div class="card">
    <h3 style="margin-top:0">Discovered Wireless Devices & Sensors</h3>
    <table>
      <thead><tr><th>Model</th><th>ID / Ch</th><th>Packets</th><th>Last Seen (UTC)</th><th>Latest Telemetry</th></tr></thead>
      <tbody id="devBody"><tr><td colspan="5">Listening on 433.92 MHz for weather stations, TPMS tire sensors, remotes...</td></tr></tbody>
    </table>
  </div>
  <div class="card">
    <h3 style="margin-top:0">Live Packet Feed</h3>
    <table>
      <thead><tr><th>UTC Time</th><th>Model</th><th>Payload JSON</th></tr></thead>
      <tbody id="evBody"></tbody>
    </table>
  </div>
</div>
<script>
document.getElementById('hubLink').href = 'http://' + window.location.hostname + ':8080/';
async function refresh() {
  const res = await fetch('/api/events');
  const data = await res.json();
  const devBody = document.getElementById('devBody');
  const devs = Object.values(data.devices);
  if (devs.length > 0) {
    devBody.innerHTML = devs.map(d => `
      <tr>
        <td><b>${d.model}</b></td>
        <td>${d.id}</td>
        <td>${d.count}</td>
        <td>${d.last_seen}</td>
        <td><pre>${JSON.stringify(d.last_data)}</pre></td>
      </tr>
    `).join('');
  }
  const evBody = document.getElementById('evBody');
  evBody.innerHTML = data.recent.slice(0, 40).map(e => `
    <tr>
      <td style="white-space:nowrap">${e.time || ''}</td>
      <td><b>${e.model || ''}</b></td>
      <td><pre>${JSON.stringify(e)}</pre></td>
    </tr>
  `).join('');
}
refresh();
setInterval(refresh, 3000);
</script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/":
            raw = HTML_PAGE.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
        elif parsed.path == "/api/events":
            with LOCK:
                payload = {"devices": DEVICE_SUMMARY, "recent": list(RECENT_EVENTS)[:60]}
            raw = json.dumps(payload).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(raw)
        else:
            self.send_error(HTTPStatus.NOT_FOUND)


def main():
    def handle_sig(sig, frame):
        global RUNNING
        RUNNING = False
        if PROC and PROC.poll() is None:
            try:
                PROC.terminate()
            except Exception:
                pass
        os._exit(0)

    signal.signal(signal.SIGTERM, handle_sig)
    signal.signal(signal.SIGINT, handle_sig)
    t = threading.Thread(target=reader_loop, daemon=True)
    t.start()
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[rtl433-web] Listening on 0.0.0.0:{PORT}", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
