#!/usr/bin/env python3
"""
433.92 MHz / ISM Band Sensor & Telemetry Decoder Web UI (rtl_433).
Decodes wireless weather stations, automotive tire pressure sensors (TPMS),
and utility telemetry on port 8433.
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
<title>433.92 MHz ISM Telemetry Decoder</title>
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
      --font-sans: -apple-system, BlinkMacSystemFont, "Inter", "Segoe UI", system-ui, sans-serif;
      --font-mono: "JetBrains Mono", "SF Mono", ui-monospace, Menlo, Consolas, monospace;
      --radius-sm: 5px;
      --radius-lg: 10px;
    }
  }
  @layer base {
    *, *::before, *::after { box-sizing: border-box; }
    body { margin: 0; font-family: var(--font-sans); font-size: 0.875rem; line-height: 1.5; background: var(--canvas); color: var(--ink); }
    .num { font-family: var(--font-mono); font-variant-numeric: tabular-nums; }
    a { color: inherit; text-decoration: none; }
  }
  @layer layout {
    .shell { max-inline-size: 1080px; margin-inline: auto; padding-inline: 1.5rem; padding-block-end: 3rem; }
    .topbar { display: flex; justify-content: space-between; align-items: center; padding-block: 0.85rem; border-block-end: 1px solid var(--line); margin-block-end: 1.25rem; }
    .panel { background: var(--surface); border: 1px solid var(--line); border-radius: var(--radius-lg); overflow: clip; margin-block-end: 1.25rem; }
    .panel-bar { padding: 0.65rem 0.9rem; border-block-end: 1px solid var(--line); font-size: 0.82rem; font-weight: 600; color: var(--ink-strong); }
  }
  @layer components {
    .btn { min-block-size: 2rem; padding: 0.3rem 0.75rem; border-radius: var(--radius-sm); border: 1px solid var(--line-strong); background: var(--surface-raised); color: var(--ink-strong); font-size: 0.78rem; font-weight: 500; }
    table { inline-size: 100%; border-collapse: collapse; font-size: 0.8rem; }
    th, td { text-align: start; padding: 0.55rem 0.85rem; border-block-end: 1px solid var(--line); }
    th { background: var(--surface-well); color: var(--ink-muted); font-size: 0.72rem; font-weight: 500; }
    pre { margin: 0; font-family: var(--font-mono); font-size: 0.74rem; color: var(--ink); white-space: pre-wrap; word-break: break-all; }
  }
</style>
</head>
<body>
<div class="shell">
  <header class="topbar">
    <div style="display:flex;align-items:baseline;gap:0.75rem">
      <strong style="font-size:0.95rem;color:var(--ink-strong)">433.92 MHz ISM Telemetry Decoder</strong>
      <span style="font-size:0.76rem;color:var(--ink-muted)">rtl_433 v25.02 · JSONL Logger</span>
    </div>
    <a class="btn" id="hubLink" href="/">Back to Console</a>
  </header>
  <section class="panel">
    <div class="panel-bar">Discovered Wireless Devices &amp; Sensors</div>
    <table>
      <thead><tr><th>Model</th><th>ID / Channel</th><th>Packets</th><th>Last Seen (UTC)</th><th>Latest Telemetry</th></tr></thead>
      <tbody id="devBody"><tr><td colspan="5" style="color:var(--ink-muted)">Listening on 433.92 MHz for weather stations, TPMS sensors, and utility telemetry...</td></tr></tbody>
    </table>
  </section>
  <section class="panel">
    <div class="panel-bar">Live Packet Stream</div>
    <table>
      <thead><tr><th>UTC Time</th><th>Model</th><th>Payload JSON</th></tr></thead>
      <tbody id="evBody"></tbody>
    </table>
  </section>
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
        <td style="font-weight:600;color:var(--ink-strong)">${d.model}</td>
        <td class="num">${d.id}</td>
        <td class="num">${d.count}</td>
        <td class="num" style="color:var(--ink-muted)">${d.last_seen}</td>
        <td><pre>${JSON.stringify(d.last_data)}</pre></td>
      </tr>
    `).join('');
  }
  const evBody = document.getElementById('evBody');
  evBody.innerHTML = data.recent.slice(0, 40).map(e => `
    <tr>
      <td class="num" style="white-space:nowrap;color:var(--ink-muted)">${e.time || ''}</td>
      <td style="font-weight:600;color:var(--ink-strong)">${e.model || ''}</td>
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
