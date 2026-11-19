#!/usr/bin/env python3
"""
SDR Voice Receiver & Scanner (AM / NFM / WFM / USB / LSB).
Streams live demodulated audio over HTTP MP3 (/stream.mp3) using rtl_fm + ffmpeg.
Check your local regulations before receiving or recording voice traffic.
"""

import json
import os
import signal
import subprocess
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

BASE_DIR = Path(__file__).resolve().parent.parent
PORT = int(os.environ.get("VOICE_PORT", "8073"))
STATE_FILE = Path(os.environ.get("VOICE_STATE_FILE", str(BASE_DIR / "data" / "voice_state.json")))

PRESETS = {
    "FM Broadcast Radio (WFM 88–108 MHz)": [
        {"name": "FM Broadcast 93.8", "freq": "93.8M", "mod": "wbfm", "squelch": 0, "gain": "29.7", "desc": "Wideband FM broadcast test frequency (continuous signal check)"},
        {"name": "FM Broadcast 95.0", "freq": "95.0M", "mod": "wbfm", "squelch": 0, "gain": "29.7", "desc": "Wideband FM broadcast station"},
        {"name": "FM Broadcast 92.4", "freq": "92.4M", "mod": "wbfm", "squelch": 0, "gain": "29.7", "desc": "Wideband FM classical broadcast"},
        {"name": "FM Broadcast 98.7", "freq": "98.7M", "mod": "wbfm", "squelch": 0, "gain": "29.7", "desc": "Wideband FM music broadcast"},
    ],
    "Amateur Radio & Weather (2m VHF 144–148 MHz)": [
        {"name": "2m FM Calling", "freq": "145.500M", "mod": "fm", "squelch": 26, "gain": "40.2", "desc": "International 2-meter amateur radio FM simplex calling"},
        {"name": "2m APRS Digital", "freq": "144.800M", "mod": "fm", "squelch": 20, "gain": "40.2", "desc": "VHF packet / APRS telemetry channel"},
        {"name": "NOAA / Weather VHF", "freq": "162.550M", "mod": "fm", "squelch": 0, "gain": "40.2", "desc": "VHF weather broadcast band (region-dependent)"},
    ],
    "International Calling & Reference (Check Local Regulations)": [
        {"name": "VHF Ch 16 Calling", "freq": "156.800M", "mod": "fm", "squelch": 28, "gain": "40.2", "desc": "International maritime VHF calling & safety frequency"},
        {"name": "Airband Reference", "freq": "121.500M", "mod": "am", "squelch": 32, "gain": "42.1", "desc": "International aeronautical VHF reference frequency"},
    ],
}

DEFAULT_CONFIG = {
    "name": "FM Broadcast 93.8",
    "freq": "93.8M",
    "mod": "wbfm",
    "squelch": 0,
    "gain": "29.7",
}


class CleanAudioBroadcaster:
    """Manages a single rtl_fm -> ffmpeg pipeline and broadcasts MP3 chunks to connected HTTP clients."""

    def __init__(self):
        self.lock = threading.Lock()
        self.config = self.load_state()
        self.rtl_proc = None
        self.ffmpeg_proc = None
        self.clients = {}
        self.running = True
        self.worker_thread = threading.Thread(target=self._loop, daemon=True)
        self.worker_thread.start()

    def load_state(self):
        if STATE_FILE.exists():
            try:
                return {**DEFAULT_CONFIG, **json.loads(STATE_FILE.read_text())}
            except Exception:
                pass
        return dict(DEFAULT_CONFIG)

    def save_state(self):
        try:
            STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
            STATE_FILE.write_text(json.dumps(self.config, indent=2))
        except Exception:
            pass

    def update_config(self, new_cfg: dict):
        with self.lock:
            self.config.update(new_cfg)
            self.save_state()
            self._stop_procs()

    def _stop_procs(self):
        for p in (self.ffmpeg_proc, self.rtl_proc):
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
        self.ffmpeg_proc = None

    def _build_rtl_cmd(self, cfg: dict):
        mod = cfg.get("mod", "am").lower()
        freqs = [f.strip() for f in str(cfg.get("freq", "93.8M")).split(",") if f.strip()]
        squelch = int(cfg.get("squelch", 0))
        gain = str(cfg.get("gain", "29.7"))

        cmd = ["rtl_fm"]
        for f in freqs:
            cmd.extend(["-f", f])

        if mod == "wbfm":
            cmd.extend(["-M", "wbfm", "-s", "200000", "-r", "32000", "-E", "deemp"])
            sample_rate = 32000
        elif mod == "am":
            cmd.extend(["-M", "am", "-s", "16000", "-r", "16000"])
            sample_rate = 16000
        elif mod in ("usb", "lsb"):
            cmd.extend(["-M", mod, "-s", "12000", "-r", "12000"])
            sample_rate = 12000
        else:
            cmd.extend(["-M", "fm", "-s", "16000", "-r", "16000"])
            sample_rate = 16000

        if gain and gain != "auto":
            cmd.extend(["-g", gain])
        if squelch > 0:
            cmd.extend(["-l", str(squelch)])

        return cmd, sample_rate

    def _loop(self):
        while self.running:
            with self.lock:
                has_clients = len(self.clients) > 0
                cfg = dict(self.config)

            if not has_clients:
                with self.lock:
                    self._stop_procs()
                time.sleep(0.25)
                continue

            try:
                rtl_cmd, sample_rate = self._build_rtl_cmd(cfg)
                ffmpeg_cmd = [
                    "ffmpeg",
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-f",
                    "s16le",
                    "-ar",
                    str(sample_rate),
                    "-ac",
                    "1",
                    "-i",
                    "pipe:0",
                    "-af",
                    "highpass=f=250,lowpass=f=4200,volume=2.2" if cfg.get("mod") != "wbfm" else "volume=1.5",
                    "-acodec",
                    "libmp3lame",
                    "-ab",
                    "96k",
                    "-ar",
                    "44100",
                    "-reservoir",
                    "0",
                    "-fflags",
                    "nobuffer",
                    "-f",
                    "mp3",
                    "pipe:1",
                ]

                with self.lock:
                    self.rtl_proc = subprocess.Popen(rtl_cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
                    self.ffmpeg_proc = subprocess.Popen(
                        ffmpeg_cmd,
                        stdin=self.rtl_proc.stdout,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.DEVNULL,
                    )
                    ff_out = self.ffmpeg_proc.stdout

                while self.running:
                    chunk = ff_out.read(1024)
                    if not chunk:
                        break
                    with self.lock:
                        if not self.clients:
                            self._stop_procs()
                            break
                        for cid, q in list(self.clients.items()):
                            q.append(chunk)
                            if len(q) > 80:
                                del q[:30]
            except Exception as e:
                print(f"[voice] error: {e}", flush=True)
                time.sleep(0.8)
            finally:
                with self.lock:
                    self._stop_procs()
            time.sleep(0.2)


BROADCASTER = CleanAudioBroadcaster()


HTML_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SDR Audio Demodulator &amp; Scanner</title>
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
      --font-sans: -apple-system, BlinkMacSystemFont, "Inter", "Segoe UI", system-ui, sans-serif;
      --font-mono: "JetBrains Mono", "SF Mono", ui-monospace, Menlo, Consolas, monospace;
      --radius-sm: 5px;
      --radius-lg: 10px;
    }
  }
  @layer base {
    *, *::before, *::after { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: var(--font-sans);
      font-size: 0.875rem;
      line-height: 1.5;
      background: var(--canvas);
      color: var(--ink);
      -webkit-font-smoothing: antialiased;
    }
    .num { font-family: var(--font-mono); font-variant-numeric: tabular-nums; }
    a { color: inherit; text-decoration: none; }
    input, select, button { font: inherit; color: inherit; }
  }
  @layer layout {
    .shell { max-inline-size: 1080px; margin-inline: auto; padding-inline: 1.5rem; padding-block-end: 3rem; }
    .topbar {
      display: flex; justify-content: space-between; align-items: center;
      padding-block: 0.85rem; border-block-end: 1px solid var(--line); margin-block-end: 1.25rem;
    }
    .tuner-panel {
      background: var(--surface); border: 1px solid var(--line); border-radius: var(--radius-lg);
      padding: 1.25rem; margin-block-end: 1.5rem;
      display: grid; grid-template-columns: 1fr 1.15fr; gap: 1.5rem;
    }
    @media (max-width: 760px) { .tuner-panel { grid-template-columns: 1fr; } }
    .preset-grid {
      display: grid; grid-template-columns: repeat(auto-fill, minmax(240px, 1fr)); gap: 0.65rem;
    }
  }
  @layer components {
    .btn {
      min-block-size: 2rem; padding-block: 0.35rem; padding-inline: 0.8rem;
      border-radius: var(--radius-sm); border: 1px solid var(--line-strong);
      background: var(--surface-raised); color: var(--ink-strong); font-size: 0.78rem;
      font-weight: 500; cursor: pointer; display: inline-flex; align-items: center; justify-content: center;
    }
    .btn-primary { background: var(--ink-strong); color: var(--canvas); border-color: transparent; font-weight: 600; }
    .freq-readout {
      font-family: var(--font-mono); font-variant-numeric: tabular-nums;
      font-size: 1.85rem; font-weight: 600; color: var(--ink-strong); letter-spacing: -0.02em;
      margin-block: 0.15rem 0.4rem;
    }
    .controls-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 0.65rem; }
    label { display: block; font-size: 0.72rem; color: var(--ink-muted); margin-block-end: 0.25rem; }
    input, select {
      inline-size: 100%; min-block-size: 2rem; padding-block: 0.3rem; padding-inline: 0.6rem;
      border-radius: var(--radius-sm); border: 1px solid var(--line-strong);
      background: var(--surface-well); color: var(--ink-strong); font-size: 0.8rem;
    }
    .cat-block { margin-block-end: 1.35rem; }
    .cat-title { font-size: 0.76rem; font-weight: 600; color: var(--ink-muted); margin-block-end: 0.5rem; }
    .preset-btn {
      background: var(--surface); border: 1px solid var(--line); border-radius: var(--radius-lg);
      padding: 0.75rem 0.9rem; text-align: start; cursor: pointer;
      display: flex; flex-direction: column; gap: 0.25rem;
    }
    .preset-btn:hover { border-color: var(--line-strong); background: var(--surface-raised); }
    .preset-top { display: flex; justify-content: space-between; align-items: baseline; font-weight: 600; font-size: 0.82rem; color: var(--ink-strong); }
    .preset-freq { font-family: var(--font-mono); font-variant-numeric: tabular-nums; color: var(--accent); font-size: 0.75rem; }
    .preset-desc { font-size: 0.74rem; color: var(--ink-muted); line-height: 1.38; }
    audio { inline-size: 100%; margin-block-start: 0.65rem; }
    .notice { font-size: 0.73rem; color: var(--ink-muted); margin-block-start: 0.6rem; }
  }
</style>
</head>
<body>
<div class="shell">
  <header class="topbar">
    <div style="display:flex;align-items:baseline;gap:0.75rem">
      <strong style="font-size:0.95rem;color:var(--ink-strong)">VHF / FM Audio Receiver &amp; Scanner</strong>
      <span style="font-size:0.76rem;color:var(--ink-muted)">rtl_fm + FFmpeg MP3 Stream</span>
    </div>
    <a class="btn" id="hubLink" href="/">Back to Console</a>
  </header>

  <section class="tuner-panel">
    <div>
      <div style="font-size:0.76rem;color:var(--signal-green);font-weight:500" id="curName">Loading...</div>
      <div class="freq-readout" id="curFreq">--</div>
      <div class="notice" id="curStatus">Select Start Stream to demodulate live audio in the browser.</div>
      <div style="margin-block-start:0.75rem">
        <button class="btn btn-primary" style="inline-size:100%" id="playBtn" onclick="toggleAudio()">Start Audio Stream</button>
      </div>
      <audio id="audioPlayer" controls preload="none"></audio>
      <div class="notice">Check your local regulations before receiving or recording voice traffic.</div>
    </div>

    <div class="controls-grid">
      <div style="grid-column: span 2;">
        <label for="inFreq">Frequency (Hz / M, or comma-separated scan list)</label>
        <input class="num" id="inFreq" type="text" value="93.8M">
      </div>
      <div>
        <label for="inMod">Modulation</label>
        <select id="inMod">
          <option value="wbfm">WFM (Broadcast 88–108 MHz)</option>
          <option value="fm">NFM (Narrowband FM)</option>
          <option value="am">AM (Amplitude Modulation)</option>
          <option value="usb">USB (Upper Sideband)</option>
          <option value="lsb">LSB (Lower Sideband)</option>
        </select>
      </div>
      <div>
        <label for="inGain">Tuner Gain (dB)</label>
        <select class="num" id="inGain">
          <option value="auto">Auto Gain</option>
          <option value="49.6">49.6 dB</option>
          <option value="42.1">42.1 dB</option>
          <option value="40.2">40.2 dB</option>
          <option value="32.8">32.8 dB</option>
          <option value="29.7">29.7 dB</option>
          <option value="19.7">19.7 dB</option>
        </select>
      </div>
      <div>
        <label for="inSquelch">Squelch Level (0 = Open)</label>
        <input class="num" id="inSquelch" type="number" min="0" max="200" value="0">
      </div>
      <div style="display:flex;align-items:flex-end;">
        <button class="btn" style="inline-size:100%" onclick="applyCustom()">Tune Frequency</button>
      </div>
    </div>
  </section>

  <div id="presetsContainer"></div>
</div>

<script>
document.getElementById('hubLink').href = 'http://' + window.location.hostname + ':8080/';
let playing = false;

async function loadState() {
  const res = await fetch('/api/state');
  const data = await res.json();
  updateUI(data.config);
  renderPresets(data.presets);
}

function updateUI(cfg) {
  document.getElementById('curName').textContent = cfg.name || 'Manual Frequency';
  document.getElementById('curFreq').textContent = cfg.freq + ' · ' + cfg.mod.toUpperCase();
  document.getElementById('inFreq').value = cfg.freq;
  document.getElementById('inMod').value = cfg.mod;
  document.getElementById('inGain').value = cfg.gain;
  document.getElementById('inSquelch').value = cfg.squelch;
}

function renderPresets(presets) {
  const container = document.getElementById('presetsContainer');
  container.innerHTML = '';
  for (const [cat, items] of Object.entries(presets)) {
    const block = document.createElement('div');
    block.className = 'cat-block';
    block.innerHTML = `<div class="cat-title">${cat}</div><div class="preset-grid"></div>`;
    const grid = block.querySelector('.preset-grid');
    for (const item of items) {
      const btn = document.createElement('button');
      btn.className = 'preset-btn';
      btn.innerHTML = `
        <div class="preset-top">
          <span>${item.name}</span>
          <span class="preset-freq">${item.freq.split(',')[0]}</span>
        </div>
        <div class="preset-desc">${item.desc}</div>
      `;
      btn.onclick = () => tunePreset(item);
      grid.appendChild(btn);
    }
    container.appendChild(block);
  }
}

async function tunePreset(item) {
  const res = await fetch('/api/tune', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(item)
  });
  const data = await res.json();
  updateUI(data.config);
  if (!playing) {
    startAudio();
  } else {
    restartAudioStream();
  }
}

async function applyCustom() {
  const item = {
    name: 'Manual Tune',
    freq: document.getElementById('inFreq').value.trim(),
    mod: document.getElementById('inMod').value,
    gain: document.getElementById('inGain').value,
    squelch: parseInt(document.getElementById('inSquelch').value || '0', 10)
  };
  await tunePreset(item);
}

function startAudio() {
  const player = document.getElementById('audioPlayer');
  player.src = '/stream.mp3?t=' + Date.now();
  player.play().catch(() => {});
  playing = true;
  document.getElementById('playBtn').textContent = 'Stop Audio Stream';
  document.getElementById('curStatus').textContent = 'Streaming live demodulated audio from RTL-SDR v5.';
}

function restartAudioStream() {
  const player = document.getElementById('audioPlayer');
  setTimeout(() => {
    player.src = '/stream.mp3?t=' + Date.now();
    player.play().catch(() => {});
  }, 350);
}

function toggleAudio() {
  const player = document.getElementById('audioPlayer');
  if (playing) {
    player.pause();
    player.src = '';
    playing = false;
    document.getElementById('playBtn').textContent = 'Start Audio Stream';
    document.getElementById('curStatus').textContent = 'Audio stream stopped.';
  } else {
    startAudio();
  }
}

loadState();
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
        if parsed.path == "/":
            raw = HTML_PAGE.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
        elif parsed.path == "/api/state":
            with BROADCASTER.lock:
                cfg = dict(BROADCASTER.config)
                listeners = len(BROADCASTER.clients)
            self._send_json({"config": cfg, "listeners": listeners, "presets": PRESETS})
        elif parsed.path == "/stream.mp3":
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "audio/mpeg")
            self.send_header("Cache-Control", "no-cache, no-store")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            cid = time.monotonic_ns()
            q = []
            with BROADCASTER.lock:
                BROADCASTER.clients[cid] = q
            try:
                while True:
                    chunk = None
                    with BROADCASTER.lock:
                        if q:
                            chunk = q.pop(0)
                    if chunk:
                        self.wfile.write(chunk)
                        self.wfile.flush()
                    else:
                        time.sleep(0.04)
            except Exception:
                pass
            finally:
                with BROADCASTER.lock:
                    BROADCASTER.clients.pop(cid, None)
        else:
            self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/tune":
            length = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(length).decode("utf-8")) if length else {}
            allowed = {k: body[k] for k in ("name", "freq", "mod", "squelch", "gain") if k in body}
            BROADCASTER.update_config(allowed)
            with BROADCASTER.lock:
                cfg = dict(BROADCASTER.config)
            self._send_json({"ok": True, "config": cfg})
        else:
            self.send_error(HTTPStatus.NOT_FOUND)


def main():
    def handle_sig(sig, frame):
        BROADCASTER.running = False
        BROADCASTER._stop_procs()
        os._exit(0)

    signal.signal(signal.SIGTERM, handle_sig)
    signal.signal(signal.SIGINT, handle_sig)
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[sdr-voice-web] Listening on 0.0.0.0:{PORT}", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
