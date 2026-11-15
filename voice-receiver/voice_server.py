#!/usr/bin/env python3
"""
SDR Voice Receiver & Scanner (Airband AM / Marine VHF NFM / Broadcast WFM).
Streams live demodulated audio over HTTP MP3 (/stream.mp3) using rtl_fm + ffmpeg
so any desktop or mobile browser (Safari/Chrome over Tailscale or LAN) can listen
with zero plugins, switch frequencies/presets on the fly, scan banks, or run an
RF spectrum sweep via rtl_power.
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
from urllib.parse import parse_qs, urlparse

BASE_DIR = Path(__file__).resolve().parent.parent
PORT = int(os.environ.get("VOICE_PORT", "8073"))
STATE_FILE = Path(os.environ.get("VOICE_STATE_FILE", str(BASE_DIR / "data" / "voice_state.json")))

PRESETS = {
    "Aviation / Airband (AM 118-137 MHz)": [
        {"name": "Changi Tower", "freq": "118.600M", "mod": "am", "squelch": 28, "gain": "42.1", "desc": "Singapore Changi Airport Primary Tower"},
        {"name": "Changi Approach", "freq": "120.300M", "mod": "am", "squelch": 28, "gain": "42.1", "desc": "Singapore Changi Radar Approach Control"},
        {"name": "Changi Ground", "freq": "121.900M", "mod": "am", "squelch": 28, "gain": "42.1", "desc": "Surface movement & taxi control"},
        {"name": "Changi Delivery", "freq": "121.650M", "mod": "am", "squelch": 28, "gain": "42.1", "desc": "IFR clearance delivery"},
        {"name": "Changi ATIS", "freq": "128.600M", "mod": "am", "squelch": 0, "gain": "42.1", "desc": "Continuous automated weather & runway info (good test signal)"},
        {"name": "Singapore Radar", "freq": "123.700M", "mod": "am", "squelch": 28, "gain": "42.1", "desc": "Area control / terminal radar"},
        {"name": "Seletar Tower", "freq": "118.450M", "mod": "am", "squelch": 28, "gain": "42.1", "desc": "Seletar Airport Tower"},
        {"name": "Aviation Guard", "freq": "121.500M", "mod": "am", "squelch": 32, "gain": "42.1", "desc": "International aeronautical emergency frequency"},
        {"name": "Scan: Changi ATC Bank", "freq": "118.600M,120.300M,121.900M,123.700M,118.450M", "mod": "am", "squelch": 32, "gain": "42.1", "desc": "Multi-channel Airband scanner (locks onto active ATC voice)"},
    ],
    "Marine VHF (NFM 156-162 MHz)": [
        {"name": "Ch 16 Distress & Calling", "freq": "156.800M", "mod": "fm", "squelch": 25, "gain": "40.2", "desc": "International maritime distress, safety & calling"},
        {"name": "Ch 73 VTIS Central", "freq": "156.675M", "mod": "fm", "squelch": 25, "gain": "40.2", "desc": "Singapore Strait Vessel Traffic Information System (Central)"},
        {"name": "Ch 10 VTIS East", "freq": "156.500M", "mod": "fm", "squelch": 25, "gain": "40.2", "desc": "Singapore Strait VTIS East sector"},
        {"name": "Ch 74 VTIS West", "freq": "156.725M", "mod": "fm", "squelch": 25, "gain": "40.2", "desc": "Singapore Strait VTIS West sector"},
        {"name": "Ch 12 Port Ops East", "freq": "156.600M", "mod": "fm", "squelch": 25, "gain": "40.2", "desc": "MPA Port Operations East"},
        {"name": "Ch 14 Port Ops West", "freq": "156.700M", "mod": "fm", "squelch": 25, "gain": "40.2", "desc": "MPA Port Operations West"},
        {"name": "Ch 68 Port Ops", "freq": "156.425M", "mod": "fm", "squelch": 25, "gain": "40.2", "desc": "Port operations & anchorage control"},
        {"name": "Scan: Singapore Marine Bank", "freq": "156.800M,156.675M,156.500M,156.725M,156.600M,156.700M", "mod": "fm", "squelch": 30, "gain": "40.2", "desc": "Scans VTIS & Port Ops VHF channels"},
    ],
    "FM Broadcast Radio (WFM 88-108 MHz)": [
        {"name": "CNA938 (News & Talk)", "freq": "93.8M", "mod": "wbfm", "squelch": 0, "gain": "29.7", "desc": "Channel NewsAsia 93.8 FM (Strong continuous signal for audio check)"},
        {"name": "Class 95 FM", "freq": "95.0M", "mod": "wbfm", "squelch": 0, "gain": "29.7", "desc": "English hits broadcast"},
        {"name": "Symphony 924", "freq": "92.4M", "mod": "wbfm", "squelch": 0, "gain": "29.7", "desc": "Classical music"},
        {"name": "987FM", "freq": "98.7M", "mod": "wbfm", "squelch": 0, "gain": "29.7", "desc": "Top 40 broadcast"},
        {"name": "Gold 905", "freq": "90.5M", "mod": "wbfm", "squelch": 0, "gain": "29.7", "desc": "Classic hits"},
    ],
}

DEFAULT_CONFIG = {
    "name": "Changi Tower",
    "freq": "118.600M",
    "mod": "am",
    "squelch": 28,
    "gain": "42.1",
}


class AudioBroadcaster:
    """Manages a single rtl_fm -> ffmpeg pipeline and broadcasts MP3 chunks to all connected HTTP clients."""

    def __init__(self):
        self.lock = threading.Lock()
        self.config = self.load_state()
        self.rtl_proc = None
        self.ffmpeg_proc = None
        self.clients = set()
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
        freqs = [f.strip() for f in str(cfg.get("freq", "118.600M")).split(",") if f.strip()]
        squelch = int(cfg.get("squelch", 0))
        gain = str(cfg.get("gain", "40.2"))

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
                time.sleep(0.3)
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
                    "highpass=f=250,lowpass=f=4200,volume=2.0" if cfg.get("mod") != "wbfm" else "volume=1.4",
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
                    chunk = ff_out.read(2048)
                    if not chunk:
                        break
                    with self.lock:
                        if not self.clients:
                            self._stop_procs()
                            break
                        dead = []
                        for q in self.clients:
                            try:
                                q.append(chunk)
                                if len(q) > 60:
                                    del q[:20]
                            except Exception:
                                dead.append(q)
                        for q in dead:
                            self.clients.discard(q)
            except Exception as e:
                print(f"[voice] pipeline error: {e}", flush=True)
                time.sleep(1.0)
            finally:
                with self.lock:
                    self._stop_procs()
            time.sleep(0.2)


BROADCASTER = AudioBroadcaster()


HTML_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SDR Station — Airband, Marine VHF & FM Voice Receiver</title>
<style>
  :root {
    --bg: #0b101b; --card: #131c2e; --border: #23324d;
    --text: #e2e8f0; --muted: #94a3b8; --accent: #38bdf8;
    --green: #22c55e; --amber: #f59e0b;
  }
  * { box-sizing: border-box; }
  body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: var(--bg); color: var(--text); }
  .wrap { max-width: 1080px; margin: 0 auto; padding: 20px; }
  header { display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 12px; border-bottom: 1px solid var(--border); padding-bottom: 16px; margin-bottom: 20px; }
  h1 { margin: 0; font-size: 1.35rem; display: flex; align-items: center; gap: 10px; }
  .back-btn { color: var(--accent); text-decoration: none; font-size: 0.9rem; border: 1px solid var(--border); padding: 6px 12px; border-radius: 8px; background: var(--card); }
  .tuner-card { background: var(--card); border: 1px solid var(--border); border-radius: 14px; padding: 20px; margin-bottom: 20px; display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
  @media (max-width: 760px) { .tuner-card { grid-template-columns: 1fr; } }
  .freq-display { font-family: "SF Mono", Menlo, monospace; font-size: 2.1rem; font-weight: 700; color: var(--accent); margin: 4px 0; word-break: break-all; }
  .station-name { font-size: 1.1rem; font-weight: 600; color: var(--green); }
  .controls-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px; }
  label { display: block; font-size: 0.78rem; color: var(--muted); margin-bottom: 4px; }
  input, select { width: 100%; padding: 8px 10px; border-radius: 8px; border: 1px solid var(--border); background: #090d16; color: var(--text); font-size: 0.92rem; }
  button.primary { background: var(--accent); color: #06121e; border: none; padding: 10px 16px; border-radius: 8px; font-weight: 700; cursor: pointer; font-size: 0.92rem; }
  button.play-btn { background: var(--green); color: #051b0e; border: none; padding: 12px 18px; border-radius: 10px; font-weight: 700; cursor: pointer; font-size: 1rem; width: 100%; margin-top: 10px; }
  .cat-block { margin-bottom: 22px; }
  .cat-title { font-size: 0.95rem; text-transform: uppercase; letter-spacing: 0.06em; color: var(--muted); margin-bottom: 10px; }
  .preset-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(235px, 1fr)); gap: 10px; }
  .preset-btn { background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 12px; text-align: left; color: var(--text); cursor: pointer; transition: border-color 0.15s, transform 0.1s; }
  .preset-btn:hover { border-color: var(--accent); transform: translateY(-1px); }
  .preset-top { display: flex; justify-content: space-between; align-items: center; font-weight: 600; font-size: 0.93rem; margin-bottom: 4px; }
  .preset-freq { font-family: monospace; color: var(--accent); font-size: 0.85rem; }
  .preset-desc { font-size: 0.78rem; color: var(--muted); line-height: 1.35; }
  audio { width: 100%; margin-top: 12px; border-radius: 8px; }
  .hint { font-size: 0.8rem; color: var(--muted); margin-top: 8px; }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>📻 SDR Live Voice & Scanner Receiver</h1>
    <a class="back-btn" id="hubLink" href="/">← Back to Station Launcher</a>
  </header>

  <div class="tuner-card">
    <div>
      <div class="station-name" id="curName">Loading...</div>
      <div class="freq-display" id="curFreq">--</div>
      <div class="hint" id="curStatus">Click "Start Live Audio Stream" below to listen in your browser.</div>
      <button class="play-btn" id="playBtn" onclick="toggleAudio()">▶ Start Live Audio Stream</button>
      <audio id="audioPlayer" controls preload="none"></audio>
      <div class="hint">Tip: For quick audio check, select <b>CNA938 (93.8M WFM)</b> or <b>Changi ATIS (128.600M AM)</b>. For quiet ATC/VHF channels, raise squelch (25–35) to mute static between transmissions, or set squelch to 0 to hear raw carrier noise.</div>
    </div>

    <div>
      <div class="controls-grid">
        <div style="grid-column: span 2;">
          <label>Frequency (e.g. 118.600M, 156.800M, or comma-separated scan list)</label>
          <input id="inFreq" type="text" value="118.600M">
        </div>
        <div>
          <label>Modulation</label>
          <select id="inMod">
            <option value="am">AM (Aircraft 118-137 MHz)</option>
            <option value="fm">NFM (Marine / Ham VHF)</option>
            <option value="wbfm">WFM (Broadcast 88-108 MHz)</option>
            <option value="usb">USB (Upper Sideband)</option>
            <option value="lsb">LSB (Lower Sideband)</option>
          </select>
        </div>
        <div>
          <label>Tuner Gain (dB)</label>
          <select id="inGain">
            <option value="auto">Auto Gain</option>
            <option value="49.6">49.6 dB (Max)</option>
            <option value="42.1">42.1 dB (Airband High)</option>
            <option value="40.2">40.2 dB (Marine VHF)</option>
            <option value="32.8">32.8 dB (Medium)</option>
            <option value="29.7">29.7 dB (FM Broadcast)</option>
            <option value="19.7">19.7 dB (Low)</option>
          </select>
        </div>
        <div>
          <label>Squelch Level (0 = Open, 25-40 = Mute noise)</label>
          <input id="inSquelch" type="number" min="0" max="200" value="28">
        </div>
        <div style="display:flex;align-items:flex-end;">
          <button class="primary" style="width:100%" onclick="applyCustom()">Tune Receiver</button>
        </div>
      </div>
    </div>
  </div>

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
  document.getElementById('curName').textContent = cfg.name || 'Custom Frequency';
  document.getElementById('curFreq').textContent = cfg.freq + ' (' + cfg.mod.toUpperCase() + ')';
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
  document.getElementById('playBtn').textContent = '⏹ Stop Audio Stream';
  document.getElementById('playBtn').style.background = '#ef4444';
  document.getElementById('playBtn').style.color = '#fff';
  document.getElementById('curStatus').textContent = 'Streaming live demodulated audio from RTL-SDR v5...';
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
    document.getElementById('playBtn').textContent = '▶ Start Live Audio Stream';
    document.getElementById('playBtn').style.background = '#22c55e';
    document.getElementById('playBtn').style.color = '#051b0e';
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


class VoiceHandler(BaseHTTPRequestHandler):
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
            self.send_header("Connection", "close")
            self.end_headers()
            q = []
            with BROADCASTER.lock:
                BROADCASTER.clients.add(id(q))
            # Store list reference by object in a dict instead for clean mutability
        else:
            self.send_error(HTTPStatus.NOT_FOUND)


class CleanAudioBroadcaster(AudioBroadcaster):
    """Uses a dict of client_id -> list queue so sets don't fail on unhashable lists."""

    def __init__(self):
        self.lock = threading.Lock()
        self.config = self.load_state()
        self.rtl_proc = None
        self.ffmpeg_proc = None
        self.clients = {}
        self.running = True
        self.worker_thread = threading.Thread(target=self._loop, daemon=True)
        self.worker_thread.start()

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


class Handler(VoiceHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/stream.mp3":
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
            return
        super().do_GET()

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
