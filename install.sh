#!/usr/bin/env bash
# Automated installer for deploying the Multi-Band SDR Station on a Raspberry Pi (Debian / RPi OS)
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_USER="${SUDO_USER:-$(whoami)}"
STATION_LAT="${STATION_LAT:-<your-lat>}"
STATION_LON="${STATION_LON:-<your-lon>}"
STATION_NAME="${STATION_NAME:-SDR Station}"

if [[ "${STATION_LAT}" == "<your-lat>" || "${STATION_LON}" == "<your-lon>" ]]; then
  echo "Note: STATION_LAT / STATION_LON not set. Range calculations will be disabled until configured in /etc/default/sdr-station."
fi

echo "=== Installing SDR Station from ${REPO_DIR} (user: ${RUN_USER}) ==="

sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
  rtl-sdr librtlsdr0 librtlsdr-dev \
  rtl-433 multimon-ng direwolf \
  soapysdr-tools soapysdr-module-rtlsdr soapyremote-server \
  build-essential cmake git pkg-config libncurses-dev \
  libusb-1.0-0-dev libcurl4-openssl-dev zlib1g-dev libssl-dev libsqlite3-dev \
  postgresql-17 postgresql-client-17 postgresql-17-timescaledb libpq-dev python3-psycopg2 \
  ffmpeg sox libsox-fmt-all jq curl lighttpd

# Install cloudflared for zero-account temporary TryCloudflare tunnels (if not already present)
if ! command -v cloudflared >/dev/null 2>&1; then
  ARCH="$(dpkg --print-architecture)"
  echo "=== Installing cloudflared (${ARCH}) for Quick Tunnels ==="
  curl -fsSL "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-${ARCH}.deb" -o /tmp/cloudflared.deb && \
    sudo dpkg -i /tmp/cloudflared.deb && rm -f /tmp/cloudflared.deb || true
fi

sudo cp "${REPO_DIR}/configs/blacklist-rtlsdr.conf" /etc/modprobe.d/blacklist-rtlsdr.conf
sudo rmmod rtl2832_sdr dvb_usb_rtl28xxu rtl2832 dvb_usb_v2 dvb_core 2>/dev/null || true
sudo usermod -aG plugdev "${RUN_USER}" || true

cat <<EOF | sudo tee /etc/default/sdr-station >/dev/null
STATION_NAME="${STATION_NAME}"
STATION_LAT="${STATION_LAT}"
STATION_LON="${STATION_LON}"
EOF

mkdir -p "${REPO_DIR}/data/ais" "${REPO_DIR}/data/adsb" "${REPO_DIR}/data/ism"

for svc in adsb-logger.service ais-catcher.service sdr-voice-web.service rtl-433-web.service sdr-rf-protocols.service rtl-tcp.service sdr-launcher.service opensanctions-sync.service opensanctions-sync.timer; do
  sed -e "s|User=pi|User=${RUN_USER}|g" \
      -e "s|/opt/sdr-station|${REPO_DIR}|g" \
      -e "s|<your-lat>|${STATION_LAT}|g" \
      -e "s|<your-lon>|${STATION_LON}|g" \
      "${REPO_DIR}/configs/${svc}" | sudo tee "/etc/systemd/system/${svc}" >/dev/null
done

sudo systemctl daemon-reload
sudo systemctl enable --now sdr-launcher.service adsb-logger.service opensanctions-sync.timer
echo "=== Done! Open http://$(hostname -I | awk '{print $1}'):8080/ ==="
