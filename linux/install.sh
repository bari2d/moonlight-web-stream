#!/usr/bin/env bash
# Build output -> Other Programs/MoonlightWeb (runtime dir), plus systemd user units.
# Usage: linux/install.sh [--build]
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
DEST="${MOONLIGHT_WEB_DEST:-/run/media/bariman/url/Other Programs/MoonlightWeb}"
UNITS="$HOME/.config/systemd/user"

if [[ "${1:-}" == "--build" ]]; then
    (cd "$REPO" && cargo build --release && npm run build)
fi

mkdir -p "$DEST/static" "$DEST/server" "$UNITS"
install -m755 "$REPO/target/release/web-server" "$REPO/target/release/streamer" "$DEST/"
# WebTransport listens on UDP 443
sudo setcap cap_net_bind_service+ep "$DEST/web-server"
cp -r "$REPO/dist/." "$DEST/static/"
chmod +x "$REPO/linux/tunnel.sh" "$REPO/linux/resume-restart.sh"

# Both units restart forever (no start-limit), and wait for the url drive at
# boot. Linger lets them run at boot without anyone logging in.
cat > "$UNITS/moonlight-web.service" <<EOF
[Unit]
Description=Moonlight Web (low-latency fork)
After=network-online.target app-dev.lizardbyte.app.Sunshine.service
StartLimitIntervalSec=0

[Service]
WorkingDirectory=$DEST
ExecStartPre=/bin/sh -c 'until [ -x "$DEST/web-server" ]; do sleep 2; done'
ExecStart="$DEST/web-server"
TimeoutStartSec=infinity
Restart=always
RestartSec=3

[Install]
WantedBy=default.target
EOF

cat > "$UNITS/moonlight-tunnel.service" <<EOF
[Unit]
Description=Cloudflare quick tunnel for Moonlight Web + redirect.bari2d.dev
After=network-online.target moonlight-web.service
Wants=moonlight-web.service
StartLimitIntervalSec=0

[Service]
ExecStartPre=/bin/sh -c 'until [ -x "$REPO/linux/tunnel.sh" ]; do sleep 2; done'
ExecStart="$REPO/linux/tunnel.sh"
TimeoutStartSec=infinity
Restart=always
RestartSec=5

[Install]
WantedBy=default.target
EOF

# Quick tunnels never survive suspend; restart the tunnel on resume instead of
# waiting for the watchdog while clients hit the dead hostname.
cat > "$UNITS/moonlight-resume.service" <<EOF
[Unit]
Description=Restart Moonlight Web tunnel after resume from suspend
After=moonlight-tunnel.service
StartLimitIntervalSec=0

[Service]
ExecStart="$REPO/linux/resume-restart.sh"
Restart=always
RestartSec=5

[Install]
WantedBy=default.target
EOF

cat > "$UNITS/moonlight-cert.service" <<EOF
[Unit]
Description=Renew WebTransport certificate for Moonlight Web

[Service]
Type=oneshot
ExecStart="$REPO/linux/cert.sh"
EOF

cat > "$UNITS/moonlight-cert.timer" <<EOF
[Unit]
Description=Daily WebTransport certificate renewal check

[Timer]
OnCalendar=daily
RandomizedDelaySec=1h
Persistent=true

[Install]
WantedBy=timers.target
EOF

systemctl --user daemon-reload
systemctl --user enable --now moonlight-cert.timer
# older installs hooked the tunnel under moonlight-web.service.wants
rm -rf "$UNITS/moonlight-web.service.wants"
loginctl enable-linger "$USER"
systemctl --user enable moonlight-web.service moonlight-tunnel.service moonlight-resume.service
# Restarting the tunnel would mint a new quick-tunnel URL; leave it running.
systemctl --user restart moonlight-web.service
systemctl --user start moonlight-tunnel.service
systemctl --user restart moonlight-resume.service
echo "Installed to $DEST"
